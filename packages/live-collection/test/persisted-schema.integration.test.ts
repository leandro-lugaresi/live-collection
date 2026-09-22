import { assert, describe, it } from "@effect/vitest"
import { DateTime, Effect, Layer, Option, Queue, Schema } from "effect"
import { createCollection } from "@tanstack/db"
import { createSQLiteCorePersistenceAdapter, persistedCollectionOptions, type PersistedCollectionCoordinator, type ProtocolEnvelope, type PersistedCollectionPersistence } from "@tanstack/db-sqlite-persistence-core"
import { HydrateBatchResult, ModelId, SyncId } from "@leandro-lugaresi/live-collection-protocol"
import { defineCollection } from "../src/define-collection.js"
import { makeLiveRuntime } from "../src/runtime/live-runtime.js"
import { CatchupClient } from "../src/client/catchup-client.js"
import { SyncJournal } from "../src/client/sync-journal.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { HydrateClient } from "../src/client/hydrate-client.js"
import { withPersistedSchema } from "../src/persistence/persisted-schema.js"
import { liveCollectionOptions } from "../src/persistence/live-collection-options.js"
import { makeNodeSqliteDriver } from "./node-sqlite-driver.js"
import { makeNodeSqlitePersistence } from "./sqlite-persistence.js"

class Category extends Schema.Class<Category>("Category")({
  id: Schema.String,
  createdAt: Schema.DateTimeUtcFromString,
}) {}

const row = new Category({ id: "category-1", createdAt: DateTime.makeUnsafe(0) })
const key = ModelId.make(row.id)

const observeCommits = (source: PersistedCollectionPersistence, committed: Queue.Queue<void>) => {
  const observe = (persistence: PersistedCollectionPersistence): PersistedCollectionPersistence => ({
      ...persistence,
      adapter: {
        loadSubset: (...args) => persistence.adapter.loadSubset(...args),
        ensureIndex: (...args) => persistence.adapter.ensureIndex(...args),
        ...(persistence.adapter.getStreamPosition ? {
          getStreamPosition: (id) => persistence.adapter.getStreamPosition!(id),
        } : {}),
        applyCommittedTx: async (id, tx) => {
          await persistence.adapter.applyCommittedTx(id, tx)
          if (tx.mutations.length > 0) await Effect.runPromise(Queue.offer(committed, undefined))
        },
      },
      ...(persistence.resolvePersistenceForCollection ? {
        resolvePersistenceForCollection: (options) => observe(persistence.resolvePersistenceForCollection!(options)),
      } : {}),
    })
  return observe(source)
}

describe("persistedSchema — native collection hydration", () => {
  for (const kind of ["global", "scoped"] as const) it.live(`restores DateTimeUtc through ${kind} collections without a server relist`, () => Effect.gen(function* () {
    const committed = yield* Queue.unbounded<void>()
    const source = makeNodeSqlitePersistence()
    const persistence = observeCommits(source, committed)
    const events = yield* Queue.unbounded<never>()
    const sync = Layer.mergeAll(
      CatchupClient.layerMemory({ events: [], lastSyncId: SyncId.make("0"), epoch: Option.none() }),
      SyncTransport.layerMemory(events),
      SyncJournal.layerMemory,
    )
    const runtime = makeLiveRuntime({ persistence, sync })
    const config = {
      runtime,
      entity: "Category",
      schema: Category,
      persistedSchema: Category,
      getKey: (category: Category) => ModelId.make(category.id),
      // The server is unavailable. Only local storage can supply the restored row.
      listFn: Effect.never,
    }
    const categories = kind === "global" ? defineCollection(config) : (() => {
      const handle = defineCollection({ ...config, scopeOf: () => "scope", listFn: () => Effect.never })
      return () => handle("scope")
    })()
    yield* Effect.addFinalizer(() => runtime.registry.disposeAll.pipe(Effect.tap(() => Effect.sync(() => runtime.dispose()))))
    const first = categories()
    yield* Effect.promise(() => first.preload())
    yield* first.utils.writeSynced(row)
    yield* Queue.take(committed)
    // Native TanStack reads spread the root row to add virtual properties, even
    // before storage. Nested runtime values must still retain their semantics.
    assert.isFalse(first.get(key) instanceof Category)
    assert.isTrue(DateTime.isDateTime(first.get(key)!.createdAt))
    yield* runtime.registry.disposeAll
    const restored = categories()
    yield* Effect.promise(() => restored.preload())
    const saved = restored.get(key)
    assert.isDefined(saved)
    assert.isTrue(DateTime.isDateTime(saved!.createdAt))
    assert.strictEqual(DateTime.toEpochMillis(saved!.createdAt), 0)
    yield* restored.utils.writeSynced(new Category({ id: row.id, createdAt: DateTime.makeUnsafe(1000) }))
    yield* Queue.take(committed)
    yield* runtime.registry.disposeAll
    const updated = categories()
    yield* Effect.promise(() => updated.preload())
    assert.strictEqual(DateTime.toEpochMillis(updated.get(key)!.createdAt), 1000)
    yield* updated.utils.deleteSynced(key)
    yield* Queue.take(committed)
    yield* runtime.registry.disposeAll
    const deleted = categories()
    yield* Effect.promise(() => deleted.preload())
    assert.isFalse(deleted.has(key))
  }).pipe(Effect.scoped))

  it.live("partial slices retain rich fields on a cache-only remount", () => Effect.gen(function* () {
    const committed = yield* Queue.unbounded<void>()
    const events = yield* Queue.unbounded<never>()
    let fetches = 0
    const sync = Layer.mergeAll(
      CatchupClient.layerMemory({ events: [], lastSyncId: SyncId.make("0"), epoch: Option.none() }),
      SyncTransport.layerMemory(events),
      SyncJournal.layerMemory,
      HydrateClient.layerMemory((request) => Effect.sync(() => {
        fetches += 1
        return {
          results: request.requests.map((r) => HydrateBatchResult.cases.Members.make({
            request: r, rows: [{ id: row.id, createdAt: "1970-01-01T00:00:00.000Z" }],
          })),
          lastSyncId: SyncId.make("1"), epoch: Option.none(),
        }
      })),
    )
    const runtime = makeLiveRuntime({ persistence: observeCommits(makeNodeSqlitePersistence(), committed), sync })
    yield* Effect.addFinalizer(() => runtime.registry.disposeAll.pipe(Effect.tap(() => Effect.sync(() => runtime.dispose()))))
    const categories = defineCollection({
      runtime, entity: "Category", schema: Category, persistedSchema: Category,
      getKey: (category) => ModelId.make(category.id), partial: { by: { id: (category) => category.id } },
    })
    const first = categories()
    yield* Effect.promise(() => first.preload())
    yield* Effect.promise(() => first.utils.loadById(row.id))
    // The observer ignores empty reset transactions; this waits for the slice itself.
    yield* Queue.take(committed)
    yield* runtime.registry.disposeAll
    const second = categories()
    yield* Effect.promise(() => second.preload())
    yield* Effect.promise(() => second.utils.loadById(row.id))
    assert.isTrue(DateTime.isDateTime(second.get(key)!.createdAt))
    assert.strictEqual(fetches, 1)
  }).pipe(Effect.scoped))

  it.live("cross-tab invalidations hydrate rich fields and remove deleted rows in the follower", () => Effect.gen(function* () {
    const adapter = createSQLiteCorePersistenceAdapter({ driver: makeNodeSqliteDriver() })
    const listeners = new Map<string, (message: ProtocolEnvelope<unknown>) => void>()
    const coordinator = (node: string): PersistedCollectionCoordinator => ({
      getNodeId: () => node,
      isLeader: () => true,
      ensureLeadership: () => Promise.resolve(),
      requestEnsurePersistedIndex: () => Promise.resolve(),
      subscribe: (_, receive) => { listeners.set(node, receive); return () => { listeners.delete(node) } },
      publish: (_, message) => {
        for (const [other, receive] of listeners) if (other !== node) receive(structuredClone(message))
      },
    })
    const mount = (node: string) => createCollection(persistedCollectionOptions({
      id: "shared", persistence: withPersistedSchema({ adapter, coordinator: coordinator(node) }, Category),
      ...liveCollectionOptions({ getKey: (category: Category) => ModelId.make(category.id) }),
    }))
    const writer = mount("writer")
    const follower = mount("follower")
    yield* Effect.addFinalizer(() => Effect.promise(async () => { await writer.cleanup(); await follower.cleanup() }))
    yield* Effect.promise(async () => { await writer.preload(); await follower.preload() })
    const changes = yield* Queue.unbounded<void>()
    const subscription = follower.subscribeChanges(() => { Effect.runSync(Queue.offer(changes, undefined)) })
    yield* Effect.addFinalizer(() => Effect.sync(() => subscription.unsubscribe()))
    yield* writer.utils.writeSynced(row)
    yield* Queue.take(changes)
    assert.isTrue(DateTime.isDateTime(follower.get(key)!.createdAt))
    yield* writer.utils.deleteSynced(key)
    yield* Queue.take(changes)
    assert.isFalse(follower.has(key))
  }).pipe(Effect.scoped))

})
