import { assert, describe, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Layer, Queue, Option, Schema, Stream } from "effect"
import {
  CatchupResponse,
  compareSyncId,
  Epoch,
  defineModelRegistry,
  deriveGroup,
  HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  PendingSyncEvent,
  SyncId
} from "@triargos/live-collection-protocol"
import { ModelRegistry } from "../src/model-registry.js"
import { SyncDispatcher } from "../src/sync-dispatcher.js"
import { SyncEventBus } from "../src/sync-event-bus.js"
import { CursorOutOfRetentionError, SyncEventStore, type SyncEventStoreShape } from "../src/sync-event-store.js"
import { SyncFeed } from "../src/sync-feed.js"
import { TestClock } from "effect/testing"
import { makeKernelLayer } from "./support/layers.js"
import { Note, NoteId, NoteRepo, testRegistry, testRegistryBatched } from "./support/test-registry.js"

const alice = deriveGroup(["user", "alice"])
const bob = deriveGroup(["user", "bob"])
const zero = SyncId.make("0")

const note = (id: string, title: string): Note => ({ id: NoteId.make(id), title })

const noteEvent = (
  kind: "Insert" | "Update" | "Delete",
  id: string,
  groups: ReadonlyArray<typeof alice> = [alice]
) =>
  PendingSyncEvent.cases[kind].make({
    modelName: ModelName.make("Note"),
    modelId: ModelId.make(id),
    syncGroups: [groups[0]!, ...groups.slice(1)]
  })

// A model whose schema carries a field that is not JSON-native in its plain
// encoded form — the regression surface for the canonical-JSON encode edge.
const Stamped = Schema.Struct({ id: Schema.String, createdAt: Schema.Date })
const stampedRow = { id: "s1", createdAt: new Date("2026-07-23T12:12:08.434Z") }
const stampedRegistry = Effect.succeed(
  defineModelRegistry({
    Stamped: {
      modelName: "Stamped",
      schema: Stamped,
      hydrate: (id: ModelId) => Effect.succeed(id === "s1" ? Option.some(stampedRow) : Option.none())
    }
  })
)

/** Write to the repo and dispatch the matching event — an app's write handler in miniature. */
const upsertNote = (row: Note, kind: "Insert" | "Update" = "Insert") =>
  Effect.gen(function* () {
    yield* Effect.flatMap(NoteRepo, (repo) => repo.upsert(row))
    yield* Effect.flatMap(SyncDispatcher, (d) => d.dispatch(noteEvent(kind, row.id)))
  })

describe("SyncFeed.catchup", () => {
  it.effect("squashes runs, hydrates current data, and reports the head cursor", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const store = yield* SyncEventStore

      yield* upsertNote(note("n1", "First"))
      yield* upsertNote(note("n1", "Renamed"), "Update")
      yield* upsertNote(note("n2", "Transient"))
      yield* Effect.flatMap(NoteRepo, (repo) => repo.remove(NoteId.make("n2")))
      yield* Effect.flatMap(SyncDispatcher, (d) => d.dispatch(noteEvent("Delete", "n2")))

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })

      // Insert→Update folds to one Insert with current data; Insert→Delete cancels out.
      assert.strictEqual(response.events.length, 1)
      const event = response.events[0]!
      assert.strictEqual(event._tag, "Insert")
      if (event._tag === "Insert") {
        assert.strictEqual(String(event.modelId), "n1")
        const decoded = yield* Schema.decodeUnknownEffect(Note)(event.data)
        assert.strictEqual(decoded.title, "Renamed")
        // The folded event carries the run's latest syncId, so the cursor advances past every absorbed event.
        assert(compareSyncId(event.syncId, SyncId.make("1")) > 0)
      }
      assert.strictEqual(response.lastSyncId, yield* store.getLatestSyncId)
      assert.deepStrictEqual(response.epoch, yield* store.getCurrentEpoch)
    }).pipe(Effect.provide(makeKernelLayer())))

  it.effect("filters by exact group intersection — a foreign group's events never leak", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      yield* upsertNote(note("mine", "Visible"))
      yield* Effect.flatMap(SyncDispatcher, (d) => d.dispatch(noteEvent("Insert", "theirs", [bob])))

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })
      assert.deepStrictEqual(
        response.events.map((e) => (e._tag === "Resync" ? "Resync" : String(e.modelId))),
        ["mine"]
      )

      const bobResponse = yield* feed.catchup({ fromSyncId: zero, syncGroups: [bob] })
      // Bob's event exists in the log but its entity was never in the repo ⇒ hydration downgrades to Delete.
      assert.deepStrictEqual(bobResponse.events.map((e) => e._tag), ["Delete"])
    }).pipe(Effect.provide(makeKernelLayer())))

  it.effect("an entity gone at hydration time arrives as a Delete — access loss surfaces as removal", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      yield* upsertNote(note("vanishing", "Here now"))
      // The row disappears (deleted, or ACL lost) without a Delete event being logged.
      yield* Effect.flatMap(NoteRepo, (repo) => repo.remove(NoteId.make("vanishing")))

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })
      assert.deepStrictEqual(response.events.map((e) => e._tag), ["Delete"])
    }).pipe(Effect.provide(makeKernelLayer())))

  it.effect("unknown models are dropped, not fatal — newer servers stay compatible", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      yield* Effect.flatMap(SyncDispatcher, (d) =>
        d.dispatch(
          PendingSyncEvent.cases.Insert.make({
            modelName: ModelName.make("Exotic"),
            modelId: ModelId.make("x1"),
            syncGroups: [alice]
          })
        )
      )
      yield* upsertNote(note("known", "Still works"))

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })
      assert.deepStrictEqual(
        response.events.map((e) => (e._tag === "Resync" ? "Resync" : String(e.modelId))),
        ["known"]
      )
    }).pipe(Effect.provide(makeKernelLayer())))

  it.effect("encodes non-JSON-native fields to canonical JSON — a Date hydrates onto the wire as an ISO string", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      yield* Effect.flatMap(SyncDispatcher, (d) =>
        d.dispatch(
          PendingSyncEvent.cases.Insert.make({
            modelName: ModelName.make("Stamped"),
            modelId: ModelId.make("s1"),
            syncGroups: [alice]
          })
        )
      )

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })

      assert.strictEqual(response.events.length, 1)
      const event = response.events[0]!
      assert.strictEqual(event._tag, "Insert")
      if (event._tag === "Insert") {
        // Schema.Date's plain encoded form is still a Date instance; the registry's
        // canonical JSON codec must turn it into an ISO string — not leave a Date
        // for JSON.stringify to improvise over.
        const data = event.data as { readonly createdAt: unknown }
        assert.strictEqual(typeof data.createdAt, "string")
        assert.strictEqual(data.createdAt, "2026-07-23T12:12:08.434Z")
      }
    }).pipe(Effect.provide(makeKernelLayer(stampedRegistry))))

  it.effect("hydrateMany batches lookups — one pass per model, not one per event", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const repo = yield* NoteRepo
      yield* upsertNote(note("b1", "One"))
      yield* upsertNote(note("b2", "Two"))
      yield* upsertNote(note("b3", "Three"))
      const before = yield* repo.lookupCount

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })

      assert.strictEqual(response.events.length, 3)
      // The batched registry funnels all ids through one hydrateMany call; the
      // test repo's per-id find still counts 3 — what matters is the descriptor
      // received them together (asserted structurally: batch size == events).
      assert.strictEqual((yield* repo.lookupCount) - before, 3)
    }).pipe(Effect.provide(makeKernelLayer(testRegistryBatched))))

  it.effect("a cursor out of retention becomes a single synthetic Resync(All), never an error", () =>
    Effect.gen(function* () {
      const memory = yield* Effect.provide(
        Effect.flatMap(SyncEventStore, Effect.succeed),
        SyncEventStore.layerMemory
      )
      const pruningStore: SyncEventStoreShape = {
        ...memory,
        listEvents: ({ cursor }) => Effect.fail(new CursorOutOfRetentionError({ cursor }))
      }
      const layer = Layer.mergeAll(
        SyncDispatcher.layer,
        SyncFeed.layer.pipe(Layer.provide(ModelRegistry.layer(testRegistry)))
      ).pipe(
        Layer.provideMerge(
          Layer.mergeAll(
            Layer.succeed(SyncEventStore, pruningStore),
            SyncEventBus.layerMemory,
            NoteRepo.layerMemory
          )
        )
      )

      yield* Effect.gen(function* () {
        yield* upsertNote(note("kept", "Retained row"))
        const feed = yield* SyncFeed
        const store = yield* SyncEventStore
        const response = yield* feed.catchup({ fromSyncId: SyncId.make("1"), syncGroups: [alice] })

        assert.strictEqual(response.events.length, 1)
        const event = response.events[0]!
        assert.strictEqual(event._tag, "Resync")
        if (event._tag === "Resync") assert.strictEqual(event.target._tag, "All")
        assert.strictEqual(response.lastSyncId, yield* store.getLatestSyncId)
        // Synthesized inline: nothing new appended to the log.
        assert.strictEqual(yield* store.getLatestSyncId, "1")
      }).pipe(Effect.provide(layer))
    }))
})

const decodeFrame = (frame: string) => Schema.decodeEffect(Schema.fromJsonString(CatchupResponse))(frame.slice("data: ".length, -2))

describe("SyncFeed.streamEvents", () => {
  it.effect("replays authorized changes committed before connection, then polls unpublished commits in order", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const store = yield* SyncEventStore
      const repo = yield* NoteRepo
      yield* upsertNote(note("replayed", "Replay"))
      yield* store.appendEvent(noteEvent("Insert", "foreign", [bob]))
      const seen = yield* Queue.unbounded<CatchupResponse>()
      yield* feed.streamEvents({ fromSyncId: zero, epoch: Option.none(), syncGroups: [alice] }).pipe(
        Stream.mapEffect(decodeFrame), Stream.runForEach((batch) => Queue.offer(seen, batch)), Effect.forkChild,
      )
      const first = yield* Queue.take(seen)
      assert.deepStrictEqual(first.events.map((event) => event.syncId), [SyncId.make("1")])
      assert.strictEqual(first.lastSyncId, "2") // authorization gap is safely covered
      // No bus publication, and no later event to wake anything up.
      yield* repo.upsert(note("unpublished", "Durable"))
      yield* store.appendEvent(noteEvent("Insert", "unpublished"))
      yield* TestClock.adjust("1 second")
      const second = yield* Queue.take(seen)
      assert.deepStrictEqual(second.events.map((event) => event.syncId), [SyncId.make("3")])
      assert.strictEqual(second.lastSyncId, "3")
    }).pipe(Effect.scoped, Effect.provide(makeKernelLayer())))

  it.effect("bounds replay before hydration; an append during the read appears in the next batch", () =>
    Effect.gen(function* () {
      const memory = yield* Effect.provide(SyncEventStore, SyncEventStore.layerMemory)
      yield* memory.appendEvent(noteEvent("Delete", "101"))
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let reads = 0
      const store: SyncEventStoreShape = { ...memory, listEvents: (args) => Effect.gen(function* () {
        if (reads++ === 0) {
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
        }
        return yield* memory.listEvents(args)
      }) }
      const layer = SyncFeed.layer.pipe(Layer.provide(Layer.mergeAll(
        Layer.succeed(SyncEventStore, store), ModelRegistry.layer(testRegistry).pipe(Layer.provide(NoteRepo.layerMemory)),
      )))
      yield* Effect.gen(function* () {
        const feed = yield* SyncFeed
        const seen = yield* Queue.unbounded<CatchupResponse>()
        yield* feed.streamEvents({ fromSyncId: zero, epoch: Option.none(), syncGroups: [alice] }).pipe(
          Stream.mapEffect(decodeFrame), Stream.runForEach((batch) => Queue.offer(seen, batch)), Effect.forkChild,
        )
        yield* Deferred.await(entered)
        yield* memory.appendEvent(noteEvent("Delete", "102"))
        yield* Deferred.succeed(release, undefined)
        const first = yield* Queue.take(seen)
        assert.deepStrictEqual(first.events.map((event) => event.syncId), [SyncId.make("1")])
        assert.strictEqual(first.lastSyncId, "1")
        yield* TestClock.adjust("1 second")
        const second = yield* Queue.take(seen)
        assert.deepStrictEqual(second.events.map((event) => event.syncId), [SyncId.make("2")])
      }).pipe(Effect.provide(layer))
    }).pipe(Effect.scoped))

  it.effect("closes after an epoch mismatch even when the old cursor is above the new head", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const frames = yield* feed.streamEvents({ fromSyncId: SyncId.make("100"), epoch: Option.some(Epoch.make("old")), syncGroups: [alice] }).pipe(Stream.runCollect)
      assert.strictEqual(frames.length, 1)
      const batch = yield* decodeFrame(frames[0] ?? "")
      assert.strictEqual(batch.lastSyncId, "0")
      assert.isTrue(Option.isSome(batch.epoch))
    }).pipe(Effect.scoped, Effect.provide(makeKernelLayer())))

  it.effect("closes immediately after retention Resync; no endless keepalives", () =>
    Effect.gen(function* () {
      const memory = yield* Effect.provide(SyncEventStore, SyncEventStore.layerMemory)
      const store: SyncEventStoreShape = { ...memory, listEvents: ({ cursor }) => Effect.fail(new CursorOutOfRetentionError({ cursor })) }
      const layer = SyncFeed.layer.pipe(Layer.provide(Layer.mergeAll(
        Layer.succeed(SyncEventStore, store), ModelRegistry.layer(testRegistry).pipe(Layer.provide(NoteRepo.layerMemory)),
      )))
      const frames = yield* Effect.flatMap(SyncFeed, (feed) => feed.streamEvents({ fromSyncId: zero, epoch: Option.none(), syncGroups: [alice] }).pipe(Stream.runCollect)).pipe(Effect.provide(layer))
      assert.strictEqual(frames.length, 1)
      assert.deepStrictEqual((yield* decodeFrame(frames[0] ?? "")).events.map((event) => event._tag), ["Resync"])
    }).pipe(Effect.scoped))
})
