import { rejects } from "node:assert/strict"
import { assert, describe, it } from "@effect/vitest"
import { DateTime, Effect, Schema } from "effect"
import {
  createSQLiteCorePersistenceAdapter,
  type PersistedCollectionCoordinator,
  type PersistedTx,
  type ProtocolEnvelope,
  SingleProcessCoordinator,
} from "@tanstack/db-sqlite-persistence-core"
import { PersistenceCodecError, withPersistedSchema } from "../src/persistence/persisted-schema.js"
import { makeNodeSqliteDriver } from "./node-sqlite-driver.js"

class Detail extends Schema.Class<Detail>("Detail")({ label: Schema.String }) {
  display() { return this.label.toUpperCase() }
}
class Category extends Schema.Class<Category>("Category")({
  id: Schema.String,
  createdAt: Schema.DateTimeUtcFromString,
  detail: Detail,
  note: Schema.optionalKey(Schema.String),
}) { readonly [key: string]: unknown }
const category = (note?: string) => new Category({
  id: "category-1", createdAt: DateTime.makeUnsafe(0), detail: new Detail({ label: "food" }),
  ...(note === undefined ? {} : { note }),
})
const tx = (mutations: PersistedTx["mutations"], seq = 1): PersistedTx => ({
  txId: `tx-${seq}`, term: 1, seq, rowVersion: seq, mutations,
})
const fixture = () => {
  const raw = createSQLiteCorePersistenceAdapter({ driver: makeNodeSqliteDriver() })
  const { adapter } = withPersistedSchema({ adapter: raw }, Category)
  return { raw, adapter }
}

const expectEncodeFailure = (write: () => Promise<void>) => Effect.promise(() => rejects(
  write,
  (error: unknown) => Schema.is(PersistenceCodecError)(error) && error.operation === "encode",
))

describe("persistence codec boundary", () => {
  it.effect("encodes SQLite rows and reconstructs classes, methods and DateTimeUtc on loads and scans", () => Effect.gen(function* () {
    const { raw, adapter } = fixture()
    const row = category("old")
    yield* Effect.promise(() => adapter.applyCommittedTx("categories", tx([
      { type: "insert", key: row.id, value: row, metadata: { source: "server" }, metadataChanged: true },
    ])))
    const [stored] = yield* Effect.promise(() => raw.loadSubset("categories", {}))
    assert.deepStrictEqual(stored!.value, {
      id: row.id, createdAt: "1970-01-01T00:00:00.000Z", detail: { label: "food" }, note: "old",
    })
    for (const rows of [
      yield* Effect.promise(() => adapter.loadSubset("categories", {})),
      yield* Effect.promise(() => adapter.scanRows!("categories", { metadataOnly: true })),
    ]) {
      const saved = rows[0]!.value
      assert.isTrue(saved instanceof Category)
      if (!(saved instanceof Category)) return yield* Effect.die("expected a decoded Category")
      assert.isTrue(DateTime.isDateTime(saved.createdAt))
      assert.strictEqual(DateTime.toEpochMillis(saved.createdAt), 0)
      assert.strictEqual(saved.detail.display(), "FOOD")
      assert.deepStrictEqual(rows[0]!.metadata, { source: "server" })
    }
    // Codec output is a full replacement: removing optional fields must persist.
    yield* Effect.promise(() => adapter.applyCommittedTx("categories", tx([
      { type: "update", key: row.id, value: category() },
    ], 2)))
    const [updated] = yield* Effect.promise(() => adapter.loadSubset("categories", {}))
    assert.isFalse(Object.hasOwn(updated!.value, "note"))
    assert.deepStrictEqual(updated!.metadata, { source: "server" })
    yield* Effect.promise(() => adapter.applyCommittedTx("categories", tx([
      { type: "delete", key: row.id, value: {} },
    ], 3)))
    assert.deepStrictEqual(yield* Effect.promise(() => adapter.loadSubset("categories", {})), [])
  }))

  it.effect("supports a runtime-only model through its explicit JSON codec", () => Effect.gen(function* () {
    class RuntimeCategory extends Schema.Class<RuntimeCategory>("RuntimeCategory")({
      id: Schema.String, createdAt: Schema.DateTimeUtc,
    }) { readonly [key: string]: unknown }
    const raw = createSQLiteCorePersistenceAdapter({ driver: makeNodeSqliteDriver() })
    const { adapter } = withPersistedSchema({ adapter: raw }, Schema.toCodecJson(RuntimeCategory))
    const row = new RuntimeCategory({ id: "runtime", createdAt: DateTime.makeUnsafe(0) })
    yield* Effect.promise(() => adapter.applyCommittedTx("runtime", tx([
      { type: "insert", key: row.id, value: row },
    ])))
    const [restored] = yield* Effect.promise(() => adapter.loadSubset("runtime", {}))
    assert.isTrue(restored!.value instanceof RuntimeCategory)
    assert.isTrue(DateTime.isDateTime(restored!.value["createdAt"]))
  }))

  it.effect("fails an invalid encode before committing any row or truncating existing data", () => Effect.gen(function* () {
    const { raw, adapter } = fixture()
    yield* Effect.promise(() => adapter.applyCommittedTx("categories", tx([
      { type: "insert", key: "category-1", value: category() },
    ])))
    yield* expectEncodeFailure(() => adapter.applyCommittedTx("categories", {
      ...tx([
        { type: "insert", key: "new", value: new Category({ ...category(), id: "new" }) },
        { type: "insert", key: "bad", value: { id: "bad", createdAt: "not a runtime date" } },
      ], 2),
      truncate: true,
    }))
    assert.strictEqual((yield* Effect.promise(() => raw.loadSubset("categories", {}))).length, 1)
    assert.strictEqual((yield* Effect.promise(() => raw.getStreamPosition!("categories"))).latestSeq, 1)
  }))

  it.effect("rejects a corrupt persisted batch rather than exposing partially decoded rows", () => Effect.gen(function* () {
    const { raw, adapter } = fixture()
    yield* Effect.promise(() => raw.applyCommittedTx("categories", tx([
      { type: "insert", key: "bad", value: { id: "bad", createdAt: "broken" } },
    ])))
    yield* Effect.promise(() => rejects(
      () => adapter.loadSubset("categories", {}),
      (error: unknown) => Schema.is(PersistenceCodecError)(error)
        && error.operation === "decode" && error.collectionId === "categories",
    ))
  }))

  it.effect("rejects encoded scalar values at the JSON object boundary", () => Effect.gen(function* () {
    const raw = createSQLiteCorePersistenceAdapter({ driver: makeNodeSqliteDriver() })
    const { adapter } = withPersistedSchema({ adapter: raw }, Schema.fromJsonString(Category))
    yield* expectEncodeFailure(() => adapter.applyCommittedTx("categories", tx([
      { type: "insert", key: "category-1", value: category() },
    ])))
    assert.deepStrictEqual(yield* Effect.promise(() => raw.loadSubset("categories", {})), [])
  }))

  it.effect("cross-tab notices and pull recovery reload through the codec without cloning runtime rows", () => Effect.gen(function* () {
    const raw = createSQLiteCorePersistenceAdapter({ driver: makeNodeSqliteDriver() })
    let receive: (message: ProtocolEnvelope<unknown>) => void = () => undefined
    let published: ProtocolEnvelope<unknown> | undefined
    let delivered: ProtocolEnvelope<unknown> | undefined
    const single: PersistedCollectionCoordinator = new SingleProcessCoordinator()
    const coordinator: PersistedCollectionCoordinator = {
      getNodeId: () => single.getNodeId(),
      isLeader: (id) => single.isLeader(id),
      ensureLeadership: (id) => single.ensureLeadership(id),
      requestEnsurePersistedIndex: (...args) => single.requestEnsurePersistedIndex(...args),
      subscribe: (_, callback) => { receive = callback; return () => undefined },
      publish: (_, message) => { published = structuredClone(message) },
      pullSince: async () => ({
        type: "rpc:pullSince:res", rpcId: "pull", ok: true,
        latestTerm: 1, latestSeq: 1, latestRowVersion: 1, requiresFullReload: false,
        changedKeys: ["category-1"], deletedKeys: [], deltas: [],
      }),
    }
    const wrapped = withPersistedSchema({ adapter: raw, coordinator }, Category).coordinator!
    const message: ProtocolEnvelope<unknown> = {
      v: 1, dbName: "test", collectionId: "categories", senderId: "other", ts: 0,
      payload: {
        type: "tx:committed", txId: "tx-1", term: 1, seq: 1, latestRowVersion: 1,
        requiresFullReload: false, changedRows: [{ key: "category-1", value: category() }], deletedKeys: [],
      },
    }
    wrapped.publish("categories", message)
    assert.deepStrictEqual(published?.payload, {
      type: "tx:committed", txId: "tx-1", term: 1, seq: 1, latestRowVersion: 1, requiresFullReload: true,
    })
    wrapped.subscribe("categories", (message) => { delivered = message })
    receive(message)
    assert.deepStrictEqual(delivered, published)
    const recovery = yield* Effect.promise(() => wrapped.pullSince!("categories", 0))
    assert.isTrue(recovery.ok && recovery.requiresFullReload)
  }))
})
