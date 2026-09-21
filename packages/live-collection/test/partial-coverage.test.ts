import { assert, describe, it } from "@effect/vitest"
import { Effect, Ref, Schema } from "effect"
import { ModelId, SyncId } from "@triargos/live-collection-protocol"
import { applySlice, makePartialApplier, seedCoverage } from "../src/partial-coverage.js"
import { SyncSignal } from "../src/client/sync-signal.js"

const Row = Schema.Struct({ id: Schema.String, subset: Schema.String, label: Schema.String })
const sid = SyncId.make
const id = ModelId.make("row")
const current = { id, subset: "loaded", label: "snapshot" }
const fixture = Effect.gen(function* () {
  const rows = new Map<ModelId, typeof Row.Type>([[id, current]])
  const coverage = yield* Ref.make(seedCoverage([{ subset: { indexKey: "subset", keyValue: "loaded" }, at: sid("5") }]))
  const deps = {
    entity: "Row", by: { subset: (row: typeof Row.Type) => row.subset },
    getKey: (row) => ModelId.make(row.id), decode: Schema.decodeUnknownEffect(Row), coverage,
    write: {
      has: (key) => rows.has(key), currentRows: () => rows.values(),
      writeSynced: (row) => Effect.sync(() => { rows.set(ModelId.make(row.id), { ...row, id: ModelId.make(row.id) }) }),
      deleteSynced: (key) => Effect.sync(() => { rows.delete(key) }),
      replaceSynced: (values) => Effect.sync(() => { rows.clear(); for (const row of values) rows.set(ModelId.make(row.id), { ...row, id: ModelId.make(row.id) }) }),
    },
  } satisfies Parameters<typeof makePartialApplier<typeof Row.Type>>[0]
  const apply = makePartialApplier(deps)
  return { rows, apply, coverage, deps }
})

describe("partial coverage freshness", () => {
  it.effect("Delete 3 and Upsert 4 cannot destroy a snapshot at 5", () => Effect.gen(function* () {
    const { rows, apply } = yield* fixture
    yield* apply(SyncSignal.Delete({ syncId: sid("3"), modelId: id }))
    yield* apply(SyncSignal.Upsert({ syncId: sid("4"), modelId: id, data: { ...current, label: "old" } }))
    assert.deepStrictEqual(rows.get(id), current)
  }))
  it.effect("an old move outside the loaded subset cannot delete a snapshot row", () => Effect.gen(function* () {
    const { rows, apply } = yield* fixture
    yield* apply(SyncSignal.Upsert({ syncId: sid("3"), modelId: id, data: { ...current, subset: "unloaded" } }))
    assert.deepStrictEqual(rows.get(id), current)
  }))
})

it.effect("EpochReset Snapshot 0 clears all old rows and marks; a fresh load accepts new events", () => Effect.gen(function* () {
  const { rows, apply, coverage } = yield* fixture
  yield* apply(SyncSignal.Snapshot({ at: sid("0"), generation: 1, reason: "EpochReset" }))
  assert.strictEqual(rows.size, 0)
  assert.strictEqual((yield* Ref.get(coverage)).marks.size, 0)
  yield* Ref.set(coverage, seedCoverage([{ subset: { indexKey: "subset", keyValue: "loaded" }, at: sid("0") }]))
  yield* apply(SyncSignal.Upsert({ syncId: sid("1"), modelId: id, data: { ...current, label: "new epoch" } }))
  assert.strictEqual(rows.get(id)?.label, "new epoch")
}))

it.effect("fresh moves between covered subsets and then outside them still apply", () => Effect.gen(function* () {
  const { rows, apply, coverage } = yield* fixture
  yield* Ref.set(coverage, seedCoverage([
    { subset: { indexKey: "subset", keyValue: "loaded" }, at: sid("5") },
    { subset: { indexKey: "subset", keyValue: "other" }, at: sid("5") },
  ]))
  yield* apply(SyncSignal.Upsert({ syncId: sid("6"), modelId: id, data: { ...current, subset: "other" } }))
  assert.strictEqual(rows.get(id)?.subset, "other")
  yield* apply(SyncSignal.Upsert({ syncId: sid("7"), modelId: id, data: { ...current, subset: "outside" } }))
  assert.strictEqual(rows.size, 0)
}))


it.effect("a new-generation slice arriving before the queued reset survives it", () => Effect.gen(function* () {
  const { rows, apply, coverage } = yield* fixture
  yield* applySlice({
    entity: "Row", extractor: (row: typeof Row.Type) => row.subset, by: { subset: (row) => row.subset },
    getKey: (row) => ModelId.make(row.id), decode: Schema.decodeUnknownEffect(Row),
    currentRows: () => rows.values(), coverage,
    subset: { indexKey: "subset", keyValue: "loaded" }, rows: [{ ...current, label: "new" }], at: sid("1"), generation: 1,
    patchSynced: ({ deleteKeys, rows: incoming }) => Effect.sync(() => {
      for (const key of deleteKeys) rows.delete(key)
      for (const row of incoming) rows.set(ModelId.make(row.id), row)
    }),
  })
  yield* apply(SyncSignal.Snapshot({ at: sid("0"), generation: 1, reason: "EpochReset" }))
  assert.strictEqual(rows.get(id)?.label, "new")
  assert.strictEqual((yield* Ref.get(coverage)).generation, 1)
}))

it.effect("an older overlapping slice cannot replace or remove a newer row", () => Effect.gen(function* () {
  const { rows, coverage } = yield* fixture
  const land = (incoming: ReadonlyArray<unknown>) => applySlice({
    entity: "Row", extractor: (row: typeof Row.Type) => row.label,
    by: { subset: (row) => row.subset, label: (row) => row.label },
    getKey: (row) => ModelId.make(row.id), decode: Schema.decodeUnknownEffect(Row),
    currentRows: () => rows.values(), coverage, subset: { indexKey: "label", keyValue: "snapshot" },
    rows: incoming, at: sid("3"), generation: 0,
    patchSynced: ({ deleteKeys, rows: incomingRows }) => Effect.sync(() => {
      for (const key of deleteKeys) rows.delete(key)
      for (const row of incomingRows) rows.set(ModelId.make(row.id), row)
    }),
  })
  yield* land([{ ...current, subset: "old" }])
  assert.deepStrictEqual(rows.get(id), current)
  yield* land([])
  assert.deepStrictEqual(rows.get(id), current)
}))


it.effect("replaying one subset cannot advance unrelated coverage past a pending delete", () => Effect.gen(function* () {
  const { rows, apply, coverage, deps } = yield* fixture
  const subset = { indexKey: "subset", keyValue: "other" }
  yield* Ref.set(coverage, seedCoverage([
    { subset: { indexKey: "subset", keyValue: "loaded" }, at: sid("5") },
    { subset, at: sid("8") },
  ]))
  const replay = makePartialApplier({ ...deps, replaySubset: subset })
  yield* replay(SyncSignal.Upsert({ syncId: sid("10"), modelId: ModelId.make("other"), data: { id: "other", subset: "other", label: "replayed" } }))
  assert.strictEqual((yield* Ref.get(coverage)).marks.get("subset")?.get("loaded"), sid("5"))
  yield* apply(SyncSignal.Delete({ syncId: sid("6"), modelId: id }))
  assert.isFalse(rows.has(id))
}))
