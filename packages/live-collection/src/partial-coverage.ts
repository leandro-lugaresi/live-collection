import { Effect, Option, Ref, Schema } from "effect"
import { compareSyncId, maxSyncId, type ModelId, type SyncId } from "@triargos/live-collection-protocol"
import type { SubsetKey } from "./core/collection-key.js"
import type { SyncSignal } from "./client/sync-signal.js"
import { SyncSignal as Signal } from "./client/sync-signal.js"

type Marks = ReadonlyMap<string, ReadonlyMap<string, SyncId>>

/** A broker recovery generation and the subsets proven current within it. */
export interface CoverageState {
  readonly generation: number
  readonly marks: Marks
}

/** Initial coverage, before any subset has loaded. */
export const emptyCoverage: CoverageState = { generation: 0, marks: new Map() }

/** Restore durable subset marks within the current broker generation. */
export const seedCoverage = (
  marks: ReadonlyArray<{ readonly subset: SubsetKey; readonly at: SyncId }>,
  generation = 0,
): CoverageState => {
  const state = new Map<string, Map<string, SyncId>>()
  for (const { subset, at } of marks) {
    const values = state.get(subset.indexKey) ?? new Map<string, SyncId>()
    values.set(subset.keyValue, at)
    state.set(subset.indexKey, values)
  }
  return { generation, marks: state }
}

/** Merge hydration and concurrent ensures, discarding all coordinates from older generations. */
export const mergeCoverage = (a: CoverageState, b: CoverageState): CoverageState => {
  if (a.generation !== b.generation) return a.generation > b.generation ? a : b
  let merged = a
  for (const [indexKey, values] of b.marks) {
    for (const [keyValue, at] of values) merged = advance(merged, { indexKey, keyValue }, at)
  }
  return merged
}

/** Every subset whose rows this coverage describes. */
export const coveredSubsets = (state: CoverageState): ReadonlyArray<SubsetKey> =>
  [...state.marks].flatMap(([indexKey, values]) => [...values.keys()].map((keyValue) => ({ indexKey, keyValue })))

const advance = (state: CoverageState, subset: SubsetKey, at: SyncId): CoverageState => {
  const values = new Map(state.marks.get(subset.indexKey) ?? [])
  const current = values.get(subset.keyValue)
  values.set(subset.keyValue, current === undefined ? at : maxSyncId(current, at))
  return { ...state, marks: new Map(state.marks).set(subset.indexKey, values) }
}

const coveringMarks = <T>(state: CoverageState, by: Record<string, (row: T) => string>, row: T): ReadonlyArray<SyncId> =>
  Object.entries(by).flatMap(([indexKey, extract]) => {
    const mark = state.marks.get(indexKey)?.get(extract(row))
    return mark === undefined ? [] : [mark]
  })

/** The authoritative synced-store operations used by the partial drain. */
export interface PartialWrite<T> {
  readonly has: (id: ModelId) => boolean
  readonly currentRows: () => IterableIterator<T>
  readonly writeSynced: (row: T) => Effect.Effect<void>
  readonly deleteSynced: (id: ModelId) => Effect.Effect<void>
  readonly replaceSynced: (rows: ReadonlyArray<T>) => Effect.Effect<void>
}

/**
 * Apply a signal under the collection's application gate. Freshness is checked
 * against both old and incoming membership before any destructive operation.
 * A subset replay advances only that subset; ordered live delivery advances all.
 */
export const makePartialApplier = <T extends object>(deps: {
  readonly entity: string
  readonly by: Record<string, (entity: T) => string>
  readonly getKey: (entity: T) => ModelId
  readonly decode: (data: unknown) => Effect.Effect<T, Schema.SchemaError>
  readonly write: PartialWrite<T>
  readonly coverage: Ref.Ref<CoverageState>
  readonly replaySubset?: SubsetKey
}): ((signal: SyncSignal) => Effect.Effect<ReadonlyArray<SubsetKey>>) => {
  const { entity, by, getKey, decode, write, coverage } = deps
  const ack = (at: SyncId) => Ref.modify(coverage, (state) => {
    const subsets = deps.replaySubset === undefined ? coveredSubsets(state) : [deps.replaySubset]
    return [subsets, subsets.reduce((next, subset) => advance(next, subset, at), state)] as const
  })
  const currentRow = (id: ModelId) => [...write.currentRows()].find((row) => getKey(row) === id)

  return Signal.$match({
    Snapshot: ({ at, generation }) => Effect.gen(function* () {
      const state = yield* Ref.get(coverage)
      if (generation < state.generation) return []
      const kept: CoverageState = generation > state.generation
        ? { generation, marks: new Map() }
        : { generation, marks: new Map([...state.marks].map(([indexKey, values]) =>
          [indexKey, new Map([...values].filter(([, mark]) => compareSyncId(mark, at) >= 0))],
        )) }
      // A new-generation ensure can beat the queued reset signal. Its rows are
      // already fresh and must survive that signal; old-generation rows never do.
      yield* write.replaceSynced([...write.currentRows()].filter((row) => coveringMarks(kept, by, row).length > 0))
      yield* Ref.set(coverage, kept)
      return []
    }),
    Delete: ({ syncId, modelId }) => Effect.gen(function* () {
      const state = yield* Ref.get(coverage)
      const row = currentRow(modelId)
      if (row !== undefined && coveringMarks(state, by, row).every((mark) => compareSyncId(syncId, mark) > 0)) {
        yield* write.deleteSynced(modelId)
      }
      return yield* ack(syncId)
    }),
    Upsert: ({ syncId, data }) => decode(data).pipe(
      Effect.flatMap((row) => Effect.gen(function* () {
        const state = yield* Ref.get(coverage)
        const previous = currentRow(getKey(row))
        const incomingMarks = coveringMarks(state, by, row)
        const previousMarks = previous === undefined ? [] : coveringMarks(state, by, previous)
        const stale = [...incomingMarks, ...previousMarks].some((mark) => compareSyncId(syncId, mark) <= 0)
        if (!stale) {
          if (incomingMarks.length > 0) yield* write.writeSynced(row)
          else if (previous !== undefined) yield* write.deleteSynced(getKey(row))
        }
        return yield* ack(syncId)
      })),
      Effect.catchTag("SchemaError", (error) => Effect.logWarning(
        `[defineCollection] skipping undecodable ${entity} event #${syncId}: ${error.message}`,
      ).pipe(Effect.andThen(ack(syncId)))),
    ),
  })
}

/**
 * Land a fetched slice under the application gate. Preserve rows proved newer by
 * another overlapping subset. A new recovery generation replaces the old cache.
 * Failed fetches never call this function and therefore never activate coverage.
 */
export const applySlice = <T extends object>(deps: {
  readonly entity: string
  readonly extractor: (entity: T) => string
  readonly by: Record<string, (entity: T) => string>
  readonly getKey: (entity: T) => ModelId
  readonly decode: (data: unknown) => Effect.Effect<T, Schema.SchemaError>
  readonly currentRows: () => IterableIterator<T>
  readonly patchSynced: (args: { readonly deleteKeys: ReadonlyArray<ModelId>; readonly rows: ReadonlyArray<T> }) => Effect.Effect<void>
  readonly coverage: Ref.Ref<CoverageState>
  readonly subset: SubsetKey
  readonly rows: ReadonlyArray<unknown>
  readonly at: SyncId
  readonly generation: number
}): Effect.Effect<void> => Effect.gen(function* () {
  const decoded = yield* Effect.forEach(deps.rows, (raw) => deps.decode(raw).pipe(
    Effect.map(Option.some),
    Effect.catchTag("SchemaError", (error) => Effect.logWarning(
      `[defineCollection] skipping undecodable ${deps.entity} slice row: ${error.message}`,
    ).pipe(Effect.as(Option.none<T>()))),
  ))
  const previous = yield* Ref.get(deps.coverage)
  if (deps.generation < previous.generation) return
  const reset = deps.generation > previous.generation
  const state = reset ? { generation: deps.generation, marks: new Map() } : previous
  const current = [...deps.currentRows()]
  const newer = (row: T) => coveringMarks(state, deps.by, row).some((mark) => compareSyncId(mark, deps.at) > 0)
  const allRows = decoded.flatMap(Option.toArray)
  const rows = allRows.filter((row) => {
    const old = current.find((candidate) => deps.getKey(candidate) === deps.getKey(row))
    return !newer(row) && (old === undefined || !newer(old))
  })
  const fetchedKeys = new Set(allRows.map(deps.getKey))
  const deleteKeys = current.filter((row) => reset || (
    deps.extractor(row) === deps.subset.keyValue && !fetchedKeys.has(deps.getKey(row)) && !newer(row)
  )).map(deps.getKey)
  yield* deps.patchSynced({ deleteKeys, rows })
  yield* Ref.set(deps.coverage, advance(state, deps.subset, deps.at))
})
