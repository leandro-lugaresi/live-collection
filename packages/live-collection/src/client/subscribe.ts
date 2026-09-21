import { Effect, Option, PubSub, type Semaphore, Stream } from "effect"
import { maxSyncId, type ModelName } from "@triargos/live-collection-protocol"
import type { SchemaVersion } from "../core/schema-version.js"
import { type CollectionKey, globalKey, scopedKey } from "../core/collection-key.js"
import type { SyncJournalShape } from "./sync-journal.js"
import type { PublishedItem } from "./ingest.js"
import type { LastAppliedTracker } from "./last-applied-tracker.js"
import { MountDecision, concernsModel, dropStale, planMount, signalFromRow } from "./mount-plan.js"
import { SyncSignal } from "./sync-signal.js"

/** Scoped collections key their persisted rows (and last-applied marks) per scope value. */
export const keyFor = (modelName: ModelName, scope: Option.Option<string>): CollectionKey<unknown> =>
  Option.match(scope, {
    onNone: () => globalKey(modelName),
    onSome: (value) => scopedKey({ entity: modelName, scope: value }),
  })

/** A delivery is meaningful only in the broker generation that produced it. */
export interface Delivery<A> {
  readonly value: A
  readonly generation: number
}

/**
 * The SERVE machine — assembles one mount stream per subscriber: the replay slice the
 * collection is missing, then the live tail, as one seamless stream.
 *
 * Ordering invariant owned here: the PubSub subscription is established BEFORE any
 * journal/meta read, so no event can fall between "read the journal" and "start tailing" —
 * the overlap is deduped by the tail guard instead.
 */
export const makeSubscribe =
  (deps: {
    readonly journal: SyncJournalShape
    readonly published: PubSub.PubSub<Delivery<PublishedItem>>
    readonly current: LastAppliedTracker["current"]
    readonly generation: () => number
    readonly gate: Semaphore.Semaphore
  }) =>
  (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly schemaVersion: SchemaVersion
    readonly initialize: Effect.Effect<void>
  }): Stream.Stream<Delivery<SyncSignal>> =>
    Stream.unwrap(
      deps.gate.withPermit(Effect.gen(function* () {
        const { modelName, scope, schemaVersion } = args
        const queue = yield* PubSub.subscribe(deps.published)
        const generation = deps.generation()
        yield* args.initialize
        const key = keyFor(modelName, scope)
        const plan = planMount({
          collectionLastApplied: yield* deps.current({ key, schemaVersion }),
          lastIngested: yield* deps.journal.getLastIngestedSyncId,
          highestPruned: yield* deps.journal.highestPrunedSyncId(modelName),
          lastResyncAt: yield* deps.journal.getLastResync,
        })
        const rows = yield* deps.journal.read({ modelName, since: plan.since })
        const tailGuard = rows.reduce((guard, row) => maxSyncId(guard, row.syncId), plan.tailGuardSeed)
        const replay = [
          ...MountDecision.$match(plan.decision, {
            Skip: () => [],
            Replay: () => [],
            Snapshot: ({ at }) => [SyncSignal.Snapshot({ at, generation, reason: "Mount" })],
          }),
          ...rows.map(signalFromRow),
        ]
        const tail = Stream.fromSubscription(queue).pipe(
          Stream.filter(({ value }) => concernsModel(modelName)(value)),
          Stream.mapAccum(() => tailGuard, (guard, item) => {
            const [next, signals] = dropStale(guard, item.value, item.generation)
            return [next, signals.map((value) => ({ value, generation: item.generation }))]
          }),
        )
        return Stream.concat(Stream.fromIterable(replay.map((value) => ({ value, generation }))), tail)
      })),
    )
