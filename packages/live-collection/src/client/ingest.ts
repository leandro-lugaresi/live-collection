import { Data, Effect, Option, type Semaphore, Stream } from "effect"
import { compareSyncId, type CatchupResponse, type HydratedSyncEventEnvelope, type SyncId, zeroSyncId } from "@triargos/live-collection-protocol"
import type { CatchupClientShape } from "./catchup-client.js"
import type { SyncJournalShape, JournalEvent } from "./sync-journal.js"
import { SyncConnectionLost, type SyncTransportShape } from "./sync-transport.js"

/**
 * What the ingest side publishes to subscriber tails. `Event`/`Resync` respect each
 * subscriber's monotonic tail guard; `EpochReset` bypasses it — after a server timeline
 * reset every mounted subscriber's guard is an old-epoch (large) syncId, so a guarded
 * item at the new-epoch (small) cursor would be silently dropped.
 */
export type PublishedItem = Data.TaggedEnum<{
  Event: { readonly row: JournalEvent }
  Resync: { readonly at: SyncId }
  EpochReset: { readonly at: SyncId }
}>
export const PublishedItem = Data.taggedEnum<PublishedItem>()

export interface RetentionOptions {
  readonly maxEventsPerModel: number
  readonly maxEventsTotal: number
  readonly trimEveryEvents: number
}

class RecoveryRequired extends Data.TaggedError("RecoveryRequired") {}

type EntityEvent = Exclude<HydratedSyncEventEnvelope, { readonly _tag: "Resync" }>

const rowFromEvent = (event: EntityEvent): JournalEvent =>
  event._tag === "Delete"
    ? { syncId: event.syncId, modelName: event.modelName, tag: "Delete", modelId: event.modelId, data: Option.none() }
    : {
        syncId: event.syncId,
        modelName: event.modelName,
        tag: event._tag,
        modelId: event.modelId,
        data: Option.some(event.data),
      }

/**
 * Single-owner network ingestion. Both HTTP catchup and SSE carry complete,
 * ordered durable batches: append all rows, publish in order, then save coverage.
 * Recovery invalidates journal metadata before fanout and survives reconnects.
 */
export const makeIngest = (deps: {
  readonly transport: SyncTransportShape
  readonly catchup: CatchupClientShape
  readonly journal: SyncJournalShape
  readonly publish: (item: PublishedItem) => Effect.Effect<void>
  readonly onEpochReset: Effect.Effect<void>
  readonly recoveryGate: Semaphore.Semaphore
  /** Flush pending last-applied marks — run before each prune so stage-2 sees fresh marks. */
  readonly flushLastApplied: Effect.Effect<void>
  readonly retention: RetentionOptions
}): Effect.Effect<void> => {
  const { transport, catchup, journal, publish, onEpochReset, flushLastApplied, retention } = deps

  // Amortized retention: prune once per `trimEveryEvents` ingested events.
  // Single-fiber by construction (only the ingest fiber touches it).
  let ingestsSinceTrim = 0
  const trimIfNeeded = (ingested: number): Effect.Effect<void> => {
    ingestsSinceTrim += ingested
    if (ingestsSinceTrim < retention.trimEveryEvents) return Effect.void
    ingestsSinceTrim = 0
    // Flush first: prune's dead-weight stage reads the durable marks, and a pending mark
    // held back by the timer would make it retain rows every collection already applied.
    return flushLastApplied.pipe(
      Effect.andThen(
        journal.prune({ maxEventsPerModel: retention.maxEventsPerModel, maxEventsTotal: retention.maxEventsTotal }),
      ),
    )
  }

  const applyCatchup = (response: CatchupResponse): Effect.Effect<void> => {
    const resyncs = response.events.filter((event) => event._tag === "Resync")
    // Any resync in the batch ⇒ everyone snapshots anyway; journaling the entities would be wasted work.
    if (resyncs.length > 0) {
      return deps.recoveryGate.withPermit(onEpochReset.pipe(
        Effect.andThen(journal.resetCoverage(response.lastSyncId)),
        Effect.andThen(publish(PublishedItem.Resync({ at: response.lastSyncId }))),
        Effect.asVoid,
      ))
    }
    // One consistent chunk: all rows durable in one append, then fanout in order, then one ingest-mark advance.
    return Effect.gen(function* () {
      const cursor = Option.getOrElse(yield* journal.getLastIngestedSyncId, () => zeroSyncId)
      const rows = response.events.filter((event): event is EntityEvent => event._tag !== "Resync" && compareSyncId(event.syncId, cursor) > 0).map(rowFromEvent)
      yield* journal.append(rows).pipe(
        Effect.andThen(Effect.forEach(rows, (row) => publish(PublishedItem.Event({ row })), { discard: true })),
        Effect.andThen(journal.setLastIngestedSyncId(response.lastSyncId)),
        Effect.andThen(trimIfNeeded(rows.length)),
        Effect.asVoid,
      )
    })
  }

  // No epoch on the wire ⇒ no checking (the backend guarantees one everlasting timeline).
  // First epoch seen ⇒ stamp it (existing state is trusted). Same ⇒ proceed. Different ⇒
  // the timeline changed identity: drop pending old-epoch marks, then the atomic
  // `resetToEpoch` wipe, then snapshot every subscriber at the new position.
  const applyEpochChecked = (response: CatchupResponse): Effect.Effect<void> =>
    Option.match(response.epoch, {
      onNone: () => applyCatchup(response),
      onSome: (epoch) =>
        journal.getEpoch.pipe(
          Effect.flatMap(
            Option.match({
              onNone: () => journal.setEpoch(epoch).pipe(Effect.andThen(applyCatchup(response))),
              onSome: (stored) =>
                stored === epoch
                  ? applyCatchup(response)
                  : deps.recoveryGate.withPermit(onEpochReset.pipe(
                      Effect.andThen(journal.resetToEpoch({ epoch, at: response.lastSyncId })),
                      Effect.andThen(publish(PublishedItem.EpochReset({ at: response.lastSyncId }))),
                      Effect.asVoid,
                    )),
            }),
          ),
        ),
    })

  // State outlives connection attempts. Recovery is also recorded in the journal
  // before its fanout, so a crash/restart cannot restore obsolete coverage.
  type State = { readonly _tag: "CatchingUp" } | { readonly _tag: "Streaming" } | { readonly _tag: "Recovering" }
  let state: State = { _tag: "CatchingUp" }

  const accept = (response: CatchupResponse): Effect.Effect<boolean, SyncConnectionLost> =>
    Effect.gen(function* () {
      const epoch = yield* journal.getEpoch
      const changed = Option.isSome(epoch) && Option.isSome(response.epoch) && epoch.value !== response.epoch.value
      // Validate the coverage claim before any write; IDs may have authorized gaps.
      let previous = zeroSyncId
      for (const event of response.events) {
        if (compareSyncId(event.syncId, previous) < 0 || compareSyncId(event.syncId, response.lastSyncId) > 0) {
          return yield* new SyncConnectionLost({ reason: "unordered or unbounded sync batch" })
        }
        previous = event.syncId
      }
      const recovery = changed || response.events.some((event) => event._tag === "Resync")
      if (recovery) state = { _tag: "Recovering" }
      yield* applyEpochChecked(response)
      return recovery
    })

  const cycle = Effect.gen(function* () {
    if (state._tag !== "Streaming") {
      const from = Option.getOrElse(yield* journal.getLastIngestedSyncId, () => zeroSyncId)
      const response = yield* catchup.fetch({ from })
      yield* accept(response)
    }
    // Catchup/Resync has installed a safe resume point. The feed replays every
    // commit after it, including commits made before this connection is opened.
    const from = Option.getOrElse(yield* journal.getLastIngestedSyncId, () => zeroSyncId)
    const epoch = yield* journal.getEpoch
    state = { _tag: "Streaming" }
    yield* Stream.runForEach(transport.connect({ from, epoch }), (response) =>
      accept(response).pipe(Effect.flatMap((recovery) =>
        recovery ? Effect.fail(new RecoveryRequired()) : Effect.void,
      )),
    )
    return yield* new SyncConnectionLost({ reason: "stream ended" })
  })

  return cycle.pipe(
    Effect.catch((error) => Effect.gen(function* () {
      const recovering = state._tag === "Recovering"
      if (!recovering) state = { _tag: "CatchingUp" }
      if (error._tag !== "RecoveryRequired") yield* Effect.logWarning(`[SyncBroker] ${error._tag}: ${error.reason}`)
      // A control frame reconnects immediately. Failed recovery requests back off,
      // retaining the recovery state for the next attempt.
      if (error._tag !== "RecoveryRequired") {
        yield* Effect.sleep("3 seconds")
      }
    })),
    Effect.forever,
  )
}
