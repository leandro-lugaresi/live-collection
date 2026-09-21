import {
    CatchupResponse,
    compareSyncId,
    type Epoch,
    type HydrateBatchRequest,
    HydrateBatchResult,
    type HydrateBatchResponse,
    HydratedSyncEventEnvelope,
    intersects,
    ResyncTarget,
    squash,
    type SyncGroup,
    type SyncId
} from "@triargos/live-collection-protocol";
import { Context, DateTime, Duration, Effect, Layer, Option, Schema, Stream } from "effect";
import * as Arr from "effect/Array";
import { makeHydrator } from "./hydrator.js";
import { ModelRegistry } from "./model-registry.js";
import { SyncEventStore } from "./sync-event-store.js";

/**
 * A batch request named an index the registry does not declare for that model — a
 * malformed request born of config drift between client and server vocabularies.
 * Loud by design: the whole batch fails (the app route answers 400), never a silent
 * empty result.
 */
export class UnknownIndexError extends Schema.TaggedError<UnknownIndexError>()(
  "UnknownIndexError",
  { modelName: Schema.String, indexKey: Schema.String }
) {}

/**
 * The read-side entry — the surfaces the client contract observes. The
 * app's routes own auth and resolve the caller's sync groups server-side; the
 * feed owns everything the client's correctness depends on.
 */
export interface SyncFeedShape {
  /**
   * One catchup page: `listEvents` → `intersects` visibility filter → `squash`
   * → hydrate (batched, `Option.none` → synthetic `Delete`, unknown model →
   * log + drop) → `{ events, lastSyncId, epoch }`. A cursor that predates
   * retention (`CursorOutOfRetentionError`) becomes a single synthetic
   * `Resync(All)` — synthesized inline, never written to the log, never an
   * error to the route.
   */
  readonly catchup: (args: {
    readonly fromSyncId: SyncId
    readonly syncGroups: ReadonlyArray<SyncGroup>
  }) => Effect.Effect<CatchupResponse>

  /**
   * Ordered durable batches. Empty batches are heartbeats. Polling recovers commits
   * even if publication failed. Resync/epoch change emits one final batch and closes.
   */
  readonly streamEvents: (args: {
    readonly fromSyncId: SyncId
    readonly epoch: Option.Option<Epoch>
    readonly syncGroups: ReadonlyArray<SyncGroup>
    readonly pollInterval?: Duration.Input
  }) => Stream.Stream<string>

  /**
   * One partial-index batch: head-read (`getLatestSyncId`) FIRST — the stamp's
   * safety: every fetched row reflects at least that position — then per request:
   * resolve model + declared index (miss ⇒ fail the whole batch with
   * {@link UnknownIndexError}) → run the index fetch with the caller's syncGroups →
   * `Option.none` ⇒ `Forbidden`, `Option.some` ⇒ `Members` with rows encoded via the
   * descriptor schema. No event-store reads beyond the head.
   */
  readonly hydrateBatch: (args: {
    readonly request: HydrateBatchRequest
    readonly syncGroups: ReadonlyArray<SyncGroup>
  }) => Effect.Effect<HydrateBatchResponse, UnknownIndexError>
}

const encodeBatch = Schema.encodeEffect(Schema.fromJsonString(CatchupResponse))

const make: Effect.Effect<SyncFeedShape, never, SyncEventStore | ModelRegistry> =
  Effect.gen(function* () {
    const store = yield* SyncEventStore
    const registry = yield* ModelRegistry
    const hydrator = makeHydrator(registry)

    const catchup: SyncFeedShape["catchup"] = Effect.fn("SyncFeed.catchup")(function* (args) {
      const epoch = yield* store.getCurrentEpoch
      const lastSyncId = yield* store.getLatestSyncId
      const response = yield* store.listEvents({ cursor: args.fromSyncId }).pipe(
        Effect.flatMap((listed) =>
          Effect.gen(function* () {
            const visible = listed.filter((event) =>
              compareSyncId(event.syncId, lastSyncId) <= 0 && intersects(event.syncGroups, args.syncGroups)
            )
            const events = yield* hydrator.hydrateEvents({
              events: squash(visible),
              syncGroups: args.syncGroups
            })
            return { events, lastSyncId, epoch }
          })
        ),
        Effect.catchTag("CursorOutOfRetentionError", () =>
          Effect.gen(function* () {
            yield* Effect.logInfo(
              `Catchup cursor ${args.fromSyncId} predates retention; answering with Resync(All)`
            )
            // A caller without any sync group holds no visible data to reset.
            const createdAt = yield* DateTime.nowAsDate
            const events = Arr.isReadonlyArrayNonEmpty(args.syncGroups)
              ? [
                  HydratedSyncEventEnvelope.cases.Resync.make({
                    target: ResyncTarget.cases.All.make({}),
                    syncGroups: args.syncGroups,
                    syncId: lastSyncId,
                    createdAt
                  })
                ]
              : []
            return { events, lastSyncId, epoch }
          })
        )
      )
      // A restore during this read invalidates every coordinate in the batch.
      const after = yield* store.getCurrentEpoch
      if (Option.getOrNull(epoch) !== Option.getOrNull(after)) return yield* catchup(args)
      return response
    })

    const streamEvents: SyncFeedShape["streamEvents"] = ({ fromSyncId, epoch, syncGroups, pollInterval = Duration.seconds(1) }) =>
      Stream.unwrap(Effect.sync(() => {
        // Per-connection state. There is no concurrent replay/live path or buffer.
        let state:
          | { readonly _tag: "Reading"; readonly cursor: SyncId; readonly epoch: Option.Option<Epoch> }
          | { readonly _tag: "Closed" } = { _tag: "Reading", cursor: fromSyncId, epoch }
        return Stream.tick(pollInterval).pipe(
          Stream.mapEffect(() => Effect.gen(function* () {
            if (state._tag === "Closed") return yield* Effect.die("closed sync feed was polled")
            const batch = yield* catchup({ fromSyncId: state.cursor, syncGroups })
            const epochChanged = Option.isSome(state.epoch) && Option.getOrNull(state.epoch) !== Option.getOrNull(batch.epoch)
            const terminal = epochChanged || batch.events.some((event) => event._tag === "Resync")
            state = terminal ? { _tag: "Closed" } : { _tag: "Reading", cursor: batch.lastSyncId, epoch: batch.epoch }
            const json = yield* encodeBatch(batch).pipe(Effect.orDie)
            return { frame: `data: ${json}\n\n`, terminal }
          })),
          Stream.takeUntil(({ terminal }) => terminal),
          Stream.map(({ frame }) => frame),
        )
      })).pipe(Stream.withSpan("SyncFeed.streamEvents"))

    const hydrateBatch: SyncFeedShape["hydrateBatch"] = Effect.fn("SyncFeed.hydrateBatch")(
      function* (args) {
        const epoch = yield* store.getCurrentEpoch
        const lastSyncId = yield* store.getLatestSyncId
        const results = yield* Effect.forEach(args.request.requests, (request) => {
          const fetch = registry.models
            .get(String(request.modelName))
            ?.indexFetch?.(request.indexKey, request.keyValue, args.syncGroups)
          if (fetch === undefined) {
            return Effect.fail(
              new UnknownIndexError({ modelName: request.modelName, indexKey: request.indexKey })
            )
          }
          return fetch.pipe(
            Effect.map(
              Option.match({
                onNone: () => HydrateBatchResult.cases.Forbidden.make({ request }),
                onSome: (rows) => HydrateBatchResult.cases.Members.make({ request, rows })
              })
            )
          )
        })
        const after = yield* store.getCurrentEpoch
        if (Option.getOrNull(epoch) !== Option.getOrNull(after)) return yield* hydrateBatch(args)
        return { results, lastSyncId, epoch }
      }
    )

    return { catchup, streamEvents, hydrateBatch }
  })

export class SyncFeed extends Context.Service<SyncFeed, SyncFeedShape>()(
  "live-collection-server/SyncFeed"
) {
  /**
   * A plain constant: the registry arrives as the `ModelRegistry` service,
   * built with `ModelRegistry.layer(registry)` — which is where the
   * descriptors' repo requirements are inferred and closed.
   */
  static readonly layer: Layer.Layer<SyncFeed, never, SyncEventStore | ModelRegistry> =
    Layer.effect(SyncFeed, make)
}
