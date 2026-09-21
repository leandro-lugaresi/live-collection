import { Context, Effect, Layer, PubSub, Stream } from "effect"
import type { SyncEvent } from "@triargos/live-collection-protocol"

/**
 * Optional in-process fanout from writers to application subscribers. SyncFeed reads
 * the durable event store directly; missed publication is recovered by its next poll.
 * Multi-node application consumers may supply another adapter.
 */
export interface SyncEventBusShape {
  readonly publish: (event: SyncEvent) => Effect.Effect<void>
  /**
   * The live tail. Each run is one independent subscriber: it registers on first
   * pull and unregisters when the stream ends or is interrupted, so an adapter must
   * not hand out a shared, already-subscribed stream — a dropped SSE connection has
   * to release its subscriber, or the bus keeps feeding a queue nobody drains.
   *
   * Events published before a run's first pull are not delivered to it. That is safe
   * only for consumers that independently recover from the durable event log.
   */
  readonly events: Stream.Stream<SyncEvent>
}

const makeMemory: Effect.Effect<SyncEventBusShape> = Effect.gen(function* () {
  const pubSub = yield* PubSub.unbounded<SyncEvent>()
  return {
    publish: (event) => PubSub.publish(pubSub, event).pipe(Effect.asVoid),
    events: Stream.fromPubSub(pubSub)
  }
})

export class SyncEventBus extends Context.Service<SyncEventBus, SyncEventBusShape>()(
  "live-collection-server/SyncEventBus"
) {
  static readonly layerMemory: Layer.Layer<SyncEventBus> = Layer.effect(SyncEventBus, makeMemory)
}
