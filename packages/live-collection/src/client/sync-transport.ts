import { CatchupResponse, type HydratedSyncEventEnvelope, type SyncResumeRequest } from "@leandro-lugaresi/live-collection-protocol";
import { Context, type Duration, Effect, Layer, Option, Queue, Schema, Stream } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

/**
 * The live connection dropped — it ended, errored, or fell silent past the keep-alive
 * window. It is **expected**, not exceptional: the broker's retry catches it, re-runs
 * catchup to heal the disconnect gap, and reconnects. The `reason` carries why, for logs.
 */
export class SyncConnectionLost extends Schema.TaggedError<SyncConnectionLost>()("SyncConnectionLost", {
  reason: Schema.String,
}) {}

/**
 * The app-wide durable batch stream. Malformed frames and connection loss fail the
 * stream; the broker resumes from its last safely covered cursor. Relative endpoint
 * URLs are passed to the platform HttpClient without constructing an absolute URL.
 */
export interface SyncTransportShape {
  /** Resume replay/live delivery from a durable cursor and timeline identity. */
  readonly connect: (resume: SyncResumeRequest) => Stream.Stream<CatchupResponse, SyncConnectionLost>
}

const decodeEvent = Schema.decodeEffect(Schema.fromJsonString(CatchupResponse))

const makeHttp = (config: {
  readonly url: string
  readonly keepAlive: Duration.Input
}): Effect.Effect<SyncTransportShape, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    // filterStatusOk: a non-2xx response is a connection failure (carrying the status), not an
    // empty SSE stream silently retried as "stream ended".
    const client = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk)
    // Every line resets the keep-alive timer: SSE servers emit `:` comment pings, so a gap longer
    // than `keepAlive` means the connection is dead even though no error surfaced.
    const connect: SyncTransportShape["connect"] = (resume) => {
      const lines = HttpClientResponse.stream(client.get(config.url, {
        urlParams: { from: resume.from, ...(Option.isSome(resume.epoch) ? { epoch: resume.epoch.value } : {}) },
      })).pipe(
        Stream.decodeText(),
        Stream.splitLines,
        Stream.timeoutOrElse({
          duration: config.keepAlive,
          orElse: () => Stream.fail(new SyncConnectionLost({ reason: "keep-alive timeout" })),
        }),
      )
      // SSE framing: an event's payload is the \n-join of its consecutive `data:` lines, dispatched at
      // the first empty line (one leading space after the colon is stripped, per spec). Comment and
      // other field lines reset the keep-alive above but contribute nothing; pending data at stream
      // end (no closing blank line) is discarded, also per spec.
      const payloads = lines.pipe(
        Stream.mapAccum(() => [] as ReadonlyArray<string>, (pending, line) => {
          if (line.length === 0) {
            return [[], pending.length === 0 ? [] : [pending.join("\n")]]
          }
          if (line.startsWith("data:")) {
            return [[...pending, line.slice("data:".length).replace(/^ /, "")], []]
          }
          return [pending, []]
        }),
      )
      return payloads.pipe(
        Stream.filter((payload) => payload.length > 0),
        // A bad batch may contain the only copy of a change; reconnect from the last
        // covered cursor instead of skipping it and acknowledging a later batch.
        Stream.mapEffect((payload) => decodeEvent(payload)),
        Stream.mapError((error) =>
          error._tag === "SyncConnectionLost" ? error : new SyncConnectionLost({ reason: error.message }),
        ),
        // A server-closed stream is still a drop — surface it so the broker reconnects.
        Stream.concat(Stream.fail(new SyncConnectionLost({ reason: "stream ended" }))),
      )
    }
    return { connect }
  })

/**
 * The live-stream service tag. Provide one of its layers as part of the `loop` layer
 * handed to `makeLiveRuntime`:
 *
 * @example
 * ```ts
 * SyncTransport.layer({ url: "/api/sync", keepAlive: "45 seconds" })
 * // requires an HttpClient, e.g.:  Layer.provide(FetchHttpClient.layer)
 * ```
 */
export class SyncTransport extends Context.Service<SyncTransport, SyncTransportShape>()("SyncTransport") {
  /**
   * SSE default: `GET {url}` as a server-sent-event stream over the platform
   * `HttpClient` (provide e.g. `FetchHttpClient.layer`). Set `keepAlive` above your
   * server's ping interval — a silence longer than it counts as a dropped connection.
   */
  static readonly layer = (config: {
    readonly url: string
    readonly keepAlive: Duration.Input
  }): Layer.Layer<SyncTransport, never, HttpClient.HttpClient> => Layer.effect(SyncTransport, makeHttp(config))

  /**
   * Test adapter for already ordered, gap-free events: each becomes one covered
   * batch. It is not a replay-capable production transport. For recovery tests,
   * inject a batch transport with Layer.succeed and a durable source.
   */
  static readonly layerMemory = (
    events: Queue.Dequeue<HydratedSyncEventEnvelope>,
  ): Layer.Layer<SyncTransport> =>
    Layer.succeed(SyncTransport, {
      connect: (resume) => Stream.fromEffectRepeat(Queue.take(events)).pipe(
        Stream.map((event): CatchupResponse => ({ events: [event], lastSyncId: event.syncId, epoch: resume.epoch })),
        Stream.catchCause(() => Stream.fail(new SyncConnectionLost({ reason: "transport closed" }))),
        Stream.concat(Stream.fail(new SyncConnectionLost({ reason: "transport closed" }))),
      ),
    })
}
