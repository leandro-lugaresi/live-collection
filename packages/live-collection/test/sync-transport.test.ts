import { Cause, DateTime, Effect, Layer, Option, Queue, Stream } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { HttpClient, type HttpClientError, HttpClientResponse } from "effect/unstable/http"
import {
  CatchupResponse,
  Epoch,
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  SyncGroup,
  SyncId,
} from "@triargos/live-collection-protocol"
import { SyncConnectionLost, SyncTransport } from "../src/client/sync-transport.js"

/** The HTTP transport over a canned web `Response` — the SSE wire is the only fake. */
const httpTransport = (respond: () => Response): Layer.Layer<SyncTransport> =>
  SyncTransport.layer({ url: "http://test/sync", keepAlive: "5 seconds" }).pipe(
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request) => Effect.succeed(HttpClientResponse.fromWeb(request, respond()))),
      ),
    ),
  )

const env = (id: string): HydratedSyncEventEnvelope => ({
  _tag: "Insert",
  syncId: SyncId.make("1"),
  modelName: ModelName.make("Webhook"),
  modelId: ModelId.make(id),
  syncGroups: [SyncGroup.make("organization:o1")],
  createdAt: DateTime.makeUnsafe(0).pipe(DateTime.toDateUtc),
  data: { id, orgId: "o1" },
})

describe("SyncTransport", () => {
  it.effect("layerMemory surfaces enqueued events on connect, in order", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      yield* Queue.offerAll(queue, [env("a"), env("b")])
      const taken = yield* Effect.flatMap(SyncTransport, (transport) =>
        transport.connect({ from: SyncId.make("0"), epoch: Option.none() }).pipe(Stream.take(2), Stream.runCollect),
      ).pipe(Effect.provide(SyncTransport.layerMemory(queue)))
      assert.deepStrictEqual(
        taken.flatMap((batch) => batch.events.map((e) => ("modelId" in e ? e.modelId : undefined))),
        [ModelId.make("a"), ModelId.make("b")],
      )
    }))

  it.effect("an SSE event split across multiple data: lines decodes as ONE event (spec framing)", () =>
    Effect.gen(function* () {
      // One envelope split at a JSON token boundary across two `data:` lines (SSE joins them with
      // \n — legal JSON whitespace), then a normal single-line event. Blank lines dispatch.
      const first =
        `{"_tag":"Insert","syncId":"1","modelName":"Webhook","modelId":"a",` +
        `"syncGroups":["organization:o1"],"createdAt":"1970-01-01T00:00:00.000Z","data":{"id":"a"}}`
      const second = first.replace(`"modelId":"a"`, `"modelId":"b"`).replace(`"id":"a"`, `"id":"b"`)
      const firstBatch = `{"events":[${first}],"lastSyncId":"1"}`
      const secondBatch = `{"events":[${second}],"lastSyncId":"1"}`
      const splitAt = firstBatch.indexOf(`"syncGroups"`)
      const body = [
        `data: ${firstBatch.slice(0, splitAt)}`,
        `data: ${firstBatch.slice(splitAt)}`,
        ``,
        `data: ${secondBatch}`,
        ``,
        ``, // join("\n") ⇒ the wire ends "…\n\n": the second event's dispatching blank line
      ].join("\n")
      const taken = yield* Effect.flatMap(SyncTransport, (transport) =>
        transport.connect({ from: SyncId.make("0"), epoch: Option.none() }).pipe(Stream.take(2), Stream.runCollect),
      ).pipe(Effect.provide(httpTransport(() => new Response(body, { status: 200 }))))
      assert.deepStrictEqual(
        taken.flatMap((batch) => batch.events.map((e) => ("modelId" in e ? e.modelId : undefined))),
        [ModelId.make("a"), ModelId.make("b")],
      )
    }))

  it.effect("a non-2xx response fails as SyncConnectionLost carrying the status — not a silent stream end", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.flatMap(SyncTransport, (transport) => Stream.runDrain(transport.connect({ from: SyncId.make("0"), epoch: Option.none() }))).pipe(
        Effect.provide(httpTransport(() => new Response("unauthorized", { status: 401 }))),
        Effect.exit,
      )
      const error = exit._tag === "Failure" ? Cause.findErrorOption(exit.cause) : Option.none()
      if (Option.isSome(error)) {
        assert.instanceOf(error.value, SyncConnectionLost)
        assert.include(error.value.reason, "401") // the status, not "stream ended"
      } else {
        assert.fail("expected a SyncConnectionLost failure")
      }
    }))

  it.effect("a closed connection fails with SyncConnectionLost (the reconnect signal)", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      yield* Queue.shutdown(queue)
      const exit = yield* Effect.flatMap(SyncTransport, (transport) =>
        Stream.runDrain(transport.connect({ from: SyncId.make("0"), epoch: Option.none() })),
      ).pipe(Effect.provide(SyncTransport.layerMemory(queue)), Effect.exit)
      assert.isTrue(exit._tag === "Failure")
      const error = exit._tag === "Failure" ? Cause.findErrorOption(exit.cause) : Option.none()
      if (Option.isSome(error)) {
        assert.instanceOf(error.value, SyncConnectionLost)
      } else {
        assert.fail("expected a SyncConnectionLost failure")
      }
    }))
})

it.effect("relative endpoint and resume parameters reach the platform client intact", () => Effect.gen(function* () {
  const client = HttpClient.makeWith<HttpClientError.HttpClientError, never, HttpClientError.HttpClientError, never>((request) => request.pipe(Effect.map((value) => {
    assert.strictEqual(value.url, "/api/sync?tenant=demo")
    assert.deepStrictEqual(value.urlParams.params, [["from", "100"], ["epoch", "A & B"]])
    return HttpClientResponse.fromWeb(value, new Response('data: {"events":[],"lastSyncId":"100","epoch":"A & B"}\n\n'))
  })), Effect.succeed)
  const transport = SyncTransport.layer({ url: "/api/sync?tenant=demo", keepAlive: "5 seconds" }).pipe(
    Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
  )
  const batches = yield* Effect.flatMap(SyncTransport, (service) => service.connect({
    from: SyncId.make("100"), epoch: Option.some(Epoch.make("A & B")),
  }).pipe(Stream.take(1), Stream.runCollect)).pipe(Effect.provide(transport))
  assert.strictEqual(batches[0]?.lastSyncId, "100")
}))

it.effect("a malformed batch fails before a later valid checkpoint can be emitted", () => Effect.gen(function* () {
  const seen: Array<CatchupResponse> = []
  const outcome = yield* Effect.flatMap(SyncTransport, (transport) => transport.connect({ from: SyncId.make("0"), epoch: Option.none() }).pipe(
    Stream.runForEach((batch) => Effect.sync(() => { seen.push(batch) })), Effect.exit,
  )).pipe(Effect.provide(httpTransport(() => new Response('data: {"broken":true}\n\ndata: {"events":[],"lastSyncId":"102"}\n\n'))))
  assert.strictEqual(outcome._tag, "Failure")
  assert.deepStrictEqual(seen, [])
}))
