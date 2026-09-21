import { assert, describe, it } from "@effect/vitest"
import { Deferred, Effect, Layer, Option, Queue, Stream } from "effect"
import { TestClock } from "effect/testing"
import { type CatchupResponse, Epoch, ModelName, ResyncTarget, SyncId, deriveGroup } from "@triargos/live-collection-protocol"
import { CatchupClient, CatchupFailed } from "../src/client/catchup-client.js"
import { SyncBroker, type SyncSignal } from "../src/client/sync-broker.js"
import { SyncJournal } from "../src/client/sync-journal.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { SchemaVersion } from "../src/core/schema-version.js"
import { globalKey } from "../src/core/collection-key.js"

const sid = SyncId.make
const epochA = Epoch.make("A")
const epochB = Epoch.make("B")
const version = SchemaVersion.make(1)
const modelName = ModelName.make("Row")
const empty = (at: string, epoch = epochA): CatchupResponse => ({ events: [], lastSyncId: sid(at), epoch: Option.some(epoch) })

const harness = <A>(body: (args: {
  readonly inputs: Queue.Queue<CatchupResponse>
  readonly connections: Queue.Queue<string>
  readonly catches: Queue.Queue<string>
  readonly failCatchup: (fail: boolean) => void
  readonly respond: (response: CatchupResponse) => void
}) => Effect.Effect<A, never, SyncBroker | SyncJournal>) => Effect.scoped(Effect.gen(function* () {
  const inputs = yield* Queue.unbounded<CatchupResponse>()
  const connections = yield* Queue.unbounded<string>()
  const catches = yield* Queue.unbounded<string>()
  let fail = false
  let response = empty("100")
  const base = Layer.mergeAll(SyncJournal.layerMemory, Layer.succeed(CatchupClient, {
    fetch: ({ from }) => Effect.gen(function* () {
      yield* Queue.offer(catches, from)
      if (fail) return yield* new CatchupFailed({ from, reason: "offline recovery" })
      return response
    }),
  }), Layer.succeed(SyncTransport, {
    connect: ({ from }) => Stream.unwrap(Queue.offer(connections, from).pipe(Effect.as(Stream.fromEffectRepeat(Queue.take(inputs))))),
  }))
  yield* body({ inputs, connections, catches, failCatchup: (value) => { fail = value }, respond: (value) => { response = value } }).pipe(
    Effect.provide(SyncBroker.layer().pipe(Layer.provideMerge(base))),
  )
}))

const subscribe = Effect.gen(function* () {
  const broker = yield* SyncBroker
  const seen = yield* Queue.unbounded<SyncSignal>()
  yield* broker.attachSubscriber({ modelName, scope: Option.none(), schemaVersion: version,
    apply: (signal) => Queue.offer(seen, signal).pipe(Effect.asVoid),
  }).pipe(Effect.forkChild)
  return seen
})

describe("durable recovery", () => {
  it.effect("validates epoch on an idle live batch with cursor A/100 and resumes immediately at B/1", () => harness(({ inputs, connections, respond }) => Effect.gen(function* () {
    const journal = yield* SyncJournal
    const broker = yield* SyncBroker
    yield* journal.setEpoch(epochA)
    yield* journal.setLastIngestedSyncId(sid("100"))
    const seen = yield* subscribe
    yield* Queue.take(seen) // mounted at A/100
    yield* broker.start.pipe(Effect.forkChild)
    assert.strictEqual(yield* Queue.take(connections), "100")
    respond(empty("1", epochB))
    yield* Queue.offer(inputs, empty("1", epochB))
    const reset = yield* Queue.take(seen)
    assert.deepStrictEqual(reset, { _tag: "Snapshot", at: sid("1"), generation: 1, reason: "EpochReset" })
    assert.strictEqual(yield* Queue.take(connections), "1")
    assert.deepStrictEqual(yield* journal.getEpoch, Option.some(epochB))
    assert.deepStrictEqual(yield* journal.getLastIngestedSyncId, Option.some(sid("1")))
  })))

  it.effect("Resync interrupts delivery; failed reconnect catchups preserve recovery and retry", () => harness(({ inputs, connections, catches, failCatchup, respond }) => Effect.gen(function* () {
    const journal = yield* SyncJournal
    const broker = yield* SyncBroker
    const seen = yield* subscribe
    yield* Queue.take(seen)
    respond(empty("0"))
    yield* broker.start.pipe(Effect.forkChild)
    yield* Queue.take(connections)
    yield* Queue.take(catches)
    failCatchup(true)
    yield* Queue.offer(inputs, { ...empty("5"), events: [{ _tag: "Resync", syncId: sid("5"), target: ResyncTarget.cases.All.make({}), syncGroups: [deriveGroup(["user", "a"])], createdAt: new Date(0) }] })
    const reset = yield* Queue.take(seen)
    assert.strictEqual(reset._tag, "Snapshot")
    assert.strictEqual(yield* Queue.take(catches), "5")
    assert.deepStrictEqual(yield* journal.getLastResync, Option.some(sid("5")))
    yield* TestClock.adjust("3 seconds")
    assert.strictEqual(yield* Queue.take(catches), "5")
    failCatchup(false)
    respond(empty("5"))
    yield* TestClock.adjust("3 seconds")
    assert.strictEqual(yield* Queue.take(connections), "5")
    assert.deepStrictEqual(yield* journal.getLastIngestedSyncId, Option.some(sid("5")))
  })))

  it.effect("an old in-flight snapshot cannot acknowledge epoch A after reset to B", () => harness(({ inputs, connections, respond }) => Effect.gen(function* () {
    const journal = yield* SyncJournal
    const broker = yield* SyncBroker
    yield* journal.setEpoch(epochA)
    yield* journal.setLastIngestedSyncId(sid("100"))
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const reset = yield* Deferred.make<void>()
    yield* broker.attachSubscriber({ modelName, scope: Option.none(), schemaVersion: version,
      apply: (signal) => signal._tag === "Snapshot" && signal.reason === "Mount"
        ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
        : Deferred.succeed(reset, undefined).pipe(Effect.asVoid),
    }).pipe(Effect.forkChild)
    yield* Deferred.await(entered)
    yield* broker.start.pipe(Effect.forkChild)
    yield* Queue.take(connections)
    respond(empty("1", epochB))
    yield* Queue.offer(inputs, empty("1", epochB))
    yield* Queue.take(connections) // reset is durable; the old apply is still suspended
    yield* Deferred.succeed(release, undefined)
    yield* Deferred.await(reset)
    yield* TestClock.adjust("100 millis")
    assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key: globalKey("Row"), schemaVersion: version }), Option.some(sid("1")))
  })))
})
