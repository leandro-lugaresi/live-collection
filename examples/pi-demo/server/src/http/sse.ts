import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { Effect, Result, Schema, Stream } from "effect"
import { SyncResumeRequest } from "@triargos/live-collection-protocol"
import { sessionGroup } from "@pi-demo/shared"
import { SyncFeed } from "@triargos/live-collection-server"
import { sessionCodeFromRequest } from "./session-auth.js"

export const SseRoute = HttpRouter.add("GET", "/api/sync", (request) =>
  Effect.gen(function* () {
    const decoded = yield* Effect.result(sessionCodeFromRequest(request))
    if (Result.isFailure(decoded)) return HttpServerResponse.empty({ status: 401 })

    const resume = yield* Effect.result(Schema.decodeUnknownEffect(SyncResumeRequest)(
      HttpServerRequest.searchParamsFromURL(new URL(request.url, "http://sync.local")),
    ))
    if (Result.isFailure(resume)) return HttpServerResponse.empty({ status: 400 })
    const feed = yield* SyncFeed
    return HttpServerResponse.stream(
      feed.streamEvents({ fromSyncId: resume.success.from, epoch: resume.success.epoch, syncGroups: [sessionGroup(decoded.success)] }).pipe(Stream.encodeText),
      {
        contentType: "text/event-stream",
        headers: {
          "cache-control": "no-cache",
          connection: "keep-alive"
        }
      }
    )
  })
)
