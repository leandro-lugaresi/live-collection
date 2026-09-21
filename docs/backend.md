# The backend contract

The library ships no server and doesn't care how yours is built. This page specifies the only thing the client can observe: **two HTTP surfaces and the invariants its correctness depends on**. Satisfy these and any backend works.

Import [`@triargos/live-collection-protocol`](./protocol.md) for the schemas to decode/encode at your edges. Backends on Effect can skip most of the hand-rolling with [`@triargos/live-collection-server`](../packages/server/README.md), which implements this contract as code — see [the kernel](#the-kernel-package) below.

## `GET /catchup?from=<syncId>` — one-shot backfill

The client sends its stored cursor and expects a JSON `CatchupResponse`: `{ events, lastSyncId, epoch? }`.

- **`events`** — every event since `from` that the caller may see, in `syncId` order, **hydrated**: `Insert`/`Update` carry the entity's *current* data; an entity that is gone or no longer visible arrives as a `Delete`. Compact the page with the protocol's `squash` before hydrating — one event per entity, not its history.
- **`lastSyncId`** — a committed prefix head read before listing; include only events at or below it. The client stores it durably as its next cursor.
- **`epoch`** — required only if your log's history can reset (see [invariants](#invariants)).
- **Retention** — if `from` predates what you retain, return a single synthetic `Resync` event with target `All` (not written to your log) plus the current `lastSyncId`. The client wipes and re-bootstraps.

The request is `{ from }` and nothing else — there is deliberately no group parameter. Resolve the caller's visibility from their auth on every call; never trust a client-supplied group set.

A failed catchup retries before streaming; it cannot be bypassed by a newer live event.

## `GET /sync?from=<syncId>&epoch=<epoch>` — durable SSE batches

Decode query parameters with `SyncResumeRequest`. Each SSE `data:` payload is one
JSON-encoded `CatchupResponse`. The client supplies its safely covered cursor and
optional epoch, never its own authorization groups.

The feed repeatedly reads a committed head, lists the bounded range, filters groups,
squashes and hydrates with authorization. The next read begins at that head. Empty
batches are heartbeats; `pollInterval` defaults to one second and must undercut the
client silence timeout. Failed known-model encoding aborts the batch. No frame can
silently certify an omitted change.

A Resync or epoch change emits a final batch and closes the stream. The client also
interrupts it and retries recovery. Events committed without bus publication remain
reachable on the next poll. See [synchronization](./synchronization.md) for the full
coverage proof and committed-prefix requirement.

Resolve groups from authentication on each connection. An open stream retains that
context; the application must terminate or re-authorize it on session/group changes.
Hydration remains an authoritative row-level access check. A Resync alone does not
revoke the authorization context of an existing connection.

## Invariants

- **`SyncId` semantics.** Monotonically increasing within one log timeline, gap-tolerant (clients order by it, never assume `n+1`), encoded as a canonical decimal string, compared numerically (`compareSyncId`) — never lexically.
- **Epoch.** SyncIds are only comparable within one timeline. If your log's history can be destroyed or replaced — an in-memory store that resets on restart, a truncation, a backup restore — mint an opaque `Epoch` and return it on every catchup and streamed batch. The client detects a change and self-heals by wiping local sync state and re-bootstrapping. Without it, clients holding old cursors freeze silently: every new event is minted "below" their stale head and discarded. It is *not* a software version — a redeploy over a durable log must not change it; a backup restore must.
- **The durable log is authoritative.** HTTP catchup and SSE share the same ordered, bounded read. Domain writes and their log events must commit atomically; a published bus event alone cannot establish coverage.
- **No echo suppression.** Originating clients must receive their own writes back through normal sync. Client-minted ids make the self-echo idempotent, and the client's optimistic-write reconciliation depends on it — a `clientId` filter anywhere breaks it. Do not add one.
- **Resync targets are structural.** A resync is the `Resync` event arm carrying a typed `ResyncTarget` (`All` / `Group` / `Model`) — never an entity event with a sentinel model name.
- **Access loss surfaces as `Delete`.** Hydration receives the caller's *current* groups and is the authoritative visibility check (the event-level filter uses groups stamped at log time; access may have changed since). Hydration returning nothing ⇒ the client receives a `Delete` and removes the row.

## The kernel package

For Effect backends, [`@triargos/live-collection-server`](../packages/server/README.md) enforces all of the above: `SyncFeed.catchup` (filter → squash → batched hydration → access-loss-as-`Delete` → retention-as-`Resync(All)` → epoch), `SyncFeed.streamEvents` (authorized durable batches with polling heartbeats), and `SyncDispatcher` (persist-then-publish, no echo suppression). You supply two ports — a `SyncEventStore` over your database and a model registry describing how each entity hydrates — and keep auth, routes, and storage. Its README is the integration guide.

## Reference implementations

- [`examples/pi-demo/server`](../examples/pi-demo/server) — a complete Effect HTTP backend consuming the kernel: repos, a model registry, session auth, and `/catchup` + SSE routes.

## See also

- [Protocol reference](./protocol.md) — the schemas, the sync-group grammar, the squasher.
- [Architecture](./architecture.md) — the client side of these two endpoints.
