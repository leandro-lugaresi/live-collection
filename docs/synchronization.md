# Durable synchronization protocol

The stream is an ordered sequence of durable catchup batches, not a bus tail. This
protocol deliberately trades one log read per idle poll (default one second) for a
single correctness path, backpressured reads, and recovery without an outbox.

## Locked interfaces

- `SyncTransport.connect({ from, epoch })` returns `Stream<CatchupResponse, SyncConnectionLost>`.
- `SyncFeed.streamEvents({ fromSyncId, epoch, syncGroups, pollInterval? })` emits SSE
  `data:` frames containing `CatchupResponse`. Empty batches are heartbeat/checkpoints.
- `SyncResumeRequest` decodes `{ from: SyncId, epoch?: Epoch }` at the HTTP boundary.
- `Snapshot` signals name their reason (`Mount`, `Resync`, or `EpochReset`) and
  carry the broker recovery generation. Subset restoration and slice callbacks
  receive that same process-local generation; old work cannot poison new coverage.

## Coverage and ordering

For each batch, read epoch E, then durable head H, then all log events in `(cursor,H]`.
Filter authorization, squash, hydrate with authorization, and check E again before
returning. A reset during the read discards the batch and retries. Store adapters must
expose a committed prefix: after observing H, no future transaction may commit an
older ID into that prefix. Domain changes and their events must commit atomically.

The client appends the entire batch before publishing its events in order, then saves
H. It never infers completeness from an individual event ID. Authorized IDs need not
be consecutive. Duplicate batches are safe. Invalid wire data closes the connection
without advancing the cursor. An encode failure for a known model fails the batch.

After emitting a batch, the server reads from H on the next poll. Changes committed
during hydration, transmission, or between catchup and SSE stay in the log for that
next read. Replay and live delivery use the same serial operation; no bus subscription,
replay/live merge, or live-event buffer is involved. Slow consumers backpressure reads.
A durable event that is never published to the bus is recovered by the next poll,
including when no later events exist. Retention loss enters recovery instead of
silently dropping changes. The existing bus remains available to application consumers.

## Recovery

Client states are `CatchingUp`, `Streaming`, and `Recovering`. Catchup failure remains
in catchup/recovery; it never opens an unsafe live tail. Every response, including
heartbeats, validates epoch even when a durable cursor exists. Epoch A/100 → B/1
atomically clears journal and coverage metadata and emits an `EpochReset` snapshot.
Partial collections discard all old coverage and rows, including at new position 0.

A Resync batch records a durable invalidation and triggers snapshots. The feed closes
after Resync or an epoch change; the client also interrupts that stream immediately.
Recovery survives connection retries. The durable resync/epoch metadata makes future
mounts rebuild after a process restart. Active full collections refetch immediately;
partial subsets are invalidated and fetched by the next `loadBy*` call.

## Limits

This guarantees eventual authorized-state convergence under a compliant store,
successful storage writes, valid model codecs, and eventual network availability.
A batch currently contains the entire retained range; there is no server paging or
server-side subset subscription, so a large backlog can still consume substantial
memory and bandwidth. This protocol does not make SQLite and journal writes atomic together, implement logout/erasure,
or continuously refresh the authorization context of an open connection. See
[Application adoption requirements](./adoption.md).

## Migration

Upgrade the client, protocol, and backend together. Old per-event SSE frames are rejected.
Custom transports must implement `connect({ from, epoch })` and return complete ordered
`CatchupResponse` batches. Feed callers must supply `fromSyncId` and `epoch`; replace
feed `keepAlive` with `pollInterval`. The client's silence timeout must exceed that
interval and expected request latency. Custom snapshot/subset adapters must preserve
the recovery generation and handle reset reasons.

Rebuild both SQLite and journal caches on first adoption: the old protocol may already
have advanced its cursor past missing events. The new protocol cannot infer those holes.
Coordinate rebuilding with the application's identity/logout policy; resetting only the
journal while displaying old cached rows is not a complete migration.
