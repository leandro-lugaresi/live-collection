---
"@leandro-lugaresi/live-collection-protocol": major
"@leandro-lugaresi/live-collection": major
"@leandro-lugaresi/live-collection-server": major
"@leandro-lugaresi/live-collection-react": major
---

Replace best-effort per-event SSE with cursor-resumed, ordered durable catchup batches.
The feed bounds replay at a committed head, polls for unpublished commits, validates
epochs on idle batches, and closes on Resync or timeline changes. The client journals
whole covered batches before advancing its cursor and preserves recovery across retries.

`SyncTransport.connect` now accepts `{ from, epoch }`; `SyncFeed.streamEvents` requires
`fromSyncId` and `epoch`, with `pollInterval` replacing feed `keepAlive`. Snapshot and
subset restoration/slice callbacks carry a recovery generation. Upgrade both ends and
rebuild caches created by the previous protocol. See `docs/synchronization.md#migration`.

Protect partial snapshots from stale deletes and membership moves, invalidate coverage
on recovery, guard overlapping snapshots, and prevent old acknowledgements from crossing
epochs. Document the remaining security and native-storage adoption requirements.
