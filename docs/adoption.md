# Application adoption requirements

This change repairs synchronization coverage and partial-collection invalidation.
It does not make this library a complete financial-data security or native-storage
solution. The server remains authoritative; both local stores are rebuildable caches
of data the server has authorized.

The review's following requirements remain open after inspecting the current adapters:

| Requirement | Current behavior and adoption gate |
| --- | --- |
| Rich domain values | Opt into `persistedSchema` to encode SQLite rows and reconstruct rich values on hydration. Test actual domain codecs through disk round-trips. TanStack still spreads top-level rows when adding virtual properties, so top-level class methods are not preserved in collection reads. See [persistence codecs](./persistence.md#persistence-codecs). |
| Authenticated identity isolation | SQLite table keys describe entity/scope, while the IndexedDB journal has its own database name and global cursor. Applications must partition **both** by deployment/user/tenant before opening a runtime. Subsets and scopes are not authorization boundaries. |
| Shutdown and account switching | `runtime.dispose()` is not awaitable. Applications still need a coordinated stop, drain, close, and switch operation across runtimes/tabs. Recovery generations prevent old synchronization acknowledgements; they do not coordinate logout. |
| Erasure | Closing/disposal does not erase SQLite/OPFS or IndexedDB records. Define and implement account-switch, logout, deletion, and revoked-session cache policies for both stores, including other open tabs. |
| Authorization revocation | Routes resolve groups at connection time. Polling reuses those groups. Hydrators must check current row access, and the application must terminate/re-authorize streams when session or group permissions change. A Resync alone cannot revoke an obsolete stream's authorization context. |
| Native support | Core remains framework-neutral; the supplied durable journal uses browser IndexedDB. Native journal/SQLite, HTTP streaming, background/resume, migrations, and device behavior remain unimplemented or unverified. |
| Operational failures | Entity decode failures are still logged/skipped by collection drains, and snapshot failures can terminate a drain. Applications need visible degraded/freshness state and deliberate repair for codec, quota, authentication, and network failures. |
| Server transactions | A database adapter must commit domain state and its event atomically and expose a committed log prefix. An allocated sequence number is not sufficient if a lower number can commit after a higher observed head. Polling heals missed publication, not missing event creation. |

Event-level exact group intersection still filters catchup and streamed replay.
Hydration rechecks visibility, maps inaccessible rows to Delete, and subset fetches
receive the server-resolved groups and return Forbidden without recording coverage.
Client extractors only control collection membership. All authorized model events may
still cross the network and enter the payload journal, including unloaded subsets.

The workspace pins Effect and its Node/Vitest integrations to `4.0.0-rc.112`.
TanStack DB remains `0.6.16` with SQLite core `0.2.8`. Compatibility with consumer
dependency versions and full applications must be assessed separately.

Required follow-up validation includes real browsers (OPFS/WASM, multiple tabs,
eviction/quota, persistence crash ordering), application-specific rich model codecs, identity
switching and erasure, server-side revocation, and a separate native-device pilot.
