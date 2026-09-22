# Persistence

Every collection stores its rows in local SQLite. A page reload hydrates from disk — no re-fetch — and sync resumes with deltas from the stored cursor. You set persistence up once, when building the runtime; after that it's invisible.

## Setup (browser)

The library takes a persistence **value**, built from TanStack DB's browser persistence package:

```ts
import {
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
} from "@tanstack/browser-db-sqlite-persistence"
import { makeLiveRuntime } from "@leandro-lugaresi/live-collection"

const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: "myapp" })
const persistence = createBrowserWASQLitePersistence({ database })

const runtime = makeLiveRuntime({ persistence, sync })
```

Open the database once at startup (it's async — the database lives in [OPFS](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system)); every collection then persists through it automatically. `defineCollection` derives a stable table id per `(entity, scope)` and wires the persisted collection for you — you never call `persistedCollectionOptions` yourself.

### Vite configuration

The persistence package bundles a worker and a wasm SQLite engine. Exclude both from dependency pre-bundling or the worker URLs break:

```ts
// vite.config.ts
export default defineConfig({
  optimizeDeps: {
    exclude: ["@tanstack/browser-db-sqlite-persistence", "@journeyapps/wa-sqlite"],
  },
})
```

## What persistence guarantees

- **Hydrate from disk.** A mounted collection loads its saved rows immediately; `listFn` only runs when there is no trustworthy local base (first ever mount, schema change, resync).
- **Deltas persist.** Every synced write — live events, catchup, replay, confirmed optimistic writes — lands in SQLite, so the base is always as fresh as the last event applied.
- **Writes are not offline-durable.** The database holds *synced* server truth. An optimistic write made while offline exists only in memory and will not survive a reload. (A durable offline mutation queue is a possible future addition, not a current feature.)

## Persistence codecs

Declare `persistedSchema` on `defineCollection` when rows contain rich runtime values.
The codec's decoded type must match the collection model; its encoded value must be a
JSON object. The library invokes its encoder before SQLite writes and its decoder on
loaded rows, including reloads and metadata scans. No Effect services may be required
by this codec.

```ts
import { Schema } from "effect"
import { ModelId } from "@leandro-lugaresi/live-collection-protocol"
import { defineCollection } from "@leandro-lugaresi/live-collection"

const Entry = Schema.Struct({
  id: Schema.String,
  createdAt: Schema.DateTimeUtcFromString,
})

const entries = defineCollection({
  runtime,
  entity: "Entry",
  schema: Entry,
  persistedSchema: Entry,
  getKey: (entry) => ModelId.make(entry.id),
  listFn: loadEntries, // Effect<ReadonlyArray<typeof Entry.Type>>
})
```

Here `createdAt` is an Effect `DateTime.Utc` in memory and an ISO string on disk.
If the model instead declares runtime-only fields such as `Schema.DateTimeUtc`, use
`persistedSchema: Schema.toCodecJson(Model)` to select Effect's JSON transforms.
The codec applies to global, scoped, and partial collections. Omitting it preserves
TanStack's existing serialization behavior; arbitrary Effect values are not restored
automatically without an appropriate codec.

The adapter decodes `Schema.Class` values before handing them to TanStack. However,
TanStack DB 0.6.16 spreads the **top-level row** when adding virtual properties to
collection reads, independently of storage. Do not rely on top-level class methods
or `instanceof` from `collection.get()` or query results. Plain record rows containing
rich fields, including nested class values, avoid that limitation.

Encoding is completed for the entire transaction before it reaches SQLite. Invalid
encodings or corrupt stored rows fail with `PersistenceCodecError` (`collectionId`,
`operation`, `cause`); they are not silently skipped. This error crosses TanStack's
Promise adapter boundary. TanStack's existing asynchronous persistence-error handling
still applies: a synced write is **not** an awaitable durability acknowledgement, and
this seam does not add automatic repair or a persistence health UI.

Keep field names and primitive fields used by SQLite predicates/indexes compatible
with their stored representation. The seam does not translate query expressions
through arbitrary schema transformations. Custom row metadata is passed through;
`persistedSchema` encodes row values only.

### Cache versioning

The cache version includes the model schema and the optional persisted codec's
encoded structural representation. Adding a codec invalidates old tables **and**
journal coverage, even when its shape is identical to the model. Changes to encoded
field types or names trigger the same coordinated rebuild.

Function bodies inside custom transformations cannot be hashed. When changing only
codec behavior without changing its shape, change its identifier annotation too:

```ts
persistedSchema: StoredEntry.pipe(Schema.annotate({ identifier: "StoredEntryV2" }))
```

This remains a rebuildable cache, not a data migration system. Rebuilds require
server access to fetch authorized rows again; partial subsets reload on demand.

## Multiple tabs

For codec-enabled collections, cross-tab commit messages invalidate local rows
instead of carrying rich runtime objects through structured cloning. Followers reload
their active subsets from SQLite through the codec; pull-recovery deltas follow the
same path. This adds local reads, without requiring a server relist. Real browser
OPFS and BroadcastChannel behavior still needs application-level validation.

Tabs sharing one `databaseName` share one persisted state — fine when they're the same logical client. If you want tabs to act as independent clients (each with its own cursor and journal), give each a distinct `databaseName` for both the SQLite database and the `SyncJournal`.

## Outside the browser

`persistedCollectionOptions` and the `PersistedCollectionPersistence` type come from `@tanstack/db-sqlite-persistence-core`; the browser package builds on it. In Node (e.g. tests) you can assemble a persistence value over any SQLite driver against the same core interface — the library only sees the value.

The lockfile selects TanStack DB `0.6.16` and SQLite persistence core `0.2.8`.
The catalog declares compatible ranges; upgrade these alpha integrations deliberately
and validate them together.

## See also

- [Getting started](./getting-started.md) — the runtime setup this slots into.
- [Architecture](./architecture.md) — how synced writes reach the store, and why scope (not persistence) bounds memory.
