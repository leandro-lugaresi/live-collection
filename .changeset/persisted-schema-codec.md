---
"@triargos/live-collection": minor
---

Add optional `persistedSchema` to `defineCollection` for encoding runtime rows into
JSON objects before SQLite writes and decoding them on hydration. This preserves
Effect DateTime and nested class semantics for global, scoped, and partial caches.
Codec shape changes invalidate both persisted rows and journal coverage. Cross-tab
notifications reload through the codec instead of cloning rich row values.

Expose `PersistenceCodecError` for invalid encodings or corrupt stored rows. Native
TanStack collection reads still spread top-level objects, so their class prototypes
are not preserved; see the persistence documentation for this limitation and migration.
