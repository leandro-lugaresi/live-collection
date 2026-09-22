import { Effect, Option, Schema } from "effect"
import type {
  PersistedCollectionCoordinator,
  PersistedCollectionPersistence,
  PersistedScannedRow,
  PersistedTx,
  PersistenceAdapter,
  ProtocolEnvelope,
} from "@tanstack/db-sqlite-persistence-core"

/** A row failed the persistence codec; the adapter never writes or returns a partial batch. */
export class PersistenceCodecError extends Schema.TaggedError<PersistenceCodecError>()(
  "PersistenceCodecError",
  { collectionId: Schema.String, operation: Schema.Literals(["encode", "decode"]), cause: Schema.Unknown },
) {}

const CommitNotice = Schema.Struct({
  type: Schema.Literal("tx:committed"),
  term: Schema.Finite,
  seq: Schema.Finite,
  txId: Schema.String,
  latestRowVersion: Schema.Finite,
})

// Coordinators may structured-clone row payloads, bypassing the SQLite read path.
// Send/receive invalidations instead: the persisted transaction is already committed,
// and the receiver reloads through its own codec (including deletes and metadata).
const invalidate = (message: ProtocolEnvelope<unknown>): ProtocolEnvelope<unknown> => {
  const notice = Schema.decodeUnknownOption(CommitNotice)(message.payload)
  return Option.isSome(notice)
    ? { ...message, payload: { ...notice.value, requiresFullReload: true } }
    : message
}

const wrapCoordinator = (source: PersistedCollectionCoordinator): PersistedCollectionCoordinator => ({
  getNodeId: () => source.getNodeId(),
  isLeader: (id) => source.isLeader(id),
  ensureLeadership: (id) => source.ensureLeadership(id),
  subscribe: (id, receive) => source.subscribe(id, (message) => receive(invalidate(message))),
  publish: (id, message) => source.publish(id, invalidate(message)),
  requestEnsurePersistedIndex: (...args) => source.requestEnsurePersistedIndex(...args),
  ...(source.requestEnsureRemoteSubset ? {
    requestEnsureRemoteSubset: (...args) => source.requestEnsureRemoteSubset!(...args),
  } : {}),
  ...(source.pullSince ? {
    pullSince: async (...args) => {
      const response = await source.pullSince!(...args)
      if (!response.ok) return response
      return {
        type: response.type,
        rpcId: response.rpcId,
        ok: true,
        latestTerm: response.latestTerm,
        latestSeq: response.latestSeq,
        latestRowVersion: response.latestRowVersion,
        requiresFullReload: true,
      }
    },
  } : {}),
})

/** Internal adapter for sync-present collections; resolution remains owned by the supplied factory. */
export const withPersistedSchema = <T extends object>(
  source: PersistedCollectionPersistence,
  schema: Schema.Codec<T, unknown>,
): PersistedCollectionPersistence => {
  const encode = Schema.encodeUnknownEffect(schema)
  const decode = Schema.decodeUnknownEffect(schema)
  const jsonObject = Schema.decodeUnknownEffect(Schema.JsonObject)
  const wrap = (persistence: PersistedCollectionPersistence): PersistedCollectionPersistence => {
    const adapter = persistence.adapter
    const decodeRows = (id: string, rows: ReadonlyArray<PersistedScannedRow>) => Effect.runPromise(
      Effect.forEach(rows, (row) => decode(row.value).pipe(
        Effect.map((value) => ({
          ...row,
          // The schema has decoded unknown storage into T. TanStack erases the
          // collection's object type to a string-keyed record at its adapter seam.
          value: value as Record<string, unknown>,
        })),
      )).pipe(Effect.mapError((cause) => new PersistenceCodecError({ collectionId: id, operation: "decode", cause }))),
    )
    const wrapped: PersistenceAdapter = {
      loadSubset: async (id, options, ctx) => decodeRows(id, await adapter.loadSubset(id, options, ctx)),
      applyCommittedTx: async (id, tx) => {
        // Encode the whole transaction first. A bad row cannot leave a partial write.
        const mutations = await Effect.runPromise(Effect.forEach(tx.mutations, (mutation): Effect.Effect<PersistedTx["mutations"][number], Schema.SchemaError> =>
          mutation.type === "delete"
            ? Effect.succeed({ ...mutation, value: {} }) // tombstones need only their key
            : encode(mutation.value).pipe(
              Effect.flatMap(jsonObject),
              // SQLite merges updates. A codec returns a complete row; replace it
              // so an omitted optional property does not retain an old stored value.
              Effect.map((value) => ({ ...mutation, type: "insert" as const, value })),
            ),
        ).pipe(Effect.mapError((cause) => new PersistenceCodecError({ collectionId: id, operation: "encode", cause }))))
        await adapter.applyCommittedTx(id, { ...tx, mutations })
      },
      ensureIndex: (...args) => adapter.ensureIndex(...args),
      ...(adapter.scanRows ? { scanRows: async (id, options) => decodeRows(id, await adapter.scanRows!(id, options)) } : {}),
      ...(adapter.loadCollectionMetadata ? { loadCollectionMetadata: (id) => adapter.loadCollectionMetadata!(id) } : {}),
      ...(adapter.getStreamPosition ? { getStreamPosition: (id) => adapter.getStreamPosition!(id) } : {}),
      ...(adapter.markIndexRemoved ? { markIndexRemoved: (...args) => adapter.markIndexRemoved!(...args) } : {}),
    }
    return {
      adapter: wrapped,
      ...(persistence.coordinator ? { coordinator: wrapCoordinator(persistence.coordinator) } : {}),
      ...(persistence.resolvePersistenceForCollection ? {
        resolvePersistenceForCollection: (options) => wrap(persistence.resolvePersistenceForCollection!(options)),
      } : {}),
      ...(persistence.resolvePersistenceForMode ? {
        resolvePersistenceForMode: (mode) => wrap(persistence.resolvePersistenceForMode!(mode)),
      } : {}),
    }
  }
  return wrap(source)
}
