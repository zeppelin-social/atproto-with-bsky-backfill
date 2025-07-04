import { CID } from 'multiformats/cid'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import { InvalidRequestError } from '@atproto/xrpc-server'
import * as lex from '../../../../lexicon/lexicons'
import * as Threadgate from '../../../../lexicon/types/app/bsky/feed/threadgate'
import { BackgroundQueue } from '../../background'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { copyIntoTable } from '../../util'
import RecordProcessor from '../processor'

const lexId = lex.ids.AppBskyFeedThreadgate
type IndexedGate = DatabaseSchemaType['thread_gate']

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: Threadgate.Record,
  timestamp: string,
): Promise<IndexedGate | null> => {
  const postUri = new AtUri(obj.post)
  if (postUri.host !== uri.host || postUri.rkey !== uri.rkey) {
    throw new InvalidRequestError(
      'Creator and rkey of thread gate does not match its post',
    )
  }
  const inserted = await db
    .insertInto('thread_gate')
    .values({
      uri: uri.toString(),
      cid: cid.toString(),
      creator: uri.host,
      postUri: obj.post,
      createdAt: normalizeDatetimeAlways(obj.createdAt),
      indexedAt: timestamp,
    })
    .onConflict((oc) => oc.doNothing())
    .returningAll()
    .executeTakeFirst()
  await db
    .updateTable('post')
    .where('uri', '=', postUri.toString())
    .set({ hasThreadGate: true })
    .executeTakeFirst()
  return inserted || null
}

const insertBulkFn = async (
  db: Database,
  records: {
    uri: AtUri
    cid: CID
    obj: Threadgate.Record
    timestamp: string
  }[],
): Promise<Array<IndexedGate>> => {
  for (const record of records) {
    const postUri = new AtUri(record.obj.post)
    if (postUri.host !== record.uri.host || postUri.rkey !== record.uri.rkey) {
      throw new InvalidRequestError(
        'Creator and rkey of thread gate does not match its post',
      )
    }
  }

  return copyIntoTable(
    db.pool,
    'thread_gate',
    ['uri', 'cid', 'creator', 'postUri', 'createdAt', 'indexedAt'],
    records.map(({ uri, cid, obj, timestamp }) => {
      const createdAt = normalizeDatetimeAlways(obj.createdAt)
      const indexedAt = timestamp
      return {
        uri: uri.toString(),
        cid: cid.toString(),
        creator: uri.host,
        postUri: obj.post,
        createdAt,
        indexedAt,
      }
    }),
  )
}

const findDuplicate = async (
  db: DatabaseSchema,
  _uri: AtUri,
  obj: Threadgate.Record,
): Promise<AtUri | null> => {
  const found = await db
    .selectFrom('thread_gate')
    .where('postUri', '=', obj.post)
    .selectAll()
    .executeTakeFirst()
  return found ? new AtUri(found.uri) : null
}

const notifsForInsert = () => {
  return []
}

const deleteFn = async (
  db: DatabaseSchema,
  uri: AtUri,
): Promise<IndexedGate | null> => {
  const deleted = await db
    .deleteFrom('thread_gate')
    .where('uri', '=', uri.toString())
    .returningAll()
    .executeTakeFirst()
  if (deleted) {
    await db
      .updateTable('post')
      .where('uri', '=', deleted.postUri)
      .set({ hasThreadGate: false })
      .executeTakeFirst()
  }
  return deleted || null
}

const notifsForDelete = () => {
  return { notifs: [], toDelete: [] }
}

export type PluginType = RecordProcessor<Threadgate.Record, IndexedGate>

export const makePlugin = (
  db: Database,
  background: BackgroundQueue,
): PluginType => {
  return new RecordProcessor(db, background, {
    lexId,
    insertFn,
    insertBulkFn,
    findDuplicate,
    deleteFn,
    notifsForInsert,
    notifsForDelete,
  })
}

export default makePlugin
