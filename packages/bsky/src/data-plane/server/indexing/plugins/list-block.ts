import { Selectable } from 'kysely'
import { CID } from 'multiformats/cid'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import * as lex from '../../../../lexicon/lexicons'
import * as ListBlock from '../../../../lexicon/types/app/bsky/graph/listblock'
import { BackgroundQueue } from '../../background'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { copyIntoTable } from '../../util'
import RecordProcessor from '../processor'

const lexId = lex.ids.AppBskyGraphListblock
type IndexedListBlock = Selectable<DatabaseSchemaType['list_block']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: ListBlock.Record,
  timestamp: string,
): Promise<IndexedListBlock | null> => {
  const inserted = await db
    .insertInto('list_block')
    .values({
      uri: uri.toString(),
      cid: cid.toString(),
      creator: uri.host,
      subjectUri: obj.subject,
      createdAt: normalizeDatetimeAlways(obj.createdAt),
      indexedAt: timestamp,
    })
    .onConflict((oc) => oc.doNothing())
    .returningAll()
    .executeTakeFirst()
  return inserted || null
}

const insertBulkFn = async (
  db: Database,
  records: {
    uri: AtUri
    cid: CID
    obj: ListBlock.Record
    timestamp: string
  }[],
): Promise<Array<IndexedListBlock>> => {
  return copyIntoTable(
    db.pool,
    'list_block',
    ['uri', 'cid', 'creator', 'subjectUri', 'createdAt', 'indexedAt'],
    records.map(({ uri, cid, obj, timestamp }) => {
      const createdAt = normalizeDatetimeAlways(obj.createdAt)
      const indexedAt = timestamp
      const sortAt =
        new Date(createdAt).getTime() < new Date(indexedAt).getTime()
          ? createdAt
          : indexedAt
      return {
        uri: uri.toString(),
        cid: cid.toString(),
        creator: uri.host,
        subjectUri: obj.subject,
        createdAt,
        indexedAt,
        sortAt,
      }
    }),
  )
}

const findDuplicate = async (
  db: DatabaseSchema,
  uri: AtUri,
  obj: ListBlock.Record,
): Promise<AtUri | null> => {
  const found = await db
    .selectFrom('list_block')
    .where('creator', '=', uri.host)
    .where('subjectUri', '=', obj.subject)
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
): Promise<IndexedListBlock | null> => {
  const deleted = await db
    .deleteFrom('list_block')
    .where('uri', '=', uri.toString())
    .returningAll()
    .executeTakeFirst()
  return deleted || null
}

const notifsForDelete = () => {
  return { notifs: [], toDelete: [] }
}

export type PluginType = RecordProcessor<ListBlock.Record, IndexedListBlock>

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
