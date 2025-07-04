import { Selectable } from 'kysely'
import { CID } from 'multiformats/cid'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import * as lex from '../../../../lexicon/lexicons'
import * as FeedGenerator from '../../../../lexicon/types/app/bsky/feed/generator'
import { BackgroundQueue } from '../../background'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { copyIntoTable } from '../../util'
import RecordProcessor from '../processor'

const lexId = lex.ids.AppBskyFeedGenerator
type IndexedFeedGenerator = Selectable<DatabaseSchemaType['feed_generator']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: FeedGenerator.Record,
  timestamp: string,
): Promise<IndexedFeedGenerator | null> => {
  const inserted = await db
    .insertInto('feed_generator')
    .values({
      uri: uri.toString(),
      cid: cid.toString(),
      creator: uri.host,
      feedDid: obj.did,
      displayName: obj.displayName,
      description: obj.description,
      descriptionFacets: obj.descriptionFacets
        ? JSON.stringify(obj.descriptionFacets)
        : undefined,
      avatarCid: obj.avatar?.ref.toString(),
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
    obj: FeedGenerator.Record
    timestamp: string
  }[],
): Promise<IndexedFeedGenerator[]> => {
  return copyIntoTable(
    db.pool,
    'feed_generator',
    [
      'uri',
      'cid',
      'creator',
      'feedDid',
      'displayName',
      'description',
      'descriptionFacets',
      'avatarCid',
      'createdAt',
      'indexedAt',
    ],
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
        feedDid: obj.did,
        displayName: obj.displayName,
        description: obj.description ?? null,
        descriptionFacets: obj.descriptionFacets
          ? JSON.stringify(obj.descriptionFacets)
          : null,
        avatarCid: obj.avatar?.ref.toString() ?? null,
        createdAt,
        indexedAt,
        sortAt,
      }
    }),
  )
}

const findDuplicate = async (): Promise<AtUri | null> => {
  return null
}

const notifsForInsert = () => {
  return []
}

const deleteFn = async (
  db: DatabaseSchema,
  uri: AtUri,
): Promise<IndexedFeedGenerator | null> => {
  const deleted = await db
    .deleteFrom('feed_generator')
    .where('uri', '=', uri.toString())
    .returningAll()
    .executeTakeFirst()
  return deleted || null
}

const notifsForDelete = () => {
  return { notifs: [], toDelete: [] }
}

export type PluginType = RecordProcessor<
  FeedGenerator.Record,
  IndexedFeedGenerator
>

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
