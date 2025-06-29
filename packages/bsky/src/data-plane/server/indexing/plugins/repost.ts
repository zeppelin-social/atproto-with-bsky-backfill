import { Insertable, Selectable, sql } from 'kysely'
import { CID } from 'multiformats/cid'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import * as lex from '../../../../lexicon/lexicons'
import * as Repost from '../../../../lexicon/types/app/bsky/feed/repost'
import { BackgroundQueue } from '../../background'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { Notification } from '../../db/tables/notification'
import { countAll, excluded } from '../../db/util'
import { copyIntoTable } from '../../util'
import { RecordProcessor } from '../processor'

const lexId = lex.ids.AppBskyFeedRepost
type Notif = Insertable<Notification>
type IndexedRepost = Selectable<DatabaseSchemaType['repost']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: Repost.Record,
  timestamp: string,
): Promise<IndexedRepost | null> => {
  const repost = {
    uri: uri.toString(),
    cid: cid.toString(),
    creator: uri.host,
    subject: obj.subject.uri,
    subjectCid: obj.subject.cid,
    via: obj.via?.uri,
    viaCid: obj.via?.cid,
    createdAt: normalizeDatetimeAlways(obj.createdAt),
    indexedAt: timestamp,
  }
  const [inserted] = await Promise.all([
    db
      .insertInto('repost')
      .values(repost)
      .onConflict((oc) => oc.doNothing())
      .returningAll()
      .executeTakeFirst(),
    db
      .insertInto('feed_item')
      .values({
        type: 'repost',
        uri: repost.uri,
        cid: repost.cid,
        postUri: repost.subject,
        originatorDid: repost.creator,
        sortAt:
          repost.indexedAt < repost.createdAt
            ? repost.indexedAt
            : repost.createdAt,
      })
      .onConflict((oc) => oc.doNothing())
      .executeTakeFirst(),
  ])

  return inserted || null
}

const insertBulkFn = async (
  db: Database,
  records: {
    uri: AtUri
    cid: CID
    obj: Repost.Record
    timestamp: string
  }[],
): Promise<Array<IndexedRepost>> => {
  const inserted = await copyIntoTable(
    db.pool,
    'repost',
    [
      'uri',
      'cid',
      'creator',
      'subject',
      'subjectCid',
      'via',
      'viaCid',
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
        subject: obj.subject.uri,
        subjectCid: obj.subject.cid,
        via: obj.via?.uri ?? null,
        viaCid: obj.via?.cid ?? null,
        createdAt,
        indexedAt,
        sortAt,
      }
    }),
  )
  await copyIntoTable(
    db.pool,
    'feed_item',
    ['type', 'uri', 'cid', 'postUri', 'originatorDid', 'sortAt'],
    records.map(({ uri, cid, obj, timestamp }) => {
      const createdAt = normalizeDatetimeAlways(obj.createdAt)
      const indexedAt = timestamp
      const sortAt =
        new Date(createdAt).getTime() < new Date(indexedAt).getTime()
          ? createdAt
          : indexedAt
      return {
        type: 'post',
        uri: uri.toString(),
        cid: cid.toString(),
        postUri: obj.subject.uri,
        originatorDid: uri.host,
        sortAt,
      }
    }),
  )
  return inserted
}

const findDuplicate = async (
  db: DatabaseSchema,
  uri: AtUri,
  obj: Repost.Record,
): Promise<AtUri | null> => {
  const found = await db
    .selectFrom('repost')
    .where('creator', '=', uri.host)
    .where('subject', '=', obj.subject.uri)
    .selectAll()
    .executeTakeFirst()
  return found ? new AtUri(found.uri) : null
}

const notifsForInsert = (obj: IndexedRepost) => {
  const subjectUri = new AtUri(obj.subject)
  // prevent self-notifications
  const isRepostFromSubjectUser = subjectUri.host === obj.creator
  if (isRepostFromSubjectUser) {
    return []
  }

  const notifs: Notif[] = [
    // Notification to the author of the reposted record.
    {
      did: subjectUri.host,
      author: obj.creator,
      recordUri: obj.uri,
      recordCid: obj.cid,
      reason: 'repost' as const,
      reasonSubject: subjectUri.toString(),
      sortAt: obj.sortAt,
    },
  ]

  if (obj.via) {
    const viaUri = new AtUri(obj.via)
    const isRepostFromViaSubjectUser = viaUri.host === obj.creator
    // prevent self-notifications
    if (!isRepostFromViaSubjectUser) {
      notifs.push(
        // Notification to the reposter via whose repost the repost was made.
        {
          did: viaUri.host,
          author: obj.creator,
          recordUri: obj.uri,
          recordCid: obj.cid,
          reason: 'repost-via-repost' as const,
          reasonSubject: viaUri.toString(),
          sortAt: obj.sortAt,
        },
      )
    }
  }

  return notifs
}

const deleteFn = async (
  db: DatabaseSchema,
  uri: AtUri,
): Promise<IndexedRepost | null> => {
  const uriStr = uri.toString()
  const [deleted] = await Promise.all([
    db
      .deleteFrom('repost')
      .where('uri', '=', uriStr)
      .returningAll()
      .executeTakeFirst(),
    db.deleteFrom('feed_item').where('uri', '=', uriStr).executeTakeFirst(),
  ])
  return deleted || null
}

const notifsForDelete = (
  deleted: IndexedRepost,
  replacedBy: IndexedRepost | null,
) => {
  const toDelete = replacedBy ? [] : [deleted.uri]
  return { notifs: [], toDelete }
}

const updateAggregates = async (db: DatabaseSchema, repost: IndexedRepost) => {
  const repostCountQb = db
    .insertInto('post_agg')
    .values({
      uri: repost.subject,
      repostCount: db
        .selectFrom('repost')
        .where('repost.subject', '=', repost.subject)
        .select(countAll.as('count')),
    })
    .onConflict((oc) =>
      oc
        .column('uri')
        .doUpdateSet({ repostCount: excluded(db, 'repostCount') }),
    )
  await repostCountQb.execute()
}

const updateAggregatesBulk = async (
  db: DatabaseSchema,
  reposts: IndexedRepost[],
) => {
  const repostCountQbs = sql`
    WITH input_values (uri) AS (
      SELECT * FROM UNNEST(${sql`${[reposts.map((r) => r.subject)]}::text[]`})
    )
    INSERT INTO post_agg ("uri", "repostCount")
    SELECT
      v.uri,
      count(repost.uri) AS repostCount
    FROM
      input_values AS v
      LEFT JOIN repost ON repost.subject = v.uri
    GROUP BY v.uri
    ON CONFLICT (uri) DO UPDATE SET "repostCount" = excluded."repostCount"
  `
  await repostCountQbs.execute(db).catch((e) => {
    throw new Error('Failed to update repost aggregates', { cause: e })
  })
}

export type PluginType = RecordProcessor<Repost.Record, IndexedRepost>

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
    updateAggregates,
    updateAggregatesBulk,
  })
}

export default makePlugin
