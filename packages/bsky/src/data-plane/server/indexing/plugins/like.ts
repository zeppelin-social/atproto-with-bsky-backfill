import { Insertable, Selectable, sql } from 'kysely'
import { CID } from 'multiformats/cid'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import * as lex from '../../../../lexicon/lexicons'
import * as Like from '../../../../lexicon/types/app/bsky/feed/like'
import { BackgroundQueue } from '../../background'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { Notification } from '../../db/tables/notification'
import { countAll, excluded } from '../../db/util'
import { RecordProcessor } from '../processor'

const lexId = lex.ids.AppBskyFeedLike

type Notif = Insertable<Notification>
type IndexedLike = Selectable<DatabaseSchemaType['like']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: Like.Record,
  timestamp: string,
): Promise<IndexedLike | null> => {
  const inserted = await db
    .insertInto('like')
    .values({
      uri: uri.toString(),
      cid: cid.toString(),
      creator: uri.host,
      subject: obj.subject.uri,
      subjectCid: obj.subject.cid,
      via: obj.via?.uri,
      viaCid: obj.via?.cid,
      createdAt: normalizeDatetimeAlways(obj.createdAt),
      indexedAt: timestamp,
    })
    .onConflict((oc) => oc.doNothing())
    .returningAll()
    .executeTakeFirst()
  return inserted || null
}

const insertBulkFn = async (
  db: DatabaseSchema,
  records: {
    uri: AtUri
    cid: CID
    obj: Like.Record
    timestamp: string
  }[],
): Promise<Array<IndexedLike>> => {
  // Transpose into an array per column to bypass the length limit on VALUES lists
  // sql.literal() is unsafe, so we compromise by not calling it on user-provided values
  const toInsert: [
    uri: Array<RawBuilder<unknown>>,
    cid: Array<RawBuilder<unknown>>,
    creator: Array<RawBuilder<unknown>>,
    subject: Array<string>,
    subjectCid: Array<string>,
    createdAt: Array<RawBuilder<unknown>>,
    indexedAt: Array<RawBuilder<unknown>>,
  ] = [[], [], [], [], [], [], []]
  for (const { uri, cid, obj, timestamp } of records) {
    toInsert[0].push(sql.literal(uri.toString()))
    toInsert[1].push(sql.literal(cid.toString()))
    toInsert[2].push(sql.literal(uri.host))
    toInsert[3].push(obj.subject.uri)
    toInsert[4].push(obj.subject.cid)
    toInsert[5].push(sql.literal(normalizeDatetimeAlways(obj.createdAt)))
    toInsert[6].push(sql.literal(timestamp))
  }
  return db
    .insertInto('like')
    .expression(
      db
        .selectFrom(
          sql<IndexedLike>`
            unnest(${sql.join(
              toInsert.map((arr) => sql`ARRAY[${sql.join(arr)}]`),
            )})
          `.as<'l'>(
            sql`l("uri", "cid", "creator", "subject", "subjectCid", "createdAt", "indexedAt")`,
          ),
        )
        .selectAll(),
    )
    .onConflict((oc) => oc.doNothing())
    .returningAll()
    .execute()
}

const findDuplicate = async (
  db: DatabaseSchema,
  uri: AtUri,
  obj: Like.Record,
): Promise<AtUri | null> => {
  const found = await db
    .selectFrom('like')
    .where('creator', '=', uri.host)
    .where('subject', '=', obj.subject.uri)
    .selectAll()
    .executeTakeFirst()
  return found ? new AtUri(found.uri) : null
}

const notifsForInsert = (obj: IndexedLike) => {
  const subjectUri = new AtUri(obj.subject)
  // prevent self-notifications
  const isLikeFromSubjectUser = subjectUri.host === obj.creator
  if (isLikeFromSubjectUser) {
    return []
  }

  const notifs: Notif[] = [
    // Notification to the author of the liked record.
    {
      did: subjectUri.host,
      author: obj.creator,
      recordUri: obj.uri,
      recordCid: obj.cid,
      reason: 'like' as const,
      reasonSubject: subjectUri.toString(),
      sortAt: obj.sortAt,
    },
  ]

  if (obj.via) {
    const viaUri = new AtUri(obj.via)
    const isLikeFromViaSubjectUser = viaUri.host === obj.creator
    // prevent self-notifications
    if (!isLikeFromViaSubjectUser) {
      notifs.push(
        // Notification to the reposter via whose repost the like was made.
        {
          did: viaUri.host,
          author: obj.creator,
          recordUri: obj.uri,
          recordCid: obj.cid,
          reason: 'like-via-repost' as const,
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
): Promise<IndexedLike | null> => {
  const deleted = await db
    .deleteFrom('like')
    .where('uri', '=', uri.toString())
    .returningAll()
    .executeTakeFirst()
  return deleted || null
}

const notifsForDelete = (
  deleted: IndexedLike,
  replacedBy: IndexedLike | null,
) => {
  const toDelete = replacedBy ? [] : [deleted.uri]
  return { notifs: [], toDelete }
}

const updateAggregates = async (db: DatabaseSchema, like: IndexedLike) => {
  const likeCountQb = db
    .insertInto('post_agg')
    .values({
      uri: like.subject,
      likeCount: db
        .selectFrom('like')
        .where('like.subject', '=', like.subject)
        .select(countAll.as('count')),
    })
    .onConflict((oc) =>
      oc.column('uri').doUpdateSet({ likeCount: excluded(db, 'likeCount') }),
    )
  await likeCountQb.execute()
}

const updateAggregatesBulk = async (
  db: DatabaseSchema,
  likes: IndexedLike[],
) => {
  const likeCountQbs = sql`
    WITH input_values (uri) AS (
      SELECT * FROM unnest(${sql`${[likes.map((l) => l.subject)]}::text[]`})
    )
    INSERT INTO post_agg ("uri", "likeCount")
    SELECT
      v.uri,
      count(like.uri) AS likeCount
    FROM
      input_values AS v
      LEFT JOIN like ON like.subject = v.uri
    GROUP BY v.uri
    ON CONFLICT (uri) DO UPDATE SET "likeCount" = excluded."likeCount"
  `
  await likeCountQbs.execute(db)
}

export type PluginType = RecordProcessor<Like.Record, IndexedLike>

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
