import { NonNullableInsertKeys, RawNode, sql } from 'kysely'
import { Pool } from 'pg'
import { from as copyFrom } from 'pg-copy-streams'
import {
  Record as PostRecord,
  ReplyRef,
} from '../../lexicon/types/app/bsky/feed/post'
import { Record as GateRecord } from '../../lexicon/types/app/bsky/feed/threadgate'
import { parseThreadGate } from '../../views/util'
import { DatabaseSchema, DatabaseSchemaType } from './db/database-schema'
import { valuesList } from './db/util'

export const getDescendentsQb = (
  db: DatabaseSchema,
  opts: {
    uri: string
    depth: number // required, protects against cycles
  },
) => {
  const { uri, depth } = opts
  const query = db.withRecursive('descendent(uri, depth)', (cte) => {
    return cte
      .selectFrom('post')
      .select(['post.uri as uri', sql<number>`1`.as('depth')])
      .where(sql`1`, '<=', depth)
      .where('replyParent', '=', uri)
      .unionAll(
        cte
          .selectFrom('post')
          .innerJoin('descendent', 'descendent.uri', 'post.replyParent')
          .where('descendent.depth', '<', depth)
          .select([
            'post.uri as uri',
            sql<number>`descendent.depth + 1`.as('depth'),
          ]),
      )
  })
  return query
}

export const getAncestorsAndSelfQb = (
  db: DatabaseSchema,
  opts: {
    uri: string
    parentHeight: number // required, protects against cycles
  },
) => {
  const { uri, parentHeight } = opts
  const query = db.withRecursive(
    'ancestor(uri, ancestorUri, height)',
    (cte) => {
      return cte
        .selectFrom('post')
        .select([
          'post.uri as uri',
          'post.replyParent as ancestorUri',
          sql<number>`0`.as('height'),
        ])
        .where('uri', '=', uri)
        .unionAll(
          cte
            .selectFrom('post')
            .innerJoin('ancestor', 'ancestor.ancestorUri', 'post.uri')
            .where('ancestor.height', '<', parentHeight)
            .select([
              'post.uri as uri',
              'post.replyParent as ancestorUri',
              sql<number>`ancestor.height + 1`.as('height'),
            ]),
        )
    },
  )
  return query
}

export const invalidReplyRoot = (
  reply: ReplyRef,
  parent: {
    record: PostRecord
    invalidReplyRoot: boolean | null
  },
) => {
  const replyRoot = reply.root.uri
  const replyParent = reply.parent.uri
  // if parent is not a valid reply, transitively this is not a valid one either
  if (parent.invalidReplyRoot) {
    return true
  }
  // replying to root post: ensure the root looks correct
  if (replyParent === replyRoot) {
    return !!parent.record.reply
  }
  // replying to a reply: ensure the parent is a reply for the same root post
  return parent.record.reply?.root.uri !== replyRoot
}
export const violatesThreadGate = async (
  db: DatabaseSchema,
  replierDid: string,
  ownerDid: string,
  rootPost: PostRecord | null,
  gate: GateRecord | null,
) => {
  const {
    canReply,
    allowFollower,
    allowFollowing,
    allowListUris = [],
  } = parseThreadGate(replierDid, ownerDid, rootPost, gate)
  if (canReply) {
    return false
  }
  if (!allowFollower && !allowFollowing && !allowListUris?.length) {
    return true
  }
  const { ref } = db.dynamic
  const nullResult = sql<null>`${null}`
  const check = await db
    .selectFrom(valuesList([replierDid]).as(sql`subject (did)`))
    .select([
      allowFollower
        ? db
            .selectFrom('follow')
            .where('subjectDid', '=', ownerDid)
            .whereRef('creator', '=', ref('subject.did'))
            .select('subjectDid')
            .as('isFollower')
        : nullResult.as('isFollower'),
      allowFollowing
        ? db
            .selectFrom('follow')
            .where('creator', '=', ownerDid)
            .whereRef('subjectDid', '=', ref('subject.did'))
            .select('creator')
            .as('isFollowed')
        : nullResult.as('isFollowed'),
      allowListUris.length
        ? db
            .selectFrom('list_item')
            .where('list_item.listUri', 'in', allowListUris)
            .whereRef('list_item.subjectDid', '=', ref('subject.did'))
            .limit(1)
            .select('listUri')
            .as('isInList')
        : nullResult.as('isInList'),
    ])
    .executeTakeFirst()

  if (allowFollowing && check?.isFollowed) {
    return false
  } else if (allowFollower && check?.isFollower) {
    return false
  } else if (allowListUris.length && check?.isInList) {
    return false
  }

  return true
}

// Transpose an array of objects into an array of arrays, each corresponding to a column
// meant to bypass the length limit on VALUES lists by instead using unnest() on the result of this function
export const transpose = <T, const Out extends unknown[]>(
  objs: T[],
  transposeCol: (obj: T) => Out,
): Out[] => {
  const out: Out[] = []
  for (const obj of objs) {
    const transposed = transposeCol(obj)
    for (const i in transposed) {
      // @ts-expect-error
      ;(out[i] ??= []).push(transposed[i])
    }
  }
  return out
}

export const raw = (sql: string, parameters: unknown[]) =>
  Object.freeze({
    sql,
    query: RawNode.createWithSql(sql),
    parameters: Object.freeze(parameters),
  })

export const executeRaw = <RowType = unknown>(
  db: DatabaseSchema,
  sql: string,
  parameters: unknown[],
  queryId?: string,
) =>
  db.getExecutor().executeQuery<RowType>(raw(sql, parameters), {
    queryId: queryId ?? Math.random().toString(36).substring(2),
  })

export const copyIntoTable = async <
  Table extends keyof DatabaseSchemaType,
  AllColumns extends NonNullableInsertKeys<DatabaseSchemaType[Table]> & string,
  const Columns extends string[],
  Rows extends ArrayIncludesAll<AllColumns, Columns> extends true
    ? Record<NonNullableInsertKeys<DatabaseSchemaType[Table]>, unknown>[]
    : ['error: columns array is not exhaustive'],
>(
  pool: Pool,
  table: Table,
  columns: Columns,
  rows: Rows,
): Promise<Rows> => {
  const client = await pool.connect()

  const tempTable = `temp_${table}_${Math.random().toString(36).slice(2)}`
  const columnsStr = columns.map((c) => `"${c}"`).join(', ')

  await client.query(`BEGIN`)

  await client
    .query(
      `
      CREATE TEMP TABLE "${tempTable}" (${columnsStr})
      ON COMMIT DROP
      AS SELECT ${columnsStr} FROM "${table}" LIMIT 0
      WITH NO DATA
      `,
    )
    .catch((e) => {
      throw new Error(`Failed to create temp table ${tempTable}`, { cause: e })
    })

  const stream = client.query(
    copyFrom(
      `COPY "${tempTable}" (${columnsStr}) FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER E'\u0007', QUOTE E'\u0006')`,
      { highWaterMark: 1024 * 1024 },
    ),
  )

  const promise = new Promise<Rows>((resolve, reject) => {
    stream.once('finish', async () => {
      await client
        .query(
          `
          INSERT INTO "${table}" (${columnsStr})
          SELECT ${columnsStr} FROM "${tempTable}"
          ON CONFLICT DO NOTHING
          `,
        )
        .catch((e) => {
          throw new Error(`Failed to insert into ${table}`, { cause: e })
        })
      await client.query(`COMMIT`)
      resolve(rows)
    })

    stream.on('error', (err) => {
      console.error(
        `Error in COPY "${tempTable}" (${columnsStr}) FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER E'\u0007', QUOTE E'\u0006')`,
      )
      reject(err)
    })
  }).finally(() => client.release())

  const matchNull = /\u0000/g

  stream.write(
    rows
      .map((row) =>
        columns
          .map((c) =>
            row[c] != null
              ? `\u0006${
                  typeof row[c] === 'string' ? row[c] : JSON.stringify(row[c])
                }\u0006`
              : '',
          )
          .join('\u0007')
          .replace(matchNull, ''),
      )
      .join('\n'),
  )

  stream.end()
  return promise
}

type ArrayIncludes<T, Arr extends any[]> = Arr extends []
  ? false
  : Arr extends [infer Head, ...infer Tail]
    ? Head extends T
      ? true
      : ArrayIncludes<T, Tail>
    : never

type ArrayIncludesAll<T extends string, Arr extends any[]> = {
  [Member in T]: ArrayIncludes<Member, Arr>
}[T] extends true
  ? true
  : false
