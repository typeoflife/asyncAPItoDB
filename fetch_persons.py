import asyncio

import asyncpg

import config


async def fetch(pool: asyncpg.Pool, query):
    async with pool.acquire() as con:
        async with con.transaction():
            async for record in con.cursor(query):
                yield record


async def main():
    pool = await asyncpg.create_pool(config.PG_DSN)
    async for record in fetch(pool, 'SELECT * from persons'):
        print(record['id'], record['name'], record['mass'])
    await pool.close()

if __name__ == '__main__':
    asyncio.run(main())