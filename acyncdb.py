import asyncio
import aiohttp
from more_itertools import chunked
import platform
import asyncpg

import config

if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def get_json(session: aiohttp.ClientSession, person_id: int) -> dict:
    url = f'https://swapi.dev/api/people/{person_id}/'
    async with session.get(url) as resp:
        return await resp.json()


async def insert_users(pool: asyncpg.Pool, person_list):
    query = 'INSERT INTO persons (id, birth_year, eye_color, films,' \
            'gender, hair_color, height, homeworld, mass, name, skin_color,' \
            'species, starships, vehicles) VALUES ($1, $2, $3, $4, $5, $6, $7,' \
            '$8, $9, $10, $11, $12, $13, $14)'
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, person_list)


async def main(count_person: int):
    tasks = []
    pool = await asyncpg.create_pool(config.PG_DSN, min_size=20, max_size=20)

    async with aiohttp.ClientSession() as session:
        for chunk in chunked(range(1, count_person + 1), 10):
            person_tasks = [asyncio.create_task(get_json(session, i)) for i in chunk]
            for task in person_tasks:
                res = await task
                tasks.append(asyncio.create_task(insert_users(pool, res)))
        await asyncio.gather(*tasks)
        await pool.close()


if __name__ == '__main__':
    asyncio.run(main(50))
