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
        response_json = await resp.json()
        return response_json


async def insert_users(pool: asyncpg.Pool, person_list):
    query = 'INSERT INTO persons (name, height, mass, hair_color,' \
            'skin_color, eye_color, birth_year, gender, homeworld, films, species,' \
            'vehicles, starships) VALUES ($1, $2, $3, $4, $5, $6, $7,' \
            '$8, $9, $10, $11, $12, $13)'
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, person_list)


async def main(count_person: int):
    tasks = []
    persons_list = []
    pool = await asyncpg.create_pool(config.PG_DSN, min_size=20, max_size=20)

    async with aiohttp.ClientSession() as session:
        for chunk in chunked(range(1, count_person + 1), 10):
            person_tasks = [asyncio.create_task(get_json(session, i)) for i in chunk]
            persons = await asyncio.gather(*person_tasks)
            for person in persons:
                persons_list.append(tuple(person.values())[:-3])
            tasks.append(asyncio.create_task(insert_users(pool, persons_list)))

        await asyncio.gather(*tasks)
        await pool.close()


if __name__ == '__main__':
    asyncio.run(main(2))
