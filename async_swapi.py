import datetime
import asyncio
from more_itertools import chunked
from models import init_db, SwapiPeople, Session
import aiohttp
from typing import List


CHUNK_SIZE = 10

async def get_person(person_id, session):
    response = await session.get(f'https://swapi.py4e.com/api/people/{person_id}/')
    person_data = await response.json()
    return person_data

async def insert_to_db(people_dict: List[dict]):
    async with Session() as session:
        people = [SwapiPeople(json=person) for person in people_dict]
        session.add_all(people)
        await session.commit()


async def main():
    await init_db()
    session = aiohttp.ClientSession()
    for people_id_chunk in chunked(range(1, 100), CHUNK_SIZE):
        coros = [get_person(person_id, session) for person_id in people_id_chunk]
        result = await asyncio.gather(*coros)
        await insert_to_db(result)
        print(result)
    await session.close()


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
