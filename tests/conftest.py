import asyncio
import os

import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = os.getenv("MONGO_URI")


@pytest_asyncio.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def mongo_client(event_loop):
    client = AsyncIOMotorClient(MONGO_URI)
    yield client
    client.close()


@pytest_asyncio.fixture(scope="session")
async def test_db(mongo_client):
    db = mongo_client["test_transactions"]
    yield db
    await mongo_client.drop_database("test_transactions")


@pytest_asyncio.fixture(scope="session")
async def test_coll(test_db):
    coll = test_db["testcoll"]
    await coll.delete_many({})
    yield coll
