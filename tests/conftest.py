import os

import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = os.getenv("MONGO_URI")


@pytest_asyncio.fixture
async def mongo_client():
    client = AsyncIOMotorClient(MONGO_URI)
    yield client
    client.close()


@pytest_asyncio.fixture
async def test_db(mongo_client):
    db = mongo_client["test_transactions"]
    yield db
    await mongo_client.drop_database("test_transactions")


@pytest_asyncio.fixture
async def test_coll(test_db):
    coll = test_db["testcoll"]
    yield coll
    await coll.delete_many({})
