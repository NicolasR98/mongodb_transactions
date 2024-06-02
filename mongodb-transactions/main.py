import asyncio
import os

from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = os.getenv("MONGO_URI")


async def main() -> None:
    client = AsyncIOMotorClient(MONGO_URI)
    db = client["test_transactions"]
    coll = db["testcoll"]

    async with await client.start_session() as session:
        async with session.start_transaction():
            try:
                await coll.insert_one({"name": "Alice"}, session=session)
                await coll.insert_one({"name": "Bob"}, session=session)
                print("Transaction committed.")
            except Exception as e:
                print(f"Transaction aborted due to error: {e}")
                await session.abort_transaction()

    client.close()


asyncio.run(main())
