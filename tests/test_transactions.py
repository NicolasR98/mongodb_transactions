import pytest


@pytest.mark.asyncio
async def test_transaction_commit(test_coll):
    async with await test_coll.database.client.start_session() as session:
        async with session.start_transaction():
            await test_coll.insert_one({"name": "Alice"}, session=session)
            await test_coll.insert_one({"name": "Bob"}, session=session)

    # Verify the documents were committed
    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 2
    assert docs[0]["name"] in ["Alice", "Bob"]
    assert docs[1]["name"] in ["Alice", "Bob"]


@pytest.mark.asyncio
async def test_transaction_abort(test_coll):
    await test_coll.delete_many({})
    async with await test_coll.database.client.start_session() as session:
        try:
            async with session.start_transaction():
                await test_coll.insert_one({"name": "Alice"}, session=session)
                await test_coll.insert_one({"name": "Bob"}, session=session)
                raise Exception("Fake error")
        except Exception:
            session.abort_transaction()

    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 0
