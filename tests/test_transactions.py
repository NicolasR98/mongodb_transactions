import pytest


@pytest.mark.asyncio
async def test_transaction_commit(test_coll):
    async with await test_coll.database.client.start_session() as session:
        async with session.start_transaction():
            await test_coll.insert_one({"name": "Alice"}, session=session)
            await test_coll.insert_one({"name": "Bob"}, session=session)

    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 2
    assert docs[0]["name"] in ["Alice", "Bob"]
    assert docs[1]["name"] in ["Alice", "Bob"]


@pytest.mark.asyncio
async def test_transaction_commit_with_existing_data(test_coll):
    await test_coll.insert_one({"name": "Nico"})
    await test_coll.insert_one({"name": "JL"})

    async with await test_coll.database.client.start_session() as session:
        async with session.start_transaction():
            await test_coll.insert_one({"name": "Alice"}, session=session)
            await test_coll.insert_one({"name": "Bob"}, session=session)

    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 4
    assert docs[2]["name"] in ["Alice", "Bob"]
    assert docs[3]["name"] in ["Alice", "Bob"]


@pytest.mark.asyncio
async def test_transaction_commit_update(test_coll):
    await test_coll.insert_one({"name": "Nico"})
    await test_coll.insert_one({"name": "JL"})

    async with await test_coll.database.client.start_session() as session:
        async with session.start_transaction():
            await test_coll.update_one(
                {"name": "Nico"}, {"$set": {"name": "Juan"}}, session=session
            )

    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 2
    assert docs[0]["name"] == "Juan"
    assert docs[1]["name"] == "JL"


@pytest.mark.asyncio
async def test_transaction_commit_combined_operations(test_coll):
    await test_coll.insert_one({"name": "Nico"})
    await test_coll.insert_one({"name": "JL"})

    async with await test_coll.database.client.start_session() as session:
        async with session.start_transaction():
            await test_coll.delete_one({"name": "JL"}, session=session)
            await test_coll.update_one(
                {"name": "Nico"}, {"$set": {"name": "Juan"}}, session=session
            )
            await test_coll.insert_one({"name": "Alice"}, session=session)

    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 2
    assert not any(doc["name"] == "JL" for doc in docs)
    assert docs[0]["name"] == "Juan"
    assert docs[1]["name"] == "Alice"


@pytest.mark.asyncio
async def test_transaction_abort(test_coll):
    async with await test_coll.database.client.start_session() as session:
        try:
            async with session.start_transaction():
                await test_coll.insert_one({"name": "Jonh"}, session=session)
                await test_coll.insert_one({"name": "Doe"}, session=session)
                raise Exception("Fake error")
        except Exception:
            session.abort_transaction()

    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 0


@pytest.mark.asyncio
async def test_transaction_abort_with_existing_data(test_coll):
    await test_coll.insert_one({"name": "Nico"})
    await test_coll.insert_one({"name": "JL"})

    async with await test_coll.database.client.start_session() as session:
        try:
            async with session.start_transaction():
                await test_coll.insert_one({"name": "Jonh"}, session=session)
                await test_coll.insert_one({"name": "Doe"}, session=session)
                raise Exception("Fake error")
        except Exception:
            session.abort_transaction()

    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 2


@pytest.mark.asyncio
async def test_transaction_abort_combined_operations(test_coll):
    await test_coll.insert_one({"name": "Nico"})
    await test_coll.insert_one({"name": "JL"})

    async with await test_coll.database.client.start_session() as session:
        try:
            async with session.start_transaction():
                await test_coll.delete_one({"name": "JL"}, session=session)
                await test_coll.update_one(
                    {"name": "Nico"}, {"$set": {"name": "Juan"}}, session=session
                )
                await test_coll.insert_one({"name": "Alice"}, session=session)
                raise Exception("Fake error")
        except Exception:
            session.abort_transaction()

    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 2
    assert docs[0]["name"] == "Nico"
    assert docs[1]["name"] == "JL"


@pytest.mark.asyncio
async def test_transaction_commit_and_abort(test_coll):
    await test_coll.insert_one({"name": "Nico"})
    await test_coll.insert_one({"name": "JL"})

    async with await test_coll.database.client.start_session() as session:
        try:
            async with session.start_transaction():
                await test_coll.delete_one({"name": "JL"}, session=session)
                await test_coll.update_one(
                    {"name": "Nico"}, {"$set": {"name": "Juan"}}, session=session
                )
                session.commit_transaction()
                await test_coll.insert_one({"name": "Alice"}, session=session)
                raise Exception("Fake error")
        except Exception:
            session.abort_transaction()

    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 1
    assert docs[0]["name"] == "Juan"
