import asyncio

import pytest
from pymongo.errors import OperationFailure


@pytest.mark.asyncio
async def test_transaction_commit(test_coll):
    async with await test_coll.database.client.start_session() as session:
        async with session.start_transaction():
            await test_coll.insert_one({"name": "Alice"}, session=session)
            await test_coll.insert_one({"name": "Bob"}, session=session)

    docs = await test_coll.find().to_list(length=100)
    assert len(docs) == 2
    assert docs[0]["name"] == "Alice"
    assert docs[1]["name"] == "Bob"


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
    assert docs[2]["name"] == "Alice"
    assert docs[3]["name"] == "Bob"


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
async def test_embedded_transactions(test_coll):
    await test_coll.insert_one({"name": "Nico"})
    await test_coll.insert_one({"name": "JL"})

    # Session 1
    async with await test_coll.database.client.start_session() as session_1:
        try:
            # Transaction 1
            async with session_1.start_transaction():
                await test_coll.insert_one({"name": "Jonh"})

                # Session 2
                async with await test_coll.database.client.start_session() as session_2:
                    try:
                        # Transaction 2
                        async with session_2.start_transaction():
                            await test_coll.insert_one(
                                {"name": "Pepe"}, session=session_2
                            )
                            await test_coll.insert_one(
                                {"name": "Pepo"}, session=session_2
                            )
                    except Exception:
                        session_2.abort_transaction()

                await test_coll.insert_one({"name": "Doe"})

                # Check that Pepe and Pepo are on the right place of the list
                docs = await test_coll.find().to_list(length=100)
                assert docs[3]["name"] == "Pepe"
                assert docs[4]["name"] == "Pepo"

        except Exception:
            session_1.abort_transaction()

    docs = await test_coll.find().to_list(length=100)
    assert docs[0]["name"] == "Nico"
    assert docs[1]["name"] == "JL"
    assert docs[2]["name"] == "Jonh"
    assert docs[3]["name"] == "Pepe"
    assert docs[4]["name"] == "Pepo"
    assert docs[5]["name"] == "Doe"


@pytest.mark.asyncio
async def test_concurrent_transactions(test_coll):
    async def transaction_1():
        async with await test_coll.database.client.start_session() as session:
            async with session.start_transaction():
                await test_coll.insert_one({"name": "Nico"}, session=session)
                await test_coll.insert_one({"name": "JL"}, session=session)

    async def transaction_2():
        async with await test_coll.database.client.start_session() as session:
            async with session.start_transaction():
                await test_coll.insert_one({"name": "Paco"}, session=session)
                await test_coll.insert_one({"name": "Jesus"}, session=session)

    with pytest.raises(OperationFailure, match="TransientTransactionError"):
        await asyncio.gather(transaction_1(), transaction_2())

    # The result can be [Nico, JL] or [Paco, Jesus] as the operation is being executed in concurrence
    assert len(await test_coll.find().to_list(length=100)) == 2


@pytest.mark.asyncio
async def test_concurrent_transactions_abort(test_coll):
    async def transaction_1():
        async with await test_coll.database.client.start_session() as session:
            async with session.start_transaction():
                try:
                    await test_coll.insert_one({"name": "Nico"}, session=session)
                    await test_coll.insert_one({"name": "JL"}, session=session)
                    raise Exception
                except Exception:
                    session.abort_transaction()

    async def transaction_2():
        async with await test_coll.database.client.start_session() as session:
            async with session.start_transaction():
                await test_coll.insert_one({"name": "Paco"}, session=session)
                await test_coll.insert_one({"name": "Jesus"}, session=session)

    with pytest.raises(OperationFailure, match="TransientTransactionError"):
        await asyncio.gather(transaction_1(), transaction_2())

    # The result can be [Nico, JL] or [Paco, Jesus] as the operation is being executed in concurrence
    assert len(await test_coll.find().to_list(length=100)) == 2


# ! This test can raise a transaction conflict or not
# @pytest.mark.asyncio
# async def test_concurrent_transactions_conflict(test_coll):
#     await test_coll.insert_one({"name": "Juan"})

#     async def transaction_1():
#         async with await test_coll.database.client.start_session() as session:
#             async with session.start_transaction():
#                 await asyncio.sleep(2)
#                 await test_coll.update_one(
#                     {"name": "Juan"}, {"$set": {"name": "Juan 1"}}, session=session
#                 )

#     async def transaction_2():
#         async with await test_coll.database.client.start_session() as session:
#             async with session.start_transaction():
#                 await asyncio.sleep(2)
#                 await test_coll.update_one(
#                     {"name": "Juan"}, {"$set": {"name": "Juan 2"}}, session=session
#                 )

#     await asyncio.gather(transaction_1(), transaction_2())

#     assert len(await test_coll.find().to_list(length=100)) == 2
