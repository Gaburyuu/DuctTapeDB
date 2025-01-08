import pytest
import pytest_asyncio
import asyncio
from src import (
    HookLoopModel,
    HookLoopTable,
)
from src.ducttapedb.hookloopdb.controller import AsyncSQLiteController
from typing import Optional


class HookLoopModelTest(HookLoopModel):
    key1: Optional[str] = None
    key2: Optional[int] = None


@pytest_asyncio.fixture()
async def setup_table():
    """Fixture to initialize HookLoopTable."""
    controller = await AsyncSQLiteController.create_memory(shared_cache=True)
    table = HookLoopTable(controller, "test_table")
    await table.initialize(indexes=["key1", "key2"])
    yield table
    await controller.close()


@pytest_asyncio.fixture
async def setup_models(setup_table):
    """Fixture to initialize HookLoopModel with the table."""
    HookLoopModel.set_table(setup_table)
    yield


@pytest.mark.asyncio
async def test_index_creation(setup_table):
    await setup_table.initialize(indexes=["key1"])
    indexes = await setup_table.controller._connection.execute(
        "PRAGMA index_list('test_table');"
    )
    rows = await indexes.fetchall()
    assert any("idx_test_table_key1" in row[1] for row in rows)


@pytest.mark.asyncio
async def test_upsert_table(setup_table):
    """Test upserting a document into the table."""
    doc = {"id": None, "data": {"key1": "value1"}}
    doc_id = await setup_table.upsert(doc)
    assert doc_id is not None


@pytest.mark.asyncio
async def test_find_table(setup_table):
    """Test finding a document in the table by ID."""
    doc = {"id": 1, "data": {"key1": "value1"}}
    await setup_table.upsert(doc)
    result = await setup_table.find(1)
    assert result is not None
    assert result["data"]["key1"] == "value1"


@pytest.mark.asyncio
async def test_search_table(setup_table):
    """Test searching for documents by conditions."""
    await setup_table.upsert({"id": 2, "data": {"key1": "value2", "key2": 20}})
    await setup_table.upsert({"id": 3, "data": {"key1": "value3", "key2": 30}})
    results = await setup_table.search({"key1": "value2"})
    assert len(results) == 1
    assert results[0]["id"] == 2


@pytest.mark.asyncio
async def test_search_advanced_table(setup_table):
    """Test advanced search with multiple conditions."""
    await setup_table.upsert({"id": 2, "data": {"key1": "value2", "key2": 20}})
    await setup_table.upsert({"id": 3, "data": {"key1": "value3", "key2": 30}})
    results = await setup_table.search_advanced(
        [
            {"key": "key1", "value": "value3", "operator": "="},
            {"key": "key2", "value": 30, "operator": ">="},
        ]
    )
    assert len(results) == 1
    assert results[0]["id"] == 3


@pytest.mark.asyncio
async def test_delete_table(setup_table):
    """Test deleting a document by ID."""
    await setup_table.delete_document(2)
    result = await setup_table.find(2)
    assert result is None


@pytest.mark.asyncio
async def test_model_save(setup_models):
    """Test saving a HookLoopModel instance."""
    model = HookLoopModelTest(id=None, key1="value4")
    saved_id = await model.save()
    assert saved_id is not None


@pytest.mark.asyncio
async def test_model_from_id(setup_models):
    """Test retrieving a model by ID using from_id."""
    model = HookLoopModelTest(id=None, key1="value5")
    saved_id = await model.save()
    fetched_model = await HookLoopModelTest.from_id(saved_id)
    assert fetched_model.id == saved_id
    assert fetched_model.key1 == "value5"


@pytest.mark.asyncio
async def test_model_from_id_and(setup_models):
    """Test retrieving a model by ID with additional conditions using from_id_and."""
    model = HookLoopModelTest(id=None, key1="value6", key2=60)
    saved_id = await model.save()

    # Successful retrieval with matching conditions
    fetched_model = await HookLoopModelTest.from_id_and(
        doc_id=saved_id, conditions={"key1": "value6", "key2": 60}
    )
    assert fetched_model.id == saved_id
    assert fetched_model.key1 == "value6"

    # Unsuccessful retrieval with non-matching conditions
    with pytest.raises(ValueError):
        await HookLoopModelTest.from_id_and(
            doc_id=saved_id, conditions={"key1": "value6", "key2": 100}
        )


@pytest.mark.asyncio
async def test_model_bulk_save(setup_models):
    """Test bulk saving multiple HookLoopModel instances."""
    models = [
        HookLoopModelTest(id=None, key1="bulk1"),
        HookLoopModelTest(id=None, key1="bulk1"),
    ]
    ids = await HookLoopModelTest.bulk_save(models)
    assert len(ids) == len(models)

    # Verify IDs were assigned
    for model, model_id in zip(models, ids):
        assert model.id == model_id


@pytest_asyncio.fixture()
async def model_tester_table_setup():
    """Fixture to initialize HookLoopTable."""
    controller = await AsyncSQLiteController.create_memory(shared_cache=True)
    table = HookLoopTable(controller, "test_table")
    await table.initialize(indexes=["key1", "key2"])
    yield table
    await controller.close()


class ModelTesterModel(HookLoopModel):
    name: str


@pytest_asyncio.fixture
async def model_tester_model_setup(model_tester_table_setup):
    """Fixture to initialize HookLoopModel with the table."""
    ModelTesterModel.set_table(model_tester_table_setup)
    yield


@pytest.mark.asyncio
async def test_bulk_save_inherited_model(model_tester_model_setup):
    """Test bulk saving instances of an inherited model."""
    models = [
        ModelTesterModel(id=None, name="name number 1"),
        ModelTesterModel(id=None, name="name number 2"),
    ]
    ids = await ModelTesterModel.bulk_save(models)
    assert len(ids) == len(models)

    # Verify IDs were assigned
    for model, model_id in zip(models, ids):
        assert model.id == model_id


@pytest.mark.asyncio
async def test_concurrent_connection_reuse():
    try:
        controller = await AsyncSQLiteController.create_memory(shared_cache=True)
        table = HookLoopTable(controller, "test_table")
        await table.initialize(indexes=["key1"])

        async def upsert_task(task_id):
            await table.upsert({"id": task_id, "data": {"key1": f"value{task_id}"}})

        tasks = [upsert_task(i) for i in range(1000)]
        await asyncio.gather(*tasks)
        results = await table.search({"key1": "value5"})

        assert len(results) == 1
        assert results[0]["data"]["key1"] == "value5"
    finally:
        await controller.close()


@pytest.mark.asyncio
async def test_bulk_save_large_batch(setup_models):
    """Test bulk saving a large batch of models."""
    large_batch = [HookLoopModelTest(id=None, key1=f"bulk{i}") for i in range(1000)]
    ids = await HookLoopModelTest.bulk_save(large_batch)

    # Assert all IDs were assigned
    assert len(ids) == len(large_batch)
    for model, model_id in zip(large_batch, ids):
        assert model.id == model_id

    # Verify data integrity for a subset
    sample = await HookLoopModelTest._table.search({"key1": "bulk500"})
    assert len(sample) == 1
    assert sample[0]["data"]["key1"] == "bulk500"


@pytest.mark.asyncio
async def test_search_advanced_operators(setup_table):
    """Test advanced search with all supported operators."""
    # Insert sample data
    await setup_table.upsert({"id": 1, "data": {"key1": "value1", "key2": 10}})
    await setup_table.upsert({"id": 2, "data": {"key1": "value2", "key2": 20}})
    await setup_table.upsert({"id": 3, "data": {"key1": "value3", "key2": 30}})

    # Test valid operators
    operators = ["=", "!=", "<", ">", "<=", ">="]
    conditions = [{"key": "key2", "value": 20, "operator": op} for op in operators]
    results = []
    for condition in conditions:
        result = await setup_table.search_advanced([condition])
        results.append(result)

    # Assertions for each operator
    assert len(results[0]) == 1  # '='
    assert len(results[1]) == 2  # '!='
    assert len(results[2]) == 1  # '<'
    assert len(results[3]) == 1  # '>'
    assert len(results[4]) == 2  # '<='
    assert len(results[5]) == 2  # '>='

    # Test invalid operator
    with pytest.raises(ValueError):
        await setup_table.search_advanced(
            [{"key": "key2", "value": 20, "operator": "INVALID"}]
        )


@pytest.mark.asyncio
async def test_context_manager():
    """Test the AsyncSQLiteController context manager."""
    async with await AsyncSQLiteController.create_memory(
        shared_cache=True
    ) as controller:
        # Verify connection is open
        assert controller._connection is not None

        # Execute a simple query
        await controller.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);"
        )
        await controller.execute("INSERT INTO test (name) VALUES (?);", ["Test Name"])

        # Verify data insertion
        cursor = await controller.execute("SELECT name FROM test;")
        rows = [row[0] async for row in cursor]
        assert rows == ["Test Name"]

    # After exiting the context, the connection should be closed
    assert controller._connection is None


@pytest.mark.asyncio
async def test_model_delete(setup_models):
    """Test deleting a model."""
    # Create and save a model
    model = HookLoopModelTest(id=None, key1="delete_me", key2=42)
    saved_id = await model.save()

    # Verify it exists in the database
    fetched_model = await HookLoopModelTest.from_id(saved_id)
    assert fetched_model is not None

    # Delete the model
    await model.delete()

    # Ensure it no longer exists
    with pytest.raises(ValueError):
        await HookLoopModelTest.from_id(saved_id)


@pytest.mark.asyncio
async def test_search_advanced_with_in_and_conditions(setup_table):
    """Test advanced search with IN operator and multiple conditions."""
    # Insert test data
    await setup_table.upsert({"id": 1, "data": {"key1": "value1", "key2": 10}})
    await setup_table.upsert({"id": 2, "data": {"key1": "value2", "key2": 20}})
    await setup_table.upsert({"id": 3, "data": {"key1": "value3", "key2": 30}})
    await setup_table.upsert({"id": 4, "data": {"key1": "value4", "key2": 40}})

    # Test IN operator
    results = await setup_table.search_advanced(
        [{"key": "key2", "operator": "IN", "value": [10, 30, 40]}]
    )
    assert len(results) == 3
    assert {result["id"] for result in results} == {1, 3, 4}

    # Test multiple conditions (AND)
    results = await setup_table.search_advanced(
        [
            {"key": "key2", "operator": ">", "value": 10},
            {"key": "key2", "operator": "<", "value": 40},
        ]
    )
    assert len(results) == 2
    assert {result["id"] for result in results} == {2, 3}


@pytest.mark.asyncio
async def test_model_json_ordering(setup_models):
    """Test models_from_db with ordering by a JSON field."""
    # Create and save multiple models
    model1 = HookLoopModelTest(id=None, key1="Item A", key2=15)
    model2 = HookLoopModelTest(id=None, key1="Item B", key2=9)
    model3 = HookLoopModelTest(id=None, key1="Item C", key2=19)
    model4 = HookLoopModelTest(id=None, key1="Item D", key2=12)

    # Save models to the database
    await model1.save()
    await model2.save()
    await model3.save()
    await model4.save()

    # Retrieve models ordered by `key2` (ascending)
    models_asc = await HookLoopModelTest.models_from_db(
        order_by='json_extract(data, "$.key2") ASC'
    )

    # Verify the models are sorted correctly
    assert len(models_asc) == 4
    assert models_asc[0].key1 == "Item B"  # Lowest key2
    assert models_asc[1].key1 == "Item D"
    assert models_asc[2].key1 == "Item A"
    assert models_asc[3].key1 == "Item C"  # Highest key2

    # Retrieve models ordered by `key2` (descending) with a limit
    models_desc = await HookLoopModelTest.models_from_db(
        order_by='json_extract(data, "$.key2") DESC', limit=2
    )

    # Verify the models are sorted correctly in descending order
    assert len(models_desc) == 2
    assert models_desc[0].key1 == "Item C"  # Highest key2
    assert models_desc[1].key1 == "Item A"  # Second highest key2


@pytest.mark.benchmark
def test_bulk_insert_benchmark(benchmark, setup_table):
    """Benchmark bulk insert operation asynchronously."""

    async def bulk_insert():
        for i in range(1000):
            await setup_table.upsert({"id": i, "data": {"key": f"value{i}"}})

    def run_bulk_insert():
        asyncio.run(bulk_insert())

    # Benchmark the synchronous wrapper
    benchmark(run_bulk_insert)


@pytest.mark.benchmark
def test_bulk_save_model_benchmark(benchmark, setup_models):
    """Benchmark bulk save operation for the model."""

    async def bulk_save():
        models = [HookLoopModelTest(id=None, key1=f"bulk{i}") for i in range(1000)]
        await HookLoopModelTest.bulk_save(models)

    def run_bulk_save():
        asyncio.run(bulk_save())

    benchmark(run_bulk_save)
