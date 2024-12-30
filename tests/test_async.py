import pytest
import pytest_asyncio
import asyncio
from src import (
    HookLoopModel,
    HookLoopTable,
)
from src.hookloopdb.controller import AsyncSQLiteController
from typing import Any, Optional


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
    indexes = await setup_table.controller.connection.execute(
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
    print(saved_id)
    print(model)

    async with HookLoopModelTest._table.controller.connection.execute(
        "SELECT id, data FROM test_table"
    ) as cursor:
        rows = await cursor.fetchall()
        print("Stored Rows:", rows)

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
