import pytest
import pytest_asyncio
import asyncio
from src import HookLoopModel, HookLoopTable
from src.ducttapedb.hookloopdb.controller import AsyncSQLiteController
from typing import Optional


# Define a test model for our HookLoopModel tests.
class HookLoopModelTest(HookLoopModel):
    key1: Optional[str] = None
    key2: Optional[int] = None


# A secondary model to test inheritance.
class ModelTesterModel(HookLoopModel):
    name: str


# ------------------------
# Fixtures
# ------------------------


@pytest_asyncio.fixture()
async def setup_table():
    """Fixture to initialize HookLoopTable with a memory database."""
    controller = await AsyncSQLiteController.create_memory(shared_cache=True)
    table = HookLoopTable(controller, "test_table")
    await table.initialize(indexes=["key1", "key2"])
    yield table
    await controller.close()


@pytest_asyncio.fixture
async def setup_models(setup_table):
    """Fixture to set the table for HookLoopModelTest."""
    HookLoopModelTest.set_table(setup_table)
    yield


@pytest_asyncio.fixture()
async def model_tester_table_setup():
    """Fixture to initialize HookLoopTable for ModelTesterModel tests."""
    controller = await AsyncSQLiteController.create_memory(shared_cache=True)
    table = HookLoopTable(controller, "test_table")
    await table.initialize(indexes=["key1", "key2"])
    yield table
    await controller.close()


@pytest_asyncio.fixture
async def model_tester_model_setup(model_tester_table_setup):
    """Fixture to set the table for ModelTesterModel."""
    ModelTesterModel.set_table(model_tester_table_setup)
    yield


# ------------------------
# Original Tests (with minor fixes)
# ------------------------


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
    doc = {"id": None, "data": {"key1": "value1"}}
    doc_id = await setup_table.upsert(doc)
    assert doc_id is not None


@pytest.mark.asyncio
async def test_find_table(setup_table):
    doc = {"id": 1, "data": {"key1": "value1"}}
    await setup_table.upsert(doc)
    result = await setup_table.find(1)
    assert result is not None
    assert result["data"]["key1"] == "value1"


@pytest.mark.asyncio
async def test_search_table(setup_table):
    await setup_table.upsert({"id": 2, "data": {"key1": "value2", "key2": 20}})
    await setup_table.upsert({"id": 3, "data": {"key1": "value3", "key2": 30}})
    results = await setup_table.search({"key1": "value2"})
    assert len(results) == 1
    assert results[0]["id"] == 2


@pytest.mark.asyncio
async def test_search_advanced_table(setup_table):
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
    await setup_table.delete_document(2)
    result = await setup_table.find(2)
    assert result is None


@pytest.mark.asyncio
async def test_model_save(setup_models):
    model = HookLoopModelTest(id=None, key1="value4")
    saved_id = await model.save()
    assert saved_id is not None


@pytest.mark.asyncio
async def test_model_from_id(setup_models):
    model = HookLoopModelTest(id=None, key1="value5")
    saved_id = await model.save()
    fetched_model = await HookLoopModelTest.from_id(saved_id)
    assert fetched_model.id == saved_id
    assert fetched_model.key1 == "value5"


@pytest.mark.asyncio
async def test_model_from_id_and(setup_models):
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
    models = [
        HookLoopModelTest(id=None, key1="bulk1"),
        HookLoopModelTest(id=None, key1="bulk1"),
    ]
    ids = await HookLoopModelTest.bulk_save(models)
    assert len(ids) == len(models)
    for model, model_id in zip(models, ids):
        assert model.id == model_id


@pytest.mark.asyncio
async def test_bulk_save_inherited_model(model_tester_model_setup):
    models = [
        ModelTesterModel(id=None, name="name number 1"),
        ModelTesterModel(id=None, name="name number 2"),
    ]
    ids = await ModelTesterModel.bulk_save(models)
    assert len(ids) == len(models)
    for model, model_id in zip(models, ids):
        assert model.id == model_id


@pytest.mark.asyncio
async def test_concurrent_connection_reuse():
    try:
        controller = await AsyncSQLiteController.create_memory(shared_cache=True)
        table = HookLoopTable(controller, "test_table")
        await table.initialize(indexes=["key1"])

        async def upsert_task(task_id):
            await table.upsert(
                {"id": task_id, "data": {"key1": f"concurrent-value-{task_id}"}}
            )

        tasks = [upsert_task(i) for i in range(1000)]
        await asyncio.gather(*tasks)
        # Ensure at least one known document is present:
        results = await table.search({"key1": "concurrent-value-5"})
        assert len(results) == 1
        assert results[0]["data"]["key1"] == "concurrent-value-5"
    finally:
        await controller.close()


@pytest.mark.asyncio
async def test_bulk_save_large_batch(setup_models):
    large_batch = [HookLoopModelTest(id=None, key1=f"bulk{i}") for i in range(1000)]
    ids = await HookLoopModelTest.bulk_save(large_batch)
    assert len(ids) == len(large_batch)
    for model, model_id in zip(large_batch, ids):
        assert model.id == model_id
    sample = await HookLoopModelTest._table.search({"key1": "bulk500"})
    assert len(sample) == 1
    assert sample[0]["data"]["key1"] == "bulk500"


@pytest.mark.asyncio
async def test_search_advanced_operators(setup_table):
    await setup_table.upsert({"id": 1, "data": {"key1": "value1", "key2": 10}})
    await setup_table.upsert({"id": 2, "data": {"key1": "value2", "key2": 20}})
    await setup_table.upsert({"id": 3, "data": {"key1": "value3", "key2": 30}})
    operators = ["=", "!=", "<", ">", "<=", ">="]
    results = []
    for op in operators:
        condition = {"key": "key2", "value": 20, "operator": op}
        result = await setup_table.search_advanced([condition])
        results.append(result)
    assert len(results[0]) == 1  # '='
    assert len(results[1]) == 2  # '!='
    assert len(results[2]) == 1  # '<'
    assert len(results[3]) == 1  # '>'
    assert len(results[4]) == 2  # '<='
    assert len(results[5]) == 2  # '>='
    with pytest.raises(ValueError):
        await setup_table.search_advanced(
            [{"key": "key2", "value": 20, "operator": "INVALID"}]
        )


@pytest.mark.asyncio
async def test_context_manager():
    controller = await AsyncSQLiteController.create_memory(shared_cache=True)
    async with controller:
        assert controller._connection is not None
        await controller.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);"
        )
        await controller.execute("INSERT INTO test (name) VALUES (?);", ["Test Name"])
        cursor = await controller.execute("SELECT name FROM test;")
        rows = [row[0] async for row in cursor]
        assert rows == ["Test Name"]
    # After context manager, the connection remains open
    assert controller._connection is not None
    await controller.close()
    assert controller._connection is None


@pytest.mark.asyncio
async def test_model_delete(setup_models):
    model = HookLoopModelTest(id=None, key1="delete_me", key2=42)
    saved_id = await model.save()
    fetched_model = await HookLoopModelTest.from_id(saved_id)
    assert fetched_model is not None
    await model.delete()
    with pytest.raises(ValueError):
        await HookLoopModelTest.from_id(saved_id)


@pytest.mark.asyncio
async def test_search_advanced_with_in_and_conditions(setup_table):
    await setup_table.upsert({"id": 1, "data": {"key1": "value1", "key2": 10}})
    await setup_table.upsert({"id": 2, "data": {"key1": "value2", "key2": 20}})
    await setup_table.upsert({"id": 3, "data": {"key1": "value3", "key2": 30}})
    await setup_table.upsert({"id": 4, "data": {"key1": "value4", "key2": 40}})
    results = await setup_table.search_advanced(
        [{"key": "key2", "operator": "IN", "value": [10, 30, 40]}]
    )
    assert len(results) == 3
    assert {result["id"] for result in results} == {1, 3, 4}
    results = await setup_table.search_advanced(
        [
            {"key": "key2", "operator": ">", "value": 10},
            {"key": "key2", "operator": "<", "value": 40},
        ]
    )
    assert len(results) == 2
    assert {result["id"] for result in results} == {2, 3}


@pytest.mark.asyncio
async def test_model_json_ordering_with_filter(setup_models):
    model1 = HookLoopModelTest(id=None, key1="Item A- JSON", key2=15)
    model2 = HookLoopModelTest(id=None, key1="Item B- JSON", key2=9)
    model3 = HookLoopModelTest(id=None, key1="Item C- JSON", key2=19)
    model4 = HookLoopModelTest(id=None, key1="Item D- JSON", key2=12)
    await model1.save()
    await model2.save()
    await model3.save()
    await model4.save()
    models_asc = await HookLoopModelTest.models_from_db(
        order_by='json_extract(data, "$.key2") ASC',
        filter_sql="json_extract(data, '$.key1') LIKE ?",
        filter_params=["%JSON"],
    )
    assert len(models_asc) == 4
    assert models_asc[0].key1 == "Item B- JSON"  # Lowest key2
    assert models_asc[1].key1 == "Item D- JSON"
    assert models_asc[2].key1 == "Item A- JSON"
    assert models_asc[3].key1 == "Item C- JSON"  # Highest key2
    models_desc = await HookLoopModelTest.models_from_db(
        order_by='json_extract(data, "$.key2") DESC',
        limit=2,
        filter_sql="json_extract(data, '$.key1') LIKE ?",
        filter_params=["%JSON"],
    )
    assert len(models_desc) == 2
    assert models_desc[0].key1 == "Item C- JSON"
    assert models_desc[1].key1 == "Item A- JSON"


@pytest.mark.benchmark
def test_bulk_insert_benchmark(benchmark, setup_table):
    async def bulk_insert():
        for i in range(1000):
            await setup_table.upsert({"id": i, "data": {"key": f"value{i}"}})

    def run_bulk_insert():
        asyncio.run(bulk_insert())

    benchmark(run_bulk_insert)


@pytest.mark.benchmark
def test_bulk_save_model_benchmark(benchmark, setup_models):
    async def bulk_save():
        models = [HookLoopModelTest(id=None, key1=f"bulk{i}") for i in range(1000)]
        await HookLoopModelTest.bulk_save(models)

    def run_bulk_save():
        asyncio.run(bulk_save())

    benchmark(run_bulk_save)


# ------------------------
# Additional Tests for Better Coverage
# ------------------------


@pytest.mark.asyncio
async def test_search_basic(setup_table):
    """
    Test the search_basic method, which was causing issues
    due to using an incorrect attribute name in the query.
    """
    await setup_table.upsert({"id": 10, "data": {"test_key": "test_value"}})
    results = await setup_table.search_basic("test_key", "test_value")
    assert len(results) == 1
    assert results[0]["id"] == 10
    assert results[0]["data"]["test_key"] == "test_value"


@pytest.mark.asyncio
async def test_get_non_data_columns(setup_table):
    """
    Ensure that get_non_data_columns returns only the non-JSON columns.
    """
    columns = await setup_table.get_non_data_columns()
    assert "id" in columns
    assert "data" not in columns


@pytest.mark.asyncio
async def test_model_refresh(setup_models):
    """
    Test that a model instance refreshes its data correctly when the underlying
    database record is updated externally.
    """
    model = HookLoopModelTest(id=None, key1="original")
    saved_id = await model.save()
    # Simulate an external update.
    new_data = {"key1": "updated"}
    await model._table.upsert({"id": saved_id, "data": new_data})
    await model.refresh()
    assert model.key1 == "updated"


# A dummy model to test error conditions when no table is set.
class DummyModel(HookLoopModel):
    dummy_field: str


@pytest.mark.asyncio
async def test_from_id_without_table():
    with pytest.raises(AttributeError):
        await DummyModel.from_id(1)


@pytest.mark.asyncio
async def test_save_without_table():
    model = HookLoopModelTest(id=None, key1="value")
    # Force the table to be None.
    HookLoopModelTest._table = None
    with pytest.raises(ValueError):
        await model.save()


@pytest.mark.asyncio
async def test_delete_without_id(setup_models):
    model = HookLoopModelTest(id=None, key1="value")
    with pytest.raises(ValueError):
        await model.delete()


@pytest.mark.asyncio
async def test_search_empty_conditions(setup_table):
    with pytest.raises(ValueError):
        await setup_table.search({})


@pytest.mark.asyncio
async def test_search_advanced_empty_filters(setup_table):
    with pytest.raises(ValueError):
        await setup_table.search_advanced([])


@pytest.mark.asyncio
async def test_bulk_save_non_model(setup_models):
    model = HookLoopModelTest(id=None, key1="valid")
    not_model = {"id": None, "key1": "invalid"}
    with pytest.raises(ValueError):
        await HookLoopModelTest.bulk_save([model, not_model])


@pytest.mark.asyncio
async def test_models_from_db_no_table():
    original_table = HookLoopModelTest._table
    HookLoopModelTest._table = None
    with pytest.raises(ValueError):
        await HookLoopModelTest.models_from_db()
    HookLoopModelTest._table = original_table


@pytest.mark.asyncio
async def test_search_all(setup_table):
    for i in range(5):
        await setup_table.upsert({"id": i + 1, "data": {"value": i}})
    results = await setup_table.search_all(order_by="id DESC", limit=3)
    assert len(results) == 3
    ids = [doc["id"] for doc in results]
    assert ids == sorted(ids, reverse=True)


@pytest.mark.asyncio
async def test_execute_script(setup_table):
    script = """
    CREATE TABLE IF NOT EXISTS test_script (id INTEGER PRIMARY KEY, name TEXT);
    INSERT INTO test_script (name) VALUES ('Script Test');
    """
    await setup_table.controller.execute_script(script)
    cursor = await setup_table.controller.execute("SELECT name FROM test_script;")
    rows = await cursor.fetchall()
    assert rows[0][0] == "Script Test"


@pytest.mark.asyncio
async def test_executemany(setup_table):
    await setup_table.controller.execute(
        "CREATE TABLE IF NOT EXISTS test_batch (id INTEGER PRIMARY KEY, val TEXT)"
    )
    data = [(None, f"val{i}") for i in range(5)]
    await setup_table.controller.executemany(
        "INSERT INTO test_batch (id, val) VALUES (?, ?)", data
    )
    cursor = await setup_table.controller.execute("SELECT val FROM test_batch;")
    rows = await cursor.fetchall()
    values = [row[0] for row in rows]
    assert set(values) == {f"val{i}" for i in range(5)}


@pytest.mark.asyncio
async def test_controller_create_file(tmp_path):
    """
    Test the file-based controller creation.
    The tmp_path fixture provides a temporary directory.
    """
    db_file = tmp_path / "test.db"
    controller = await AsyncSQLiteController.create_file(str(db_file))
    await controller.execute(
        "CREATE TABLE test_file (id INTEGER PRIMARY KEY, name TEXT);"
    )
    await controller.execute("INSERT INTO test_file (name) VALUES (?);", ("File Test",))
    await controller.commit()
    cursor = await controller.execute("SELECT name FROM test_file;")
    rows = await cursor.fetchall()
    assert rows[0][0] == "File Test"
    await controller.close()
