import pytest
import pytest_asyncio
import asyncio
from src import (
    SafetyTapeTable,
)
from src.ducttapedb.hookloopdb.controller import AsyncSQLiteController
from typing import Optional



@pytest_asyncio.fixture(scope='module')
async def setup_controller():
    """Fixture to initialize HookLoopTable."""
    controller = await AsyncSQLiteController.create_memory(shared_cache=True)
    yield controller
    await controller.close()



@pytest.mark.asyncio
async def test_safetytapetable_initialization(setup_controller):
    """Test that SafetyTapeTable initializes correctly with the `version` column."""
    table_name = "safetytape_table"
    safety_tape_table = SafetyTapeTable(setup_controller, table_name)
    await safety_tape_table.initialize()

    # Verify that the `version` column exists
    query = f"PRAGMA table_info({table_name})"
    cursor = await setup_controller.execute(query)
    columns = [row[1] async for row in cursor]
    assert "version" in columns

    # Verify that other columns are present
    assert "id" in columns
    assert "data" in columns

@pytest.mark.asyncio
async def test_safetytapetable_insert(setup_controller):
    """Test inserting a new document into SafetyTapeTable."""
    table_name = "safetytape_table"
    safety_tape_table = SafetyTapeTable(setup_controller, table_name)
    await safety_tape_table.initialize()

    # Insert a new document
    document = {"data": {"key": "value"}}
    doc_id = await safety_tape_table.upsert(document)

    # Verify the inserted document
    result = await safety_tape_table.find(doc_id)
    assert result["id"] == doc_id
    assert result["version"] == 0  # Default version for new documents
    assert result["data"] == {"key": "value"}

@pytest.mark.asyncio
async def test_safetytapetable_update_correct_version(setup_controller):
    """Test updating a document with the correct version in SafetyTapeTable."""
    table_name = "safetytape_table"
    safety_tape_table = SafetyTapeTable(setup_controller, table_name)
    await safety_tape_table.initialize()

    # Insert a new document
    document = {"data": {"key": "value"}}
    doc_id = await safety_tape_table.upsert(document)

    # Update the document with the correct version
    updated_document = {"id": doc_id, "data": {"key": "new_value"}, "version": 0}
    updated_id = await safety_tape_table.upsert(updated_document)

    # Verify the updated document
    result = await safety_tape_table.find(updated_id)
    assert result["id"] == updated_id
    assert result["version"] == 1  # Version should be incremented
    assert result["data"] == {"key": "new_value"}

@pytest.mark.asyncio
async def test_safetytapetable_update_incorrect_version(setup_controller):
    """Test updating a document with an incorrect version in SafetyTapeTable."""
    table_name = "safetytape_table"
    safety_tape_table = SafetyTapeTable(setup_controller, table_name)
    await safety_tape_table.initialize()

    # Insert a new document
    document = {"data": {"key": "value"}}
    doc_id = await safety_tape_table.upsert(document)

    # Attempt to update the document with an incorrect version
    updated_document = {"id": doc_id, "data": {"key": "new_value"}, "version": 99}
    with pytest.raises(RuntimeError, match="Version mismatch detected"):
        await safety_tape_table.upsert(updated_document)

    # Verify the document remains unchanged
    result = await safety_tape_table.find(doc_id)
    assert result["version"] == 0  # Version should remain the same
    assert result["data"] == {"key": "value"}

@pytest.mark.asyncio
async def test_safetytapetable_concurrent_updates(setup_controller):
    """Test concurrent updates to the same document using SafetyTapeTable."""
    table_name = "safetytape_table"
    safety_tape_table = SafetyTapeTable(setup_controller, table_name)
    await safety_tape_table.initialize()

    # Insert a new document
    document = {"data": {"key": "value"}}
    doc_id = await safety_tape_table.upsert(document)

    # Define two update tasks with the same version
    async def update_task_1():
        updated_document = {"id": doc_id, "data": {"key": "new_value_1"}, "version": 0}
        return await safety_tape_table.upsert(updated_document)

    async def update_task_2():
        updated_document = {"id": doc_id, "data": {"key": "new_value_2"}, "version": 0}
        return await safety_tape_table.upsert(updated_document)

    # Run updates concurrently and catch version mismatches
    task_1 = asyncio.create_task(update_task_1())
    task_2 = asyncio.create_task(update_task_2())
    completed, pending = await asyncio.wait([task_1, task_2], return_when=asyncio.ALL_COMPLETED)

    # Only one task should succeed
    successful_updates = [t.result() for t in completed if not t.exception()]
    assert len(successful_updates) == 1

    # Verify the final state of the document
    result = await safety_tape_table.find(doc_id)
    assert result["version"] == 1  # Version should be incremented once
    assert result["data"] in [{"key": "new_value_1"}, {"key": "new_value_2"}]
