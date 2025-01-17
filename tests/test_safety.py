import pytest
import pytest_asyncio
import asyncio
from src import (
    SafetyTapeTable,
    SafetyTapeModel,
)
from src.ducttapedb.hookloopdb.controller import AsyncSQLiteController
from typing import Optional


@pytest_asyncio.fixture(scope="module")
async def setup_controller():
    """Fixture to initialize HookLoopTable."""
    controller = await AsyncSQLiteController.create_memory(shared_cache=True)
    yield controller
    await controller.close()


class Monster(SafetyTapeModel):
    name: str
    level: int
    attack: Optional[int] = 10


@pytest_asyncio.fixture
async def setup_table(setup_controller):
    """Fixture to set up a SafetyTapeTable and the Monster model."""
    table_name = "monster_table"
    table = SafetyTapeTable(setup_controller, table_name)
    await table.initialize()
    Monster.set_table(table)
    yield table


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
    doc_id, version = await safety_tape_table.upsert(document)

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
    doc_id, version = await safety_tape_table.upsert(document)

    # Update the document with the correct version
    updated_document = {"id": doc_id, "data": {"key": "new_value"}, "version": 0}
    updated_id, version = await safety_tape_table.upsert(updated_document)

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
    doc_id, version = await safety_tape_table.upsert(document)

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
    doc_id, version = await safety_tape_table.upsert(document)

    # Define two update tasks with the same version
    async def update_task_1():
        updated_document = {
            "id": doc_id,
            "data": {"key": "new_value_1"},
            "version": version,
        }
        return await safety_tape_table.upsert(updated_document)

    async def update_task_2():
        updated_document = {
            "id": doc_id,
            "data": {"key": "new_value_2"},
            "version": version,
        }
        return await safety_tape_table.upsert(updated_document)

    # Run updates concurrently and catch version mismatches
    task_1 = asyncio.create_task(update_task_1())
    task_2 = asyncio.create_task(update_task_2())
    completed, pending = await asyncio.wait(
        [task_1, task_2], return_when=asyncio.ALL_COMPLETED
    )

    # Log results
    for task in completed:
        if task.exception():
            print(f"Task failed: {task.exception()}")
        else:
            print(f"Task succeeded: {task.result()}")

    # Only one task should succeed
    successful_updates = [t.result() for t in completed if not t.exception()]
    assert len(successful_updates) == 1

    # Verify the final state of the document
    final_document = await safety_tape_table.find(doc_id)
    assert final_document["version"] == version + 1
    assert final_document["data"] in [{"key": "new_value_1"}, {"key": "new_value_2"}]


@pytest.mark.asyncio
async def test_upsert_insert_returns_id_and_version(setup_table):
    """Test that upsert returns the correct ID and version for inserts."""
    table = setup_table
    document = {"data": {"name": "Dracky", "level": 5}}
    id_value, version = await table.upsert(document)

    assert id_value is not None
    assert version == 0


@pytest.mark.asyncio
async def test_upsert_update_returns_id_and_version(setup_table):
    """Test that upsert returns the correct ID and version for updates."""
    table = setup_table
    document = {"data": {"name": "Onion Slime", "level": 10}}
    id_value, version = await table.upsert(document)

    # Update the document
    updated_document = {
        "id": id_value,
        "version": version,
        "data": {"name": "She Slime", "level": 8},
    }
    updated_id, updated_version = await table.upsert(updated_document)

    assert updated_id == id_value
    assert updated_version == version + 1


@pytest.mark.asyncio
async def test_model_insert_and_update(setup_table):
    """Test inserting and updating Slime models with optimistic locking."""
    # Insert a new Slime
    slime = Monster(name="Slime", level=2)
    await slime.save()
    assert slime.id is not None
    assert slime.version == 0

    # Update the slime
    slime.level += 1  # level up!
    await slime.save()
    assert slime.version == 1

    # Attempt to update with incorrect version
    slime.version = 99
    with pytest.raises(RuntimeError, match="Version mismatch detected"):
        await slime.save()


@pytest.mark.asyncio
async def test_slime_concurrent_updates(setup_table):
    """Test concurrent updates to Monster models using optimistic locking."""
    # Insert a Slime
    slime = Monster(name="Metal Slime", level=11)
    await slime.save()

    # Define concurrent update tasks
    async def update_task_1():
        slime.level = 12
        slime.attack = 11
        await slime.save()

    async def update_task_2():
        slime.level = 14
        slime.attack = 15
        await slime.save()

    # Run concurrent updates
    task_1 = asyncio.create_task(update_task_1())
    task_2 = asyncio.create_task(update_task_2())
    completed, pending = await asyncio.wait(
        [task_1, task_2], return_when=asyncio.ALL_COMPLETED
    )

    # Verify one succeeded and the other failed
    assert sum(1 for t in completed if t.exception() is None) == 1
    assert sum(1 for t in completed if isinstance(t.exception(), RuntimeError)) == 1

    # Verify final Slime state
    final_slime = await Monster.from_id(slime.id)
    assert final_slime.version == 1
    assert final_slime.level in [12, 14]


@pytest.mark.asyncio
async def test_model_refresh(setup_table):
    """Test refreshing a model instance with database data."""
    # Insert a new Monster into the database
    monster = Monster(name="Dragon", level=20)
    await monster.save()

    # look up the monster
    monster = await Monster.from_id(monster.id)
    # Update the database directly
    table = setup_table
    await table.upsert(
        {
            "id": monster.id,
            "version": monster.version,
            "data": {"name": "Updated Dragon", "level": 25},
        }
    )

    # Refresh the model
    await monster.refresh()

    # Verify the model is updated
    assert monster.name == "Updated Dragon"
    assert monster.level == 25


@pytest.mark.asyncio
async def test_monster_bulk_save(setup_table):
    """Test bulk saving Monster models without updating model instances."""
    # Create new Monster models
    monsters: list[Monster] = [
        Monster(name="Slime", level=2),
        Monster(name="Dragon", level=20),
        Monster(name="Slime Knight", level=17),
    ]

    # Perform bulk save
    await Monster.bulk_save(monsters)

    # refresh the models and check their
    for monster in monsters:
        await monster.refresh()
        assert monster.id is not None  # Models remain unsynchronized
        assert monster.version is not None  # Models are updated


@pytest.mark.asyncio
async def test_updated_at_tracking(setup_table):
    """Test that the `updated_at` field is updated correctly on changes."""
    # Insert a new Monster
    monster = Monster(name="Dragon", level=10)
    await monster.save()

    # Fetch the record directly from the database to check timestamps
    table = setup_table
    query = f"SELECT created_at, updated_at FROM {table.table_name} WHERE id = ?"
    cursor = await table.controller.execute(query, (monster.id,))
    created_at, updated_at = await cursor.fetchone()

    assert created_at is not None
    assert updated_at is not None
    assert created_at == updated_at  # On initial insert, created_at == updated_at

    # Update the Monster
    await asyncio.sleep(1)
    monster.level = 15
    await monster.save()

    # Fetch the record again to verify updated_at has changed
    cursor = await table.controller.execute(query, (monster.id,))
    new_created_at, new_updated_at = await cursor.fetchone()

    assert new_created_at == created_at  # created_at should remain unchanged
    assert new_updated_at != updated_at  # updated_at should be updated


@pytest.mark.asyncio
async def test_model_soft_delete(setup_table):
    """Test soft-deleting a model instance."""
    # Insert a new Monster
    monster = Monster(name="Slime", level=5)
    await monster.save()

    # Soft-delete the Monster
    await monster.soft_delete()

    # Verify the Monster is excluded from regular queries
    models = await Monster.models_from_db()
    assert monster.id not in [m.id for m in models]

    # Verify the Monster still exists in the database (soft-deleted)
    table = setup_table
    query = f"SELECT deleted_at FROM {table.table_name} WHERE id = ?"
    cursor = await table.controller.execute(query, (monster.id,))
    deleted_at = await cursor.fetchone()
    assert deleted_at is not None  # Soft delete should set `deleted_at` timestamp


@pytest.mark.asyncio
async def test_model_restore(setup_table):
    """Test restoring a soft-deleted model instance."""
    # Insert a new Monster and soft-delete it
    monster = Monster(name="Dragon", level=10)
    await monster.save()
    await monster.soft_delete()

    # Verify the Monster is excluded from regular queries
    models = await Monster.models_from_db()
    assert monster.id not in [m.id for m in models]

    # Restore the Monster
    await monster.restore()

    # Verify the Monster is included in regular queries again
    models = await Monster.models_from_db()
    assert monster.id in [m.id for m in models]

    # Verify `deleted_at` is cleared
    table = setup_table
    query = f"SELECT deleted_at FROM {table.table_name} WHERE id = ?"
    cursor = await table.controller.execute(query, (monster.id,))
    deleted_at = await cursor.fetchone()
    assert deleted_at[0] is None  # `deleted_at` should be cleared


@pytest.mark.asyncio
async def test_restore_from_id(setup_table):
    """Test restoring a soft-deleted record by ID."""
    # Insert a new Monster and soft-delete it
    monster = Monster(name="Goblin", level=7)
    await monster.save()
    await monster.soft_delete()

    # Verify the Monster is excluded from regular queries
    models = await Monster.models_from_db()
    assert monster.id not in [m.id for m in models]

    # Restore the Monster using the class method
    await Monster.restore_from_id(monster.id)

    # Verify the Monster is included in regular queries again
    models = await Monster.models_from_db()
    assert monster.id in [m.id for m in models]

    # Verify `deleted_at` is cleared
    table = setup_table
    query = f"SELECT deleted_at FROM {table.table_name} WHERE id = ?"
    cursor = await table.controller.execute(query, (monster.id,))
    deleted_at = await cursor.fetchone()
    assert deleted_at[0] is None  # `deleted_at` should be cleared


@pytest.mark.asyncio
async def test_models_from_db_with_soft_deletes(setup_table):
    """Test models_from_db excludes and includes soft-deleted records."""
    # Insert two Monsters with a unique level (99) to identify them in this test
    monster1 = Monster(name="Healslime", level=99)
    monster2 = Monster(name="Dragon", level=99)
    await monster1.save()
    await monster2.save()

    # Soft-delete one Monster
    await monster1.soft_delete()

    # Verify non-deleted records are returned by default
    models = await Monster.models_from_db(
        filter_sql="json_extract(data, '$.level') = ?", filter_params=[99]
    )
    assert len(models) == 1
    assert models[0].id == monster2.id
