import pytest
from src import DuctTapeDB, DuctTapeModel
from random import randint


class Slime(DuctTapeModel):
    name: str
    level: int


@pytest.fixture(scope="module")
def slime_db():
    """Fixture to create an in-memory database for slimes."""
    db = DuctTapeDB.create_memory("slimes")
    return db


@pytest.fixture(scope="module", autouse=True)
def slime_model(slime_db):
    """Set the DB for ExampleModel once for the module."""
    Slime.set_db(slime_db)


def test_bulk_save():
    """Test bulk_save with multiple Slime instances."""
    slimes = [
        Slime(name="Slime 1", level=10),
        Slime(name="Slime 2", level=20),
        Slime(name="Slime 3", level=30),
    ]

    # Perform bulk save
    saved_ids = Slime.bulk_save(slimes)

    # Verify all IDs are set
    assert len(saved_ids) == len(slimes), "All slimes should have IDs after bulk_save."
    for slime, slime_id in zip(slimes, saved_ids):
        assert slime.id == slime_id, "Slime ID should match returned ID."

    # Verify data in the database
    for slime in slimes:
        retrieved = Slime.from_id(slime.id)
        assert retrieved.name == slime.name
        assert retrieved.level == slime.level


def test_bulk_save_stress_with_transaction():
    """Test bulk_save with many models, mixing new and existing records."""

    # Is it wrong to do it this way or the slime way and declare it outside of the func
    class StressModel(DuctTapeModel):
        name: str
        value: int

    # Set up the database
    stress_db = DuctTapeDB.create_memory("stress_test")
    StressModel.set_db(stress_db)

    # Generate 1,000 models
    models = [StressModel(name=f"Item {i}", value=randint(1, 100)) for i in range(1000)]

    # Perform initial bulk save
    saved_ids = StressModel.bulk_save(models)

    # Verify all IDs are unique and assigned
    assert len(saved_ids) == len(models), "All models should have IDs after bulk_save."
    assert len(set(saved_ids)) == len(saved_ids), "IDs should be unique."

    # Modify and mix some existing models with new ones
    for i, model in enumerate(models):
        if i % 2 == 0:  # Modify existing models
            model.value += 10
        else:  # Create new models
            models[i] = StressModel(name=f"New Item {i}", value=randint(1, 100))

    # Perform bulk save again
    new_saved_ids = StressModel.bulk_save(models)

    # Verify all models are saved and IDs are consistent for updated ones
    assert len(new_saved_ids) == len(models), "All models should be saved."
    for i, model in enumerate(models):
        if i % 2 == 0:  # Existing models should keep their IDs
            assert (
                new_saved_ids[i] == saved_ids[i]
            ), f"Model {i} ID should remain unchanged."
        else:  # New models should have unique new IDs
            assert new_saved_ids[i] not in saved_ids, f"Model {i} should have a new ID."

    # Ensure data integrity
    for model in models:
        retrieved = StressModel.from_id(model.id)
        assert retrieved.name == model.name
        assert retrieved.value == model.value
