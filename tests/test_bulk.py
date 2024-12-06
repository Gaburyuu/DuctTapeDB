import pytest
from src.ducttapedb import DuctTapeDB, DuctTapeModel


class Slime(DuctTapeModel):
    name: str
    level: int


@pytest.fixture
def slime_db():
    """Fixture to create an in-memory database for slimes."""
    db = DuctTapeDB.create_memory("slimes")
    Slime.set_db(db)
    return db


def test_bulk_save(slime_db):
    """Test bulk_save with multiple Slime instances."""
    slimes = [
        Slime(name="Slime 1", level=10),
        Slime(name="Slime 2", level=20),
        Slime(name="Slime 3", level=30),
    ]

    # Perform bulk save
    saved_ids = Slime.bulk_save(slimes)

    print(slimes)
    print(saved_ids)

    # Verify all IDs are set
    assert len(saved_ids) == len(slimes), "All slimes should have IDs after bulk_save."
    for slime, slime_id in zip(slimes, saved_ids):
        assert slime.id == slime_id, "Slime ID should match returned ID."

    # Verify data in the database
    for slime in slimes:
        retrieved = Slime.from_id(slime.id)
        assert retrieved.name == slime.name
        assert retrieved.level == slime.level
