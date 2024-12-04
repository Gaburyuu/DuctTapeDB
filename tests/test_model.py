import pytest
from src import DuctTapeModel, DuctTapeDB


class ExampleModel(DuctTapeModel):
    name: str
    level: int


@pytest.fixture
def memory_db() -> DuctTapeDB:
    """Fixture to provide an in-memory DuctTapeDB instance."""
    db = DuctTapeDB.create_memory()
    return db


def test_from_id_success(memory_db):
    """Test loading a valid document by ID using an in-memory database."""
    with memory_db as db:
        db._initialize_table()

        # Insert a test document
        doc_id = db.upsert_document({"name": "Slime", "level": 5})
        print("id is", doc_id)

        # Retrieve the document using from_id
        instance = ExampleModel.from_id(db, doc_id)

        assert isinstance(instance, ExampleModel)
        assert instance.id == doc_id
        assert instance.name == "Slime"
        assert instance.level == 5


def test_from_id_not_found(memory_db):
    """Test loading a non-existent document by ID."""
    with memory_db as db:
        db._initialize_table()

        with pytest.raises(
            ValueError, match="Document with id=2 not found in the database."
        ):
            ExampleModel.from_id(db, 2)


def test_from_id_validation_error(memory_db):
    """Test loading a document that fails validation."""
    with memory_db as db:
        db._initialize_table()

        # Insert an invalid document (missing 'level')
        _id = db.upsert_document({"name": "Metal Slime"})

        with pytest.raises(
            ValueError, match="Failed to validate data from the database:"
        ):
            ExampleModel.from_id(db, _id)
