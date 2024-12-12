import pytest
from src import DuctTapeModel, DuctTapeDB


class ExampleModel(DuctTapeModel):
    name: str
    level: int


@pytest.fixture(scope="module")
def memory_db() -> DuctTapeDB:
    """Fixture to provide a module-wide in-memory DuctTapeDB instance."""
    db = DuctTapeDB.create_memory()
    db._initialize_table()
    return db


@pytest.fixture(scope="module", autouse=True)
def example_model_db(memory_db):
    """Set the DB for ExampleModel once for the module."""
    ExampleModel.set_db(memory_db)


def test_from_id_success(memory_db):
    """Test loading a valid document by ID using an in-memory database."""

    # Insert a test document
    doc_id = memory_db.upsert_document({"name": "Slime", "level": 5})

    # Retrieve the document using from_id
    instance = ExampleModel.from_id(doc_id)

    assert isinstance(instance, ExampleModel)
    assert instance.id == doc_id
    assert instance.name == "Slime"
    assert instance.level == 5


def test_from_id_not_found():
    """Test loading a non-existent document by ID."""
    bad_id = 99999
    with pytest.raises(
        ValueError, match=f"Document with id={bad_id} not found in the database."
    ):
        ExampleModel.from_id(bad_id)


def test_from_id_validation_error(memory_db):
    """Test loading a document that fails validation."""

    # Insert an invalid document (missing 'level')
    _id = memory_db.upsert_document({"name": "Metal Slime"})

    with pytest.raises(ValueError, match="Failed to validate data from the database:"):
        ExampleModel.from_id(_id)


def test_save_new_instance(memory_db):
    """Test saving a new instance generates an ID."""
    instance = ExampleModel(name="Slime", level=5)

    doc_id = instance.save()

    assert doc_id > 0  # ID should be auto-generated
    saved_doc = memory_db.find(doc_id)
    assert saved_doc["data"]["name"] == "Slime"


def test_save_existing_instance(memory_db):
    """Test saving an existing instance updates the database."""
    # Insert initial data
    initial_doc = {"id": 1, "data": {"name": "Slime", "level": 5}}
    memory_db.upsert_document(initial_doc)

    # Create model and modify it
    instance = ExampleModel(id=1, name="Slime", level=10)
    doc_id = instance.save()

    assert doc_id == 1  # ID should remain the same
    updated_doc = memory_db.find(1)
    assert updated_doc["data"]["level"] == 10
