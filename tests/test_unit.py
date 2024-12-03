import pytest
from src.ducttapedb import DuctTapeDB
import tempfile
import os


@pytest.fixture
def memory_db() -> DuctTapeDB:
    """in memory db"""
    return DuctTapeDB.create_memory()


def get_temp_db_path(prefix: str = "default"):
    """Create and return a temporary file-based database path."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, prefix=prefix, suffix=".db")
    return temp_file.name


@pytest.fixture
def file_db() -> DuctTapeDB:
    """Fixture to create a file-based NoSQLLiteDB instance."""
    db_path = get_temp_db_path()
    db = DuctTapeDB.create("main", db_path)

    yield db

    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)


def query_for_table(db: DuctTapeDB):
    """queries if a table exists and returns the result"""

    with db:
        db._initialize_table()
        # Check if the expected table exists
        query = """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name=?
        """
        cursor = db.conn.execute(query, (db.table,))
        result = cursor.fetchone()
        return result


def test_db_initialized(memory_db: DuctTapeDB):
    """Test if the database is initialized and the table exists."""

    result = query_for_table(memory_db)

    # Assert that the table exists
    assert result is not None, "Database table should exist after initialization."
    assert (
        result[0] == memory_db.table
    ), f"Expected table '{memory_db.table}', got '{result[0]}'"


def test_factory_methods():
    """Test if the database is initialized and the table exists after creating with factories"""
    memory_db = DuctTapeDB.create_memory()

    result = query_for_table(memory_db)

    # Assert that the table exists
    assert result is not None, "Database table should exist after initialization."
    assert (
        result[0] == memory_db.table
    ), f"Expected table '{memory_db.table}', got '{result[0]}'"


def test_file_db(file_db):
    """tests the creation of a file db, not memory"""
    result = query_for_table(file_db)

    # Assert that the table exists
    assert result is not None, "Database table should exist after initialization."
    assert (
        result[0] == file_db.table
    ), f"Expected table '{file_db.table}', got '{result[0]}'"


def test_invalid_db_path():
    """Test that invalid database paths raise an error."""
    with pytest.raises(RuntimeError, match="Failed to connect"):
        DuctTapeDB(path="/invalid/path/to/db.sqlite", table="main")
