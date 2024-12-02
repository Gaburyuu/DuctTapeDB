import pytest
from src.ducttapedb import DuctTapeDB


@pytest.fixture
def memory_db() -> DuctTapeDB:
    """in memory db"""
    return DuctTapeDB(table="main")


def test_db_initialized(memory_db: DuctTapeDB):
    """Test if the database is initialized and the table exists."""

    with memory_db as db:
        # Init in memory db every time
        db._initialize_table()

        # Check if the expected table exists
        query = """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name=?
        """
        cursor = db.conn.execute(query, (db.table,))
        result = cursor.fetchone()

        # Assert that the table exists
        assert result is not None, "Database table should exist after initialization."
        assert result[0] == db.table, f"Expected table '{db.table}', got '{result[0]}'"
