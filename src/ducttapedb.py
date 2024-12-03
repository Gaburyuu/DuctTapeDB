import sqlite3
import json
from threading import local
from typing import Self, Any


class DuctTapeDB:
    """ezpz json store/retriever"""

    # apparently I can have each thread have it's own connection
    # am i using threading correctly i wonder
    _local = local()

    def __init__(
        self,
        path: str = "file::memory:?cache=shared",
        table: str = "documents",
        wal: bool = True,
        auto_init=True,
    ):
        self.path = path
        self.table = table
        self.wal = wal
        if auto_init:
            self.connect()
            self._initialize_table()
            self.close()

    @classmethod
    def create(cls, table: str, path: str) -> Self:
        """Super basic factory"""
        return cls(path=path, table=table)

    @classmethod
    def create_memory(cls, table: str = "documents", shared_cache: bool = True) -> Self:
        """Creates an obj with an in memory db"""
        if shared_cache:
            path = "file::memory:?cache=shared"
        else:
            path = ":memory:"
        # No WAL mode in memory dbs
        return cls(path=path, table=table, wal=False, auto_init=False)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def connect(self) -> sqlite3.Connection:
        """Creates a brand-new thread-local connection with optional WAL support."""
        if not hasattr(self._local, "connection") or self._local.connection is None:
            try:
                # Create connection
                self._local.connection = sqlite3.connect(
                    self.path, uri=True, check_same_thread=False
                )

                # Set SQLite PRAGMAs
                self._local.connection.execute("PRAGMA foreign_keys = ON;")
                self._local.connection.execute("PRAGMA busy_timeout = 5000;")
                if self.wal:
                    mode = self._local.connection.execute(
                        "PRAGMA journal_mode = WAL;"
                    ).fetchone()[0]
                    if mode != "wal":
                        raise RuntimeError(
                            f"Failed to enable WAL mode. Current mode: {mode}"
                        )

            except sqlite3.Error as e:
                raise RuntimeError(f"Failed to connect to the database: {e}")

        return self._local.connection

    def close(self):
        if hasattr(self._local, "connection") and self._local.connection is not None:
            self._local.connection.close()
            self._local.connection = None

    @property
    def conn(self):
        return self.connect()

    def _initialize_table(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data JSON NOT NULL
            )
        """
        self.conn.execute(query)

        # Create the index
        make_index = f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table}_name
            ON {self.table} (json_extract(data, '$.name'))
        """
        self.conn.execute(make_index)
        self.conn.commit()

    def upsert_document(self, document: dict[Any, Any]) -> int:
        """Insert a JSON document or update it if it already exists."""
        query = f"""
            INSERT INTO {self.table} (id, data)
            VALUES (?, json(?))
            ON CONFLICT (id)
            DO UPDATE SET
                data = json(?)
        """
        id_value = document.get("id")
        json_data = json.dumps(document)

        try:
            cursor = self.conn.execute(query, (id_value, json_data, json_data))
            self.conn.commit()
            return id_value or cursor.lastrowid
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"Error during upsert of document {id_value}") from e

    def delete_document(self, id: int):
        """Delete a document from the database by unique field value (id)"""
        query = f"DELETE FROM {self.table} WHERE id = ?"
        self.conn.execute(query, (id,))
        self.conn.commit()
