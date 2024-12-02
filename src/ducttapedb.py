import sqlite3
import json
from threading import local


class DuctTapeDB:
    """ezpz json store/retriever"""

    # apparently I can have each thread have it's own connection
    # am i using threading correctly i wonder
    _local = local()

    def __init__(
        self,
        path: str = "file::memory:?cache=shared",
        table: str = "main",
        wal: bool = True,
    ):
        self.path = path
        self.table = table
        self.wal = wal
        self.connect()
        self._initialize_table()
        self.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def connect(self) -> sqlite3.Connection:
        """creates a brand new connection"""
        if not hasattr(self._local, "connection") or self._local.connection is None:
            self._local.connection = sqlite3.connect(
                self.path, uri=True, check_same_thread=False
            )
            self._local.connection.execute("PRAGMA foreign_keys = ON;")
            if self.wal:
                self._local.connection.execute(
                    "PRAGMA journal_mode = WAL;"
                )  # Enable WAL mode
            self._local.connection.execute(
                "PRAGMA busy_timeout = 5000;"
            )  # Wait up to 5 seconds for locks

        return self._local.connection

    def close(self):
        if hasattr(self._local, "connection") and self._local.connection is not None:
            self._local.connection.close()
            self._local.connection = None

    @property
    def conn(self):
        return self.connect()

    def _initialize_table(self):
        # TODO custom id name?
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
