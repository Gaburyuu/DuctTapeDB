import sqlite3
import json
from threading import local
from typing import Self, Any
from . import validators as v


class DuctTapeDB:
    """Initialize the DuctTapeDB instance.

    Args:
        path (str): Path to the SQLite database. Defaults to in-memory shared cache.
        table (str): Name of the table to use. Defaults to "documents".
        wal (bool): Whether to enable Write-Ahead Logging (WAL) mode. Defaults to True.
        auto_init (bool):
            Whether to automatically initialize the table on creation. Defaults to True.
            Will leave the created object in the connected state.
            In memory DBs will go awa
    """

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

    @classmethod
    def create(cls, table: str, path: str) -> Self:
        """Super basic factory"""
        return cls(path=path, table=table)

    @classmethod
    def create_memory(
        cls, table: str = "documents", shared_cache: bool = True, auto_init: bool = True
    ) -> Self:
        """Creates an obj with an in memory db"""
        if shared_cache:
            path = "file::memory:?cache=shared"
        else:
            path = ":memory:"
        # No WAL mode in memory dbs
        return cls(path=path, table=table, wal=False, auto_init=auto_init)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def connect(self) -> sqlite3.Connection:
        """Establish a thread-local connection to the SQLite database.

        Returns:
            sqlite3.Connection: SQLite connection object.

        Raises:
            RuntimeError: If connection initialization fails.
        """
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
        """Close the thread-local database connection."""
        if hasattr(self._local, "connection") and self._local.connection is not None:
            self._local.connection.close()
            self._local.connection = None

    @property
    def conn(self):
        return self.connect()

    def _initialize_table(self, indexes: list[str] = None):
        """Initialize the database table with optional JSON indexes."""
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data JSON NOT NULL
            )
        """
        self.conn.execute(query)

        # Create indexes
        indexes = indexes or []
        for index in indexes:
            make_index = f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table}_{index}
                ON {self.table} (json_extract(data, '$.{index}'))
            """
            self.conn.execute(make_index)
        self.conn.commit()

    def insert(self, document: dict[Any, Any]) -> int:
        return self.upsert_document(document)

    def upsert_document(self, document: dict[Any, Any]) -> int:
        """Insert a document or update it if it already exists.

        Args:
            document (dict[Any, Any]):
                The document to insert or update. If the document does not have an "id",
                the entire document is stored. If the document has an "id", only the "data"
                field is stored.

        Returns:
            int: The ID of the inserted or updated document.

        Raises:
            RuntimeError: If the operation fails.
        """
        v.validate_document(document)

        id_value = document.get("id")

        if id_value is None:
            # Serialize the entire document when no ID is provided
            json_data = json.dumps(document)
            query = f"""
                INSERT INTO {self.table} (data)
                VALUES (json(?))
            """
            params = (json_data,)
        else:
            # Serialize only the "data" field when an ID is provided
            if "data" not in document:
                raise ValueError(
                    "Documents with an 'id' must also include a 'data' field."
                )

            json_data = json.dumps(document["data"])
            query = f"""
                INSERT INTO {self.table} (id, data)
                VALUES (?, json(?))
                ON CONFLICT (id)
                DO UPDATE SET
                    data = json(?)
            """
            params = (id_value, json_data, json_data)

        try:
            cursor = self.conn.execute(query, params)
            self.conn.commit()
            return cursor.lastrowid if id_value is None else id_value
        except sqlite3.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Error during upsert of document {id_value}") from e

    def delete_document(self, id: int):
        """Delete a document by its unique ID.

        Args:
            id (int): Unique identifier of the document to delete.

        Returns:
            None
        """
        v.validate_id(id)
        query = f"DELETE FROM {self.table} WHERE id = ?"
        self.conn.execute(query, (id,))
        self.conn.commit()

    def find(self, id: int) -> dict | None:
        """Retrieve a document by its unique ID.

        Args:
            id (int): Unique identifier of the document.

        Returns:
            dict | None: The document as a dictionary if found, or None if not found.
        """
        v.validate_id(id)
        query = f"""
            SELECT id, data
            FROM {self.table}
            WHERE id = ?
        """
        cursor = self.conn.execute(query, (id,))
        row = cursor.fetchone()
        if row:
            return {"id": row[0], "data": json.loads(row[1])}
        return None

    def search(self, key: str, value: Any) -> list[dict]:
        """Search for documents by a JSON key-value pair.

        Args:
            key (str): The JSON key to search for.
            value (Any): The value to match against the JSON key.

        Returns:
            list[dict]: A list of matching documents as dictionaries.
        """
        v.validate_key_value(key, value)
        query = f"""
            SELECT id, data
            FROM {self.table}
            WHERE json_extract(data, '$.' || ?) = ?
        """
        cursor = self.conn.execute(query, (key, value))
        results = [
            {"id": row[0], "data": json.loads(row[1])} for row in cursor.fetchall()
        ]
        return results

    def aggregate(
        self,
        operation: str,
        json_key: str,
        where_values: list[dict] = None,
        where_raw: str = None,
    ) -> Any:
        """Perform an aggregate operation on a specific JSON key with optional WHERE clauses.

        Args:
            operation (str): SQL aggregate function (e.g., 'COUNT', 'SUM', 'AVG').
            json_key (str): JSON key to aggregate (e.g., 'age', 'price').
            where_values (list[dict], optional): List of conditions to apply,
                e.g., [{"field": "age", "sign": ">", "value": 30}]. Defaults to None.
            where_raw (str, optional): Raw SQL WHERE clause string,
                e.g., "json_extract(data, '$.age') > 30". Defaults to None.

        Returns:
            Any: Result of the aggregate operation, or None if no result.

        Raises:
            ValueError: If operation or json_key is invalid.
            RuntimeError: If a database error occurs.

        Notes:
            Always prefer `where_values` over `where_raw` to ensure query safety.
        """
        valid_operations = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
        if operation.upper() not in valid_operations:
            raise ValueError(f"Invalid SQL aggregate function: {operation}")

        if not json_key:
            raise ValueError("JSON key cannot be empty")

        query = f"SELECT {operation}(json_extract(data, '$.' || ?)) FROM {self.table}"
        params = [json_key]

        if where_raw and where_values:
            raise ValueError("Specify either 'where_values' or 'where_raw', not both.")

        try:
            if where_raw:
                query += " WHERE " + where_raw
            elif where_values:
                conditions = [
                    "json_extract(data, '$.' || ?) {} ?".format(cond["sign"])
                    for cond in where_values
                ]
                query += " WHERE " + " AND ".join(conditions)
                params += [cond["field"] for cond in where_values] + [
                    cond["value"] for cond in where_values
                ]

            cursor = self.conn.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else None

        except Exception as e:
            raise RuntimeError(f"Database error during aggregate operation: {e}")
