from src.ducttapedb.hookloopdb import HookLoopTable
import json
from typing import Any


class SafetyTapeTable(HookLoopTable):

    async def initialize(self, indexes: list[str] = None):
        """
        Initialize the table, ensuring a `version` column is present.

        Args:
            indexes (list[str], optional): List of JSON keys to index. Defaults to None.
        """
        # Create the table with a version column
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version INTEGER DEFAULT 0,
                data JSON NOT NULL
            )
        """
        await self.controller.execute(create_table_query)

        # Ensure the `version` column exists (e.g., for legacy tables without it)
        check_version_column_query = f"""
            PRAGMA table_info({self.table_name})
        """
        cursor = await self.controller.execute(check_version_column_query)
        columns = [row[1] for row in await cursor.fetchall()]

        if "version" not in columns:
            add_version_column_query = f"""
                ALTER TABLE {self.table_name} ADD COLUMN version INTEGER DEFAULT 0
            """
            await self.controller.execute(add_version_column_query)

        # Add any additional indexes
        indexes = indexes or []
        for index in indexes:
            create_index_query = f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_{index}
                ON {self.table_name} (json_extract(data, '$.{index}'))
            """
            await self.controller.execute(create_index_query)

        await self.controller.commit()

    async def upsert(self, document: dict[Any, Any]) -> int:
        """
        Insert or update a document with optimistic locking.

        Args:
            document (dict[Any, Any]): The document to insert or update.
                - `id` (int): The unique identifier of the document.
                - `version` (int, optional): The current version of the document.

        Returns:
            int: The ID of the inserted or updated document.

        Raises:
            RuntimeError: If the update fails due to a version mismatch.
            ValueError: If `id` or `version` is missing for updates.
        """
        id_value = document.get("id")
        json_data = json.dumps(document.get("data", {}))
        version = document.get("version")

        if id_value is None:
            # Insert a new document with version 0
            query = f"INSERT INTO {self.table_name} (version, data) VALUES (0, json(?))"
            params = (json_data,)
        else:
            if version is None:
                raise ValueError("Version must be provided for updates in SafetyTape.")

            # Update only if the version matches
            query = f"""
                UPDATE {self.table_name}
                SET data = json(?), version = version + 1
                WHERE id = ? AND version = ?
            """
            params = (json_data, id_value, version)

        async with self.controller._connection.execute(query, params) as cursor:
            await self.controller.commit()
            if cursor.rowcount == 0:  # No rows affected, indicating a version mismatch
                raise RuntimeError(
                    f"Update failed for id={id_value}. Version mismatch detected."
                )
            return id_value or cursor.lastrowid

    async def find(self, doc_id: int) -> dict | None:
        """
        Retrieve a document by ID, including its version.

        Args:
            doc_id (int): The ID of the document to retrieve.

        Returns:
            dict | None: The document with its `id`, `version`, and `data` fields.
        """
        query = f"SELECT id, version, data FROM {self.table_name} WHERE id = ?"
        cursor = await self.controller.execute(query, (doc_id,))
        result = await cursor.fetchone()
        if result:
            return {
                "id": result[0],
                "version": result[1],
                "data": json.loads(result[2]),
            }
        return None
