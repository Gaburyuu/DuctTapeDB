from ..hookloopdb import HookLoopTable
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                deleted_at TIMESTAMP NULL,
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

        self.columns = await self.get_non_data_columns()

    async def find(self, doc_id: int) -> dict | None:
        """
        Find a document by ID, excluding soft-deleted records by default.

        Args:
            doc_id (int): The ID of the document.

        Returns:
            dict | None: The document, or None if not found.
        """
        query = f"""
            SELECT id, version, data
            FROM {self.table_name}
            WHERE id = ? AND deleted_at IS NULL
        """
        cursor = await self.controller.execute(query, (doc_id,))
        result = await cursor.fetchone()
        if result:
            return {
                "id": result[0],
                "version": result[1],
                "data": json.loads(result[-1]),
            }
        return None

    async def upsert(self, document: dict[Any, Any]) -> tuple[int, int]:
        """
        Insert or update a document with optimistic locking.

        Args:
            document (dict[Any, Any]): The document to insert or update.
                - `id` (int): The unique identifier of the document.
                - `version` (int, optional): The current version of the document.

        Returns:
            tuple[int, int]: A tuple containing:
                - The ID of the inserted or updated document.
                - The version of the inserted or updated document.

        Raises:
            RuntimeError: If the update fails due to a version mismatch.
            ValueError: If `id` or `version` is missing for updates.
        """
        id_value = document.get("id")
        json_data = json.dumps(document.get("data", {}))
        version = document.get("version")

        if id_value is None:
            query = f"""
                INSERT INTO {self.table_name} (version, created_at, updated_at, data)
                VALUES (0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, json(?))
                RETURNING id, version
            """
            params = [
                json_data,
            ]
        else:
            if version is None:
                raise ValueError("Version must be provided for updates in SafetyTape.")
            query = f"""
                UPDATE {self.table_name}
                SET data = json(?), version = version + 1, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND version = ?
                RETURNING id, version
            """
            params = [json_data, id_value, version]

        async with self.controller._connection.execute(query, params) as cursor:
            result = await cursor.fetchone()
            if result is None:
                raise RuntimeError(
                    f"Update failed for id={id_value}. Version mismatch detected."
                )

        return result[0], result[1]
