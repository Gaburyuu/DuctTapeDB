from typing import TypeVar, Type, Optional
from .table import SafetyTapeTable
from typing import Any
import json
from ..hookloopdb.model import HookLoopModel

T = TypeVar("T", bound="SafetyTapeModel")


class SafetyTapeModel(HookLoopModel):
    id: Optional[int] = None
    version: Optional[int] = None  # Add version field
    _table: Optional[SafetyTapeTable] = None

    @classmethod
    def set_table(cls, table: SafetyTapeTable):
        cls._table = table

    @classmethod
    async def from_id(cls: Type[T], doc_id: int) -> T:
        if not cls._table:
            raise ValueError("No table is set for this model.")
        document = await cls._table.find(doc_id)
        if not document:
            raise ValueError(f"Document with id={doc_id} not found.")
        data = {
            "id": document["id"],
            "version": document["version"],  # Include version
            **document["data"],
        }
        return cls.model_validate(data)

    @classmethod
    async def from_id_and(
        cls: Type[T], doc_id: int, conditions: dict[str, Any] = None
    ) -> T:
        """
        Retrieve a document by ID, ensuring it meets additional optional conditions.

        Args:
            doc_id (int): The unique ID of the record in the table.
            conditions (dict[str, Any], optional): Additional JSON key-value conditions to match.
                - Keys represent JSON fields within the document.
                - Values represent the required values for those fields.
                - If no conditions are provided, only the ID is used for matching.

        Returns:
            T: An instance of the model if the document is found and conditions are satisfied.

        Raises:
            ValueError: If:
                - No table is set for the model.
                - No document exists with the given ID.
                - The document does not meet the specified conditions.

        Example Usage:
            # Retrieve a document by ID with additional conditions
            model_instance = await HookLoopModel.from_id_and(
                doc_id=42,
                conditions={"status": "active", "role": "admin"}
            )
            print(model_instance)
        """
        if not cls._table:
            raise ValueError("No table is set for this model.")

        conditions = {"id": doc_id, **(conditions or {})}
        results = await cls._table.search(conditions)
        if not results:
            raise ValueError(
                f"No document found with id={doc_id} and conditions={conditions}"
            )

        document = results[0]
        return await cls.from_db_row(document)

    @classmethod
    async def from_db_row(cls: Type[T], data: dict[str, Any]) -> T:
        return cls.model_validate(
            {"id": data["id"], "version": data["version"], **data["data"]}
        )

    @classmethod
    async def models_from_db(
        cls: Type[T],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: str = "id ASC",
        filter_sql: Optional[str] = None,
        filter_params: Optional[list[Any]] = None,
    ) -> list[T]:
        """
        Retrieve rows from the database table and convert them into model instances.

        Excludes soft-deleted records (`deleted_at IS NOT NULL`) by default. Allows
        filtering, sorting, and paginating results.

        Args:
            limit (Optional[int]): The maximum number of rows to return. Defaults to None (no limit).
            offset (Optional[int]): The number of rows to skip before starting to return rows. Defaults to None (no offset).
            order_by (str): The column or SQL expression to order by. Defaults to "id ASC".
                Can include JSON expressions like `json_extract(data, '$.key')`. Use caution to avoid
                introducing SQL injection vulnerabilities.
            filter_sql (Optional[str]): A raw SQL filter condition to apply to the query. This
                string is appended to the `WHERE` clause. Use with caution and avoid accepting
                user input directly, as it may introduce SQL injection vulnerabilities.
            filter_params (Optional[list[Any]]): Parameters for the SQL filter condition, passed as a list.

        Returns:
            list[T]: A list of model instances corresponding to the retrieved rows.

        Raises:
            ValueError: If no table is set for this model.

        Warning:
            Both `filter_sql` and `order_by` can introduce SQL injection vulnerabilities if they include
            unsanitized user input. Always validate or sanitize these inputs.

        Examples:
            # Retrieve models with a custom filter for non-deleted records
            models = await YourModel.models_from_db(
                filter_sql="json_extract(data, '$.status') = ?",
                filter_params=["active"],
                limit=10
            )

            # Retrieve models ordered by a JSON field
            models = await YourModel.models_from_db(
                order_by="json_extract(data, '$.priority') DESC",
                limit=5
            )
        """
        if not cls._table:
            raise ValueError("No table is set for this model.")

        # Build base query
        query = f"SELECT id, version, data FROM {cls._table.table_name} WHERE deleted_at IS NULL"
        params = filter_params or []

        # Add additional filters
        if filter_sql:
            query += f" AND ({filter_sql})"

        # Add ordering and pagination
        query += f" ORDER BY {order_by}"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        if offset is not None:
            query += " OFFSET ?"
            params.append(offset)

        # Execute the query
        async with cls._table.controller._semaphore:
            cursor = await cls._table.controller.execute(query, params)
            rows = [
                {"id": row[0], "version": row[1], "data": json.loads(row[2])}
                for row in await cursor.fetchall()
            ]

        # Convert rows to model instances
        return [await cls.from_db_row(row) for row in rows]

    async def save(self) -> int:
        if not self._table:
            raise ValueError("No table is set for this model.")
        data = self.model_dump(exclude={"id", "version"})
        self.id, self.version = await self._table.upsert(
            {"id": self.id, "version": self.version, "data": data}
        )
        return self.id

    @classmethod
    async def bulk_save(cls, models: list["SafetyTapeModel"]) -> None:
        """
        Save multiple models at once.

        Note:
            This method enforces optimistic locking and ensures all models are
            synchronized with the database by calling `.save()` followed by `.refresh()`.
            Perhaps this will eventually be upgraded to use executemany, but for
            now it's just a convenience wrapper inside of SafetyTapeModel.
            HookLoopModel has a faster method with executemany.

        Args:
            models (list[SafetyTapeModel]): List of models to save.
        """
        if not all(isinstance(model, cls) for model in models):
            raise ValueError(
                "All models must be instances of the calling class or its subclasses."
            )

        for model in models:
            await model.save()
            await model.refresh()

    async def soft_delete(self) -> None:
        """
        Mark the model instance as deleted by setting `deleted_at`.

        Raises:
            ValueError: If the model has no ID or the table is not set.
        """
        if not self._table:
            raise ValueError("No table is set for this model.")
        if not self.id:
            raise ValueError("Cannot delete a model without an ID.")

        query = f"""
            UPDATE {self._table.table_name}
            SET deleted_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """
        await self._table.controller.execute(query, (self.id,))
        await self._table.controller.commit()

    async def restore(self) -> None:
        """
        Restore a soft-deleted model instance.

        Raises:
            ValueError: If the model has no ID or the table is not set.
        """
        if not self._table:
            raise ValueError("No table is set for this model.")
        if not self.id:
            raise ValueError("Cannot restore a model without an ID.")

        query = f"""
            UPDATE {self._table.table_name}
            SET deleted_at = NULL
            WHERE id = ?
        """
        await self._table.controller.execute(query, (self.id,))
        await self._table.controller.commit()

    @classmethod
    async def restore_from_id(cls, doc_id: int) -> None:
        """
        Restore a soft-deleted record by its ID.

        Args:
            doc_id (int): The ID of the record to restore.

        Raises:
            ValueError: If no table is set for the model.
            RuntimeError: If the document is not found in the database.
        """
        if not cls._table:
            raise ValueError("No table is set for this model.")

        query = f"""
            UPDATE {cls._table.table_name}
            SET deleted_at = NULL
            WHERE id = ?
        """
        cursor = await cls._table.controller.execute(query, (doc_id,))
        await cls._table.controller.commit()

        if cursor.rowcount == 0:
            raise RuntimeError(
                f"Document with id={doc_id} not found or is not soft-deleted."
            )
