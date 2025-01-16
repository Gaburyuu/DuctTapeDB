from typing import TypeVar, Type, Optional
from pydantic import BaseModel
from .table import HookLoopTable
from typing import Any
import json

T = TypeVar("T", bound="HookLoopModel")


class HookLoopModel(BaseModel):
    id: Optional[int] = None
    _table: Optional[HookLoopTable] = None

    @classmethod
    def set_table(cls, table: HookLoopTable):
        cls._table = table

    @classmethod
    async def from_id(cls: Type[T], doc_id: int) -> T:
        if not cls._table:
            raise ValueError("No table is set for this model.")
        document = await cls._table.find(doc_id)
        if not document:
            raise ValueError(f"Document with id={doc_id} not found.")
        data = {"id": document["id"], **document["data"]}
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

        # Combine `id` with additional conditions
        conditions = {"id": doc_id, **(conditions or {})}

        # Use search for database-side filtering
        results = await cls._table.search(conditions)
        if not results:
            raise ValueError(
                f"No document found with id={doc_id} and conditions={conditions}"
            )

        # Use the first result (id should be unique)
        document = results[0]
        data = {"id": document["id"], **document["data"]}
        return cls.model_validate(data)

    @classmethod
    async def from_db_row(cls: Type[T], data: dict[str, Any]) -> T:
        """Create an object from a database row.

        Args:
            data (dict[str, Any]): A dictionary representing the database row.
                Expected format:
                - `id` (int): The primary key of the row.
                - `data` (dict): The JSON data associated with the row.

        Returns:
            T: An instance of the class created from the database row.
        """
        return cls.model_validate(data.get("data"))

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

        Args:
            limit (Optional[int]): The maximum number of rows to return. Defaults to None (no limit).
            offset (Optional[int]): The number of rows to skip before starting to return rows. Defaults to None (no offset).
            order_by (str): The column or SQL expression to order by (e.g., 'id ASC', 'json_extract(data, "$.key") DESC').
            filter_sql (Optional[str]): A raw SQL filter condition to apply to the query.
            filter_params (Optional[list[Any]]): Parameters for the SQL filter condition.

        Returns:
            list[T]: A list of model instances corresponding to the retrieved rows.

        Raises:
            ValueError: If no table is set for this model.
        """
        if not cls._table:
            raise ValueError("No table is set for this model.")

        # Base query
        query = f"SELECT id, data FROM {cls._table.table_name}"
        params = filter_params or []

        # Add filtering if provided
        if filter_sql:
            query += " WHERE " + filter_sql

        # Add ordering
        query += " ORDER BY " + order_by

        # Add limit and offset
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        if offset is not None:
            query += " OFFSET ?"
            params.append(offset)

        # Execute the parameterized query
        async with cls._table.controller._semaphore:  # Ensure concurrency control
            cursor = await cls._table.controller.execute(query, params)
            rows = [
                {"id": row[0], "data": json.loads(row[1])}
                for row in await cursor.fetchall()
            ]

        # Convert rows into model instances
        return [await cls.from_db_row(row) for row in rows]

    async def save(self) -> int:
        if not self._table:
            raise ValueError("No table is set for this model.")
        data = self.model_dump(exclude={"id"})
        self.id = await self._table.upsert({"id": self.id, "data": data})
        return self.id

    async def refresh(self) -> None:
        """
        Refresh the model instance with the current state from the database.

        Updates only the fields defined in the model, based on `self.model_fields_set`.

        Raises:
            ValueError: If the model has no ID or the table is not set.
            RuntimeError: If the document is not found in the database.
        """

        if not self._table:
            raise ValueError("No table is set for this model.")
        if not self.id:
            raise ValueError("Cannot refresh a model without an ID.")

        # Retrieve the document from the database
        document = await self._table.find(self.id)
        if not document:
            raise RuntimeError(f"Document with id={self.id} not found.")

        # Extract relevant fields based on model_fields_set
        updated_data = {
            field: document.get(field)
            for field in self.model_fields_set
            if field in document
        }

        # Unpack 'data' and merge with the rest of the fields
        updated_data.update(document.get("data", {}))

        # Update the model's fields
        for field, value in updated_data.items():
            setattr(self, field, value)

    @classmethod
    async def bulk_save(cls, models: list["HookLoopModel"]) -> list[int]:
        """
        Save multiple models at once using a single transaction, assigning IDs only to new rows.

        Args:
            models (list[HookLoopModel]): List of models to save.

        Returns:
            list[int]: A list of IDs for the saved models.
        """
        if not all(isinstance(model, cls) for model in models):
            raise ValueError(
                "All models must be instances of the calling class or its subclasses."
            )

        if not cls._table:
            raise ValueError("No table is set for this model.")

        query = f"""
            INSERT INTO {cls._table.table_name} (id, data)
            VALUES (?, json(?))
            ON CONFLICT (id) DO UPDATE SET
            data = json(?)
        """
        params = []
        new_models = []

        for model in models:
            json_data = model.model_dump_json(exclude={"id"})
            if model.id is None:
                params.append((None, json_data, json_data))
                new_models.append(model)
            else:
                params.append((model.id, json_data, json_data))

        conn = cls._table.connection
        async with conn.execute("BEGIN TRANSACTION"):
            await conn.executemany(query, params)

            # Assign IDs to new models
            if new_models:
                result = await conn.execute(
                    f"SELECT id FROM {cls._table.table_name} ORDER BY id DESC LIMIT ?",
                    (len(new_models),),
                )
                new_ids = [row[0] for row in await result.fetchall()]
                for model, new_id in zip(new_models, reversed(new_ids)):
                    model.id = new_id

        await conn.commit()

        return [model.id for model in models]

    async def delete(self):
        """Delete the model instance."""
        if not self._table:
            raise ValueError("No table is set for this model.")
        if self.id is None:
            raise ValueError("Cannot delete an object without an ID.")

        await self._table.delete_document(self.id)
