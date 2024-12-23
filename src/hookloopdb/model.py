from typing import TypeVar, Type, Optional
from pydantic import BaseModel
from .table import HookLoopTable
from typing import Any
import json
from aiosqlite import Connection as Aioconnection

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

    async def save(self) -> int:
        if not self._table:
            raise ValueError("No table is set for this model.")
        data = self.model_dump(exclude={"id"})
        self.id = await self._table.upsert({"id": self.id, "data": data})
        return self.id

    @classmethod
    async def bulk_save(cls, models: list["HookLoopModel"]) -> list[int]:
        """Save multiple models at once, assigning IDs only to new rows."""
        if not cls._table:
            raise ValueError("No table is set for this model.")

        # Prepare data for batch insert/update
        query = f"""
            INSERT INTO {cls._table.table_name} (id, data)
            VALUES (?, json(?))
            ON CONFLICT (id) DO UPDATE SET
            data = json(?)
        """
        params = []
        new_models = []  # Track models without IDs for later assignment
        for model in models:
            data = model.model_dump_json(exclude={"id"})

            if model.id is None:
                # New row: id will be auto-generated
                params.append((None, data, data))
                new_models.append(model)
            else:
                # Existing row: keep the provided ID
                params.append((model.id, data, data))

        # Begin transaction
        with cls._table.connection as conn:
            # Get the current max ID before inserting
            current_max_id = conn.execute(
                f"SELECT COALESCE(MAX(id), 0) FROM {cls._table.table_name}"
            ).fetchone()[0]

            # Perform the bulk operation
            conn.executemany(query, params)

            # Get the new max ID after inserting
            new_max_id = conn.execute(
                f"SELECT MAX(id) FROM {cls._table.table_name}"
            ).fetchone()[0]

        # Assign IDs to newly inserted models
        if new_models:
            assert len(new_models) == (
                new_max_id - current_max_id
            ), "Mismatch in expected ID assignments."
            for model, new_id in zip(
                new_models, range(current_max_id + 1, new_max_id + 1)
            ):
                model.id = new_id

        # Return all IDs
        return [model.id for model in models]
