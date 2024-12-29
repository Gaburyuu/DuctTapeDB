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
        print("data", data)
        self.id = await self._table.upsert({"id": self.id, "data": data})
        return self.id

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
