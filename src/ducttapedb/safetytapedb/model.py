from typing import TypeVar, Type, Optional
from pydantic import BaseModel
from .table import SafetyTapeTable
from typing import Any
import json

T = TypeVar("T", bound="SafetyTapeModel")


class SafetyTapeModel(BaseModel):
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
        return cls.model_validate({"id": data["id"], "version": data["version"], **data["data"]})

    @classmethod
    async def models_from_db(
        cls: Type[T],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: str = "id ASC",
        filter_sql: Optional[str] = None,
        filter_params: Optional[list[Any]] = None,
    ) -> list[T]:
        if not cls._table:
            raise ValueError("No table is set for this model.")

        query = f"SELECT id, version, data FROM {cls._table.table_name}"
        params = filter_params or []

        if filter_sql:
            query += " WHERE " + filter_sql
        query += " ORDER BY " + order_by
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        if offset is not None:
            query += " OFFSET ?"
            params.append(offset)

        async with cls._table.controller._semaphore:
            cursor = await cls._table.controller.execute(query, params)
            rows = [
                {"id": row[0], "version": row[1], "data": json.loads(row[2])}
                for row in await cursor.fetchall()
            ]

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
    async def bulk_save(cls, models: list["SafetyTapeModel"]) -> list[int]:
        """
        Save multiple models at once.

        Note:
            This method does NOT enforce optimistic locking. It performs inserts
            for new models and updates existing ones without version checks.

        Args:
            models (list[SafetyTapeModel]): List of models to save.

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
            INSERT INTO {cls._table.table_name} (id, version, data)
            VALUES (?, 0, json(?))
            ON CONFLICT (id) DO UPDATE SET
            version = version + 1,
            data = json(?)
        """
        params = [
            (
                model.id,
                model.model_dump_json(exclude={"id", "version"}),
                model.model_dump_json(exclude={"id", "version"}),
            )
            for model in models
        ]

        conn = cls._table.connection
        async with conn.execute("BEGIN TRANSACTION"):
            await conn.executemany(query, params)
            await conn.commit()

        # Assign IDs for new models
        new_models = [model for model in models if model.id is None]
        if new_models:
            result = await conn.execute(
                f"SELECT id FROM {cls._table.table_name} ORDER BY id DESC LIMIT ?",
                (len(new_models),),
            )
            new_ids = [row[0] for row in await result.fetchall()]
            for model, new_id in zip(new_models, reversed(new_ids)):
                model.id = new_id

        return [model.id for model in models]

