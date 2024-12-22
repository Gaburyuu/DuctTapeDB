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

    async def save(self) -> int:
        if not self._table:
            raise ValueError("No table is set for this model.")
        data = self.model_dump(exclude={"id"})
        self.id = await self._table.upsert({"id": self.id, "data": data})
        return self.id

    async def bulk_upsert(self, documents: list[dict[Any, Any]]):
        query = f"""
            INSERT INTO {self.table_name} (id, data)
            VALUES (?, json(?))
            ON CONFLICT (id) DO UPDATE SET data = json(?)
        """
        params = []
        for doc in documents:
            id_value = doc.get("id")
            json_data = json.dumps(doc.get("data", {}))
            params.append((id_value, json_data, json_data))

        async with self.controller.connection as conn:
            await conn.executemany(query, params)
            await conn.commit()
