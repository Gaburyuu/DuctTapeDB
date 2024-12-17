from typing import TypeVar, Type, Optional
from pydantic import BaseModel, ValidationError
from .table import AsyncDuctTapeTable

T = TypeVar("T", bound="AsyncDuctTapeModel")


class AsyncDuctTapeModel(BaseModel):
    id: Optional[int] = None
    _table: Optional[AsyncDuctTapeTable] = None

    @classmethod
    def set_table(cls, table: AsyncDuctTapeTable):
        cls._table = table

    @classmethod
    async def from_id(cls: Type[T], doc_id: int) -> T:
        if not cls._table:
            raise ValueError("No table is set for this model.")
        document = await cls._table.find(doc_id)
        if not document:
            raise ValueError(f"Document with id={doc_id} not found.")
        return cls(id=document["id"], **document["data"])

    async def save(self) -> int:
        if not self._table:
            raise ValueError("No table is set for this model.")
        data = self.model_dump(exclude={"id"})
        self.id = await self._table.upsert({"id": self.id, "data": data})
        return self.id
