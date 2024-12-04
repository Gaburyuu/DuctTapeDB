from typing import TypeVar, Type, Optional, Self, Any
from pydantic import BaseModel, ValidationError
from .ducttapedb import DuctTapeDB

T = TypeVar("T", bound="DuctTapeModel")


class DuctTapeModel(BaseModel):
    id: Optional[int] = None

    @classmethod
    def from_id(cls: Type[T], db: DuctTapeDB, doc_id: int) -> T:
        """
        Create a model instance by looking up a database record by ID.

        Args:
            cls (Type[T]): The model class to instantiate.
            db (DuctTapeDB): The database instance to query.
            doc_id (int): The unique ID of the record in the database.

        Returns:
            T: An instance of the calling class.

        Raises:
            ValueError: If the document is not found or fails validation.
        """
        document = db.find(doc_id)

        if not document:
            raise ValueError(f"Document with id={doc_id} not found in the database.")

        try:
            # Validate and return the instance
            # first flatten it
            data = {"id": document["id"], **document["data"]}
            return cls.model_validate(data)
        except ValidationError as e:
            raise ValueError(f"Failed to validate data from the database: {e}")
