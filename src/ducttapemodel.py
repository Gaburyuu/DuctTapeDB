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

    def save(self, db: DuctTapeDB) -> int:
        """
        Save the model instance to the database.

        Args:
            db (DuctTapeDB): The database instance to save to.

        Returns:
            int: The ID of the saved document. If the instance is newly created,
                this will be the auto-generated ID.
        """

        # Prepare data for saving
        data = self.model_dump(exclude={"id"})

        if self.id is not None:
            # Update existing document
            document = {"id": self.id, "data": data}
            db.upsert_document(document)
        else:
            # Insert new document and update the instance's ID
            self.id = db.upsert_document(data)

        return self.id
