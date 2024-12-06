from typing import TypeVar, Type, Optional, ClassVar
from pydantic import BaseModel, ValidationError
from .ducttapedb import DuctTapeDB
import json

T = TypeVar("T", bound="DuctTapeModel")


class DuctTapeModel(BaseModel):
    id: Optional[int] = None
    # shared db reference
    _db: ClassVar[Optional[DuctTapeDB]] = None

    @classmethod
    def set_db(cls, db: DuctTapeDB):
        """Set the shared database connection."""
        cls._db = db

    @classmethod
    def from_id(cls: Type[T], doc_id: int) -> T:
        """
        Create a model instance by looking up a database record by ID.

        Args:
            cls (Type[T]): The model class to instantiate.
            doc_id (int): The unique ID of the record in the database.

        Returns:
            T: An instance of the calling class.

        Raises:
            ValueError: If the document is not found or fails validation.
            ValueError: If no database connection is set.
        """
        if cls._db is None:
            raise ValueError("No database connection set.")

        document = cls._db.find(doc_id)

        if not document:
            raise ValueError(f"Document with id={doc_id} not found in the database.")

        try:
            # Validate and return the instance
            # first flatten it
            data = {"id": document["id"], **document["data"]}
            return cls.model_validate(data)
        except ValidationError as e:
            raise ValueError(f"Failed to validate data from the database: {e}")

    def save(self) -> int:
        """
        Save the model instance to the database.

        Args:
            db (DuctTapeDB): The database instance to save to.

        Returns:
            int: The ID of the saved document. If the instance is newly created,
                this will be the auto-generated ID.
        Raises:
            ValueError: If no database connection is set.
        """
        if self._db is None:
            raise ValueError("No database connection set.")

        # Prepare data for saving
        data = self.model_dump(exclude={"id"})

        if self.id is not None:
            # Update existing document
            document = {"id": self.id, "data": data}
            self._db.upsert_document(document)
        else:
            # Insert new document and update the instance's ID
            self.id = self._db.upsert_document(data)

        return self.id

    @classmethod
    def bulk_save(cls, models: list["DuctTapeModel"]) -> list[int]:
        """Save multiple models at once using a batch operation.

        Args:
            models (list[DuctTapeModel]): List of model instances to save.

        Returns:
            list[int]: A list of IDs for the saved models.

        Raises:
            ValueError: If no database connection is set.
        """
        if cls._db is None:
            raise ValueError("No database connection set.")

        # Prepare data for batch insert/update
        query = f"""
            INSERT INTO {cls._db.table} (id, data)
            VALUES (?, json(?))
            ON CONFLICT (id) DO UPDATE SET
            data = json(?)
        """
        params = []
        for model in models:
            data = model.model_dump(exclude={"id"})
            json_data = json.dumps(data)
            params.append((model.id, json_data, json_data))

        # Execute as a batch
        cls._db.conn.executemany(query, params)
        cls._db.conn.commit()

        # Update model IDs and return them
        saved_ids = []
        for model in models:
            if model.id is None:
                # Retrieve the last inserted ID for new records
                model.id = cls._db.conn.execute(
                    "SELECT last_insert_rowid()"
                ).fetchone()[0]
            saved_ids.append(model.id)

        return saved_ids
