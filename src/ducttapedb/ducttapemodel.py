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
        """Save multiple models at once, assigning IDs only to new rows."""
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
        new_models = []  # Track models without IDs for later assignment
        for model in models:
            data = model.model_dump(exclude={"id"})
            json_data = json.dumps(data)

            if model.id is None:
                # New row: id will be auto-generated
                params.append((None, json_data, json_data))
                new_models.append(model)
            else:
                # Existing row: keep the provided ID
                params.append((model.id, json_data, json_data))

        # Begin transaction
        with cls._db.conn as conn:
            # Get the current max ID before inserting
            current_max_id = conn.execute(
                f"SELECT COALESCE(MAX(id), 0) FROM {cls._db.table}"
            ).fetchone()[0]

            # Perform the bulk operation
            conn.executemany(query, params)

            # Get the new max ID after inserting
            new_max_id = conn.execute(
                f"SELECT MAX(id) FROM {cls._db.table}"
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
