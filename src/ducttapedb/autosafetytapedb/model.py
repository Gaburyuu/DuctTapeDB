from ..safetytapedb.model import SafetyTapeModel
from typing import Any
import json


class AutoSafetyTapeModel(SafetyTapeModel, validate_assignment=True):
    updated_fields: set[str] = set()

    def __setattr__(self, key: str, value: Any) -> None:
        """
        Track fields that are being updated and ensure validation.
        """
        if key in self.model_fields and getattr(self, key, None) != value:
            super().__setattr__(key, value)  # Set the value with validation
            self.updated_fields.add(key)

    async def save(self) -> int:
        """
        Save the model to the database, updating only the fields that were modified.

        Returns:
            int: The ID of the saved model.

        Raises:
            ValueError: If no table is set for the model.
            RuntimeError: If the save operation fails.
        """
        if not self._table:
            raise ValueError("No table is set for this model.")

        if not self.id:
            # Insert the entire model as it's a new record
            data = self.model_dump(exclude={"id", "version", "updated_fields"})
            query = f"""
                INSERT INTO {self._table.table_name} (created_at, updated_at, data)
                VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, json(?))
                RETURNING id, version
            """
            params = (json.dumps(data),)
            async with self._table.controller._connection.execute(
                query, params
            ) as cursor:
                result = await cursor.fetchone()
                self.id, self.version = result
            self.updated_fields.clear()
            return self.id

        # If no fields were updated, return early
        if not self.updated_fields:
            return self.id

        # Prepare partial update query
        update_fields = list(self.updated_fields)
        update_query = f"""
            UPDATE {self._table.table_name}
            SET {", ".join([f"data = json_set(data, '$.{field}', ?)" for field in update_fields])},
                version = version + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND version = ?
            RETURNING version
        """
        update_values = [getattr(self, field) for field in update_fields] + [
            self.id,
            self.version,
        ]

        # Execute the update
        async with self._table.controller._connection.execute(
            update_query, update_values
        ) as cursor:
            result = await cursor.fetchone()
            if result is None:
                raise RuntimeError(
                    f"Update failed for id={self.id}. Version mismatch detected."
                )
            self.version = result[0]

        # Clear updated fields
        self.updated_fields.clear()

        return self.id

    def get_partial_update_data(self) -> dict:
        """
        Get the fields that have been updated since the last save.

        Returns:
            dict: A dictionary of updated fields and their current values.
        """
        return {field: getattr(self, field) for field in self.updated_fields}

    async def asetattr(self, key: str, value: Any) -> None:
        """
        Override attribute assignment to track updates and save changes.

        Args:
            key (str): The name of the attribute being updated.
            value (Any): The new value for the attribute.

        Raises:
            RuntimeError: If the save operation fails due to version mismatch or other errors.
        """
        if key in self.model_fields and getattr(self, key, None) != value:
            super().__setattr__(key, value)  # Set the value with validation
            self.updated_fields.add(key)
            await self.save()
