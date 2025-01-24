from .model import AutoSafetyTapeModel
from typing import Any
import asyncio


class AutoAutoSafetyTapeModel(AutoSafetyTapeModel):
    """
    An extension of AutoSafetyTapeModel that automatically saves changes to the database
    whenever a field is updated.

    Overrides the __setattr__ method to:
    - Track updated fields.
    - Save the updated fields to the database immediately.

    Note:
        - This implementation assumes the save operation is reliable and will succeed.
        - Exceptions during save will propagate, so ensure proper error handling in usage.
    """

    _debounce_tasks: dict[str, asyncio.Task] = {}


    def debounced_update(self, key: str, value: Any, delay: float = 0.1) -> None:
        """
        Update the field and debounce the save operation.

        Args:
            key (str): The name of the field to update.
            value (Any): The new value for the field.
            delay (float): The debounce delay in seconds (default: 0.1).

        Raises:
            RuntimeError: If no event loop is running.
        """
        # Update the field
        self.__setattr__(key, value)

        # Cancel any existing debounce task for this field
        if key in self._debounce_tasks and not self._debounce_tasks[key].done():
            self._debounce_tasks[key].cancel()

        # Schedule a new debounce task for saving
        async def debounced_save():
            try:
                await asyncio.sleep(delay)  # Wait for the debounce period
                await self.save()  # Save the model
            except asyncio.CancelledError:
                pass  # Ignore canceled tasks

        loop = asyncio.get_running_loop()
        self._debounce_tasks[key] = loop.create_task(debounced_save())


    async def setattr(self, key: str, value: Any) -> None:
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


    async def _debounced_save(self):
        """
        Wait for a short debounce period before saving.
        """
        try:
            await asyncio.sleep(0.1)  # Adjust debounce duration as needed
            await self.save()
        except asyncio.CancelledError:
            pass  # Ignore cancellations when a new save is triggered