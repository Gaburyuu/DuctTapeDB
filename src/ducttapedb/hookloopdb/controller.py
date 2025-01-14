import asyncio
import aiosqlite


class AsyncSQLiteController:
    def __init__(self, db_path: str, semaphore: int = 100):
        self.db_path: str = db_path
        self._connection: aiosqlite.Connection = None
        self._lock = asyncio.Lock()  # Lock to ensure task safety
        self._semaphore = asyncio.Semaphore(semaphore)  # Limit concurrent queries

    async def connect(self, uri: bool = False):
        """Establish a connection to the SQLite database."""
        async with self._lock:
            if self._connection:
                return

            self._connection = await aiosqlite.connect(self.db_path, uri=uri)

            # Switch to Write-Ahead Logging (WAL) mode
            await self._connection.execute("PRAGMA journal_mode = WAL;")

            # Set synchronous mode to NORMAL
            await self._connection.execute("PRAGMA synchronous = NORMAL;")

            # Increase cache size to 64MB
            await self._connection.execute("PRAGMA cache_size = -64000;")

            # Store temporary tables and results in memory
            await self._connection.execute("PRAGMA temp_store = MEMORY;")

            # Wait up to 5000ms (5 seconds)
            await self._connection.execute("PRAGMA busy_timeout = 5000;")

    async def close(self):
        """Close the database connection."""
        async with self._lock:
            if self._connection:
                await self._connection.close()
                self._connection = None

    async def __aenter__(self):
        if not self._connection:
            await self.connect()  # Ensure connection is established
        await self._connection.execute("BEGIN")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self._connection.execute("COMMIT")
        else:
            await self._connection.execute("ROLLBACK")

    async def execute(self, query: str, params=None):
        """Execute a single query."""
        async with self._semaphore:  # Limit concurrent queries
            async with self._lock:
                if not self._connection:
                    raise RuntimeError("Database connection is not established.")

                return await self._connection.execute(query, params or ())

    async def executemany(self, query: str, param_list):
        """Execute multiple queries in a batch."""
        async with self._semaphore:  # Limit concurrent queries
            async with self._lock:
                if not self._connection:
                    raise RuntimeError("Database connection is not established.")

                await self._connection.executemany(query, param_list)

    async def execute_script(self, script: str):
        """Execute multiple SQL commands as a script."""
        async with self._semaphore:  # Limit concurrent queries
            async with self._lock:
                if not self._connection:
                    raise RuntimeError("Database connection is not established.")

                await self._connection.executescript(script)

    async def commit(self):
        """Commit the current transaction."""
        async with self._semaphore:  # Limit concurrent queries
            async with self._lock:
                if not self._connection:
                    raise RuntimeError("Database connection is not established.")

                await self._connection.commit()

    @classmethod
    async def create_memory(cls, shared_cache: bool = False) -> "AsyncSQLiteController":
        """
        Factory method to create an in-memory AsyncSQLiteController.

        Args:
            shared_cache (bool): If True, creates a shared-cache in-memory DB.

        Returns:
            AsyncSQLiteController: An instance of AsyncSQLiteController with an in-memory database.
        """
        db_path = "file::memory:?cache=shared" if shared_cache else ":memory:"
        controller = cls(db_path)
        await controller.connect(uri=shared_cache)
        return controller

    @classmethod
    async def create_file(
        cls, filepath: str, uri: bool = False
    ) -> "AsyncSQLiteController":
        """
        Factory method to create a file-based AsyncSQLiteController.

        Args:
            filepath (str): The path to the SQLite database file.
            uri (bool): If True, treat `filepath` as a URI.

        Returns:
            AsyncSQLiteController: An instance of AsyncSQLiteController with a file-based database.
        """
        controller = cls(filepath)
        await controller.connect(uri=uri)
        return controller
