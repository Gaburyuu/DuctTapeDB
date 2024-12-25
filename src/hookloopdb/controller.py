import aiosqlite


class AsyncSQLiteController:
    def __init__(self, db_path: str):
        self.db_path: str = db_path
        self.connection: aiosqlite.Connection = None

    async def connect(self, uri: bool = False):
        """Establish a connection to the SQLite database."""
        if self.connection:
            return

        self.connection = await aiosqlite.connect(self.db_path, uri=uri)
        # Switch to Write-Ahead Logging (WAL) mode
        # WHAT: This changes SQLite's journaling mode to WAL, which separates reads and writes into two files.
        #       This allows concurrent reads while writes are being performed in the WAL file.
        # WHY: WAL mode is ideal for high-concurrency scenarios (like FastAPI) because it improves read performance
        #      and allows multiple readers while a single writer is active.
        await self.connection.execute("PRAGMA journal_mode = WAL;")

        # Set synchronous mode to NORMAL
        # WHAT: The "synchronous" PRAGMA determines how aggressively SQLite ensures that data is written to disk.
        #       "NORMAL" means SQLite syncs writes less often than "FULL" mode, which reduces disk I/O.
        # WHY: NORMAL mode is a trade-off between durability and performance, suitable for most web applications.
        #      It ensures fast writes while maintaining reasonable safety for data (you might lose the last few
        #      transactions during a crash but the database remains intact).
        await self.connection.execute("PRAGMA synchronous = NORMAL;")

        # Increase cache size to 64MB
        # WHAT: This sets the size of SQLite's in-memory page cache. The negative value (-64000) specifies
        #       the size in kilobytes, so this allocates 64MB of memory to caching database pages.
        # WHY: A larger cache reduces the need to repeatedly read database pages from disk, improving
        #      performance for read-heavy operations. It’s especially useful for apps with frequent queries.
        await self.connection.execute("PRAGMA cache_size = -64000;")

        # Store temporary tables and results in memory
        # WHAT: Temporary tables, indices, and other intermediate results are stored in memory instead of disk.
        # WHY: Using RAM for temporary storage speeds up operations like sorting and joins, which can be common
        #      in complex queries or when working with intermediate data during bulk inserts/updates.
        await self.connection.execute("PRAGMA temp_store = MEMORY;")

    async def close(self):
        """Close the database connection."""
        if self.connection:
            await self.connection.close()
            self.connection = None

    async def execute(self, query: str, params=None):
        """Execute a single query."""
        async with self.connection.execute(query, params or ()) as cursor:
            return await cursor.fetchall()

    async def executemany(self, query: str, param_list):
        """Execute multiple queries in a batch."""
        await self.connection.executemany(query, param_list)

    async def execute_script(self, script: str):
        """Execute multiple SQL commands as a script."""
        await self.connection.executescript(script)

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
