import aiosqlite


class AsyncSQLiteController:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection: aiosqlite.Connection = None

    async def connect(self):
        """Establish a connection to the SQLite database."""
        self.connection = await aiosqlite.connect(self.db_path)
        await self.connection.execute("PRAGMA journal_mode = WAL;")
        await self.connection.execute("PRAGMA synchronous = NORMAL;")
        await self.connection.execute("PRAGMA cache_size = -64000;")
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
        await self.connection.commit()

    async def execute_script(self, script: str):
        """Execute multiple SQL commands as a script."""
        await self.connection.executescript(script)
