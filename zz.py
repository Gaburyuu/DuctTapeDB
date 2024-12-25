import asyncio
import aiosqlite


async def main():
    try:
        # Connect to an in-memory SQLite database
        db = await aiosqlite.connect(":memory:")
        try:
            # Execute a query to get the metadata (list of tables)
            await db.execute(
                "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT);"
            )
            await db.commit()

            # Fetch the metadata
            async with db.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ) as cursor:
                tables = await cursor.fetchall()
                print("Tables in the database:", tables)
        finally:
            # Explicitly close the connection
            await db.close()

    except Exception as e:
        print("An error occurred:", e)


# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
