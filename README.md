# DuctTapeDB

**DuctTapeDB** is a lightweight persistence layer for **Pydantic** models using **SQLite** (both sync & async). Think of it as a quick, no-frills doc store that supports partial updates, optimistic concurrency, and both synchronous and asynchronous usage.

## Key Features

- **Synchronous & Asynchronous** support out of the box:
  - Use `DuctTapeDB` for a straightforward sync workflow.
  - Use `HookLoopTable + AsyncSQLiteController` for async operations.
- **Pydantic Integration**:
  - Define models by extending `DuctTapeModel` or its variants (like `SafetyTapeModel` or `AutoSafetyTapeModel`).
  - Automatic validation, model dumping, etc. courtesy of Pydantic.
- **Optimistic Locking** with a built-in version column (`SafetyTapeModel`):
  - Prevent conflicting updates by incrementing a `version` each time.
- **Partial JSON Updates**:
  - Update only changed fields in the JSON column, saving time and concurrency headaches.
- **Soft Deletes** and extra auditing columns (`created_at`, `updated_at`, etc.) if you need them.
- **Extensive Test Suite** showing concurrency handling, partial updates, and more.

## Installation

```bash
pip install ducttapedb
```

*(Or if you want to install from source, clone the repo and do `pip install .`.)*

## Quick Start (Sync)

Below is a simple synchronous example using `DuctTapeDB`:

```python
from ducttapedb import DuctTapeDB, DuctTapeModel

# 1. Define your model
class Hero(DuctTapeModel):
    name: str
    level: int

# 2. Create a DB instance (in-memory by default)
db = DuctTapeDB.create_memory(table="heroes")
# Set the shared DB on the model class
Hero.set_db(db)

# 3. Create and save a model
erdrick = Hero(name="Erdrick", level=50)
erdrick.save()          # returns the auto-generated ID
print(erdrick.id)       # e.g. 1

# 4. Retrieve the same record
loaded = Hero.from_id(erdrick.id)
print(loaded.name)      # "Erdrick"
print(loaded.level)     # 50

# 5. Update & re-save
loaded.level = 99
loaded.save()
```

## Quick Start (Async)

For asynchronous usage, you’ll typically work with `HookLoopTable`, an `AsyncSQLiteController`, and a Pydantic model that extends `HookLoopModel`:

```python
import asyncio
from ducttapedb.hookloopdb import HookLoopModel, HookLoopTable
from ducttapedb.hookloopdb.controller import AsyncSQLiteController
from typing import Optional

class AsyncHero(HookLoopModel):
    name: str
    level: int
    hp: Optional[int] = 100  # default HP

async def main():
    controller = await AsyncSQLiteController.create_memory(shared_cache=True)
    table = HookLoopTable(controller, "async_heroes")
    await table.initialize(indexes=["name", "level"])

    # 1. Set table on the model
    AsyncHero.set_table(table)

    # 2. Create & save a model
    hero = AsyncHero(name="Async Erdrick", level=30)
    await hero.save()
    print("New Hero ID:", hero.id)

    # 3. Load by ID
    loaded = await AsyncHero.from_id(hero.id)
    print("Loaded Hero:", loaded)

asyncio.run(main())
```

## SafetyTapeModel for Optimistic Locking

If you need concurrency protection and version checks, use `SafetyTapeModel` and `SafetyTapeTable`. Each update increments a `version` column, so if two processes (or tasks) try to update the same row simultaneously, the second one to save will raise a `RuntimeError` if the version is stale.

```python
from ducttapedb.safetytapedb import SafetyTapeModel, SafetyTapeTable

class Monster(SafetyTapeModel):
    name: str
    level: int
    # version is auto-handled behind the scenes

# ...

# On save, if version doesn’t match, you get a RuntimeError
```

## AutoSafetyTapeModel for Partial Updates

For even more convenience, use `AutoSafetyTapeModel` which tracks updated fields automatically. Only changed fields are written back to the DB:

```python
from ducttapedb.safetytapedb import AutoSafetyTapeModel

class AutoMonster(AutoSafetyTapeModel):
    name: str
    level: int

monster = AutoMonster(name="Slime", level=5)
await monster.save()   # Full insert
monster.level = 6
await monster.save()   # Updates only level in JSON
# Or as a one-liner
await monster.asetattr(key="level", value=7)
```

## Partial Updates in Action

With `AutoSafetyTapeModel`, you can see which fields changed by checking `updated_fields`. If none changed, no update is performed. That’s a big concurrency boost for heavily contended rows.

## Testing

We use `pytest` (including `pytest-asyncio`). To run tests:

```bash
pytest
```

*(We also have concurrency stress tests, partial update tests, version mismatch tests, etc.)*

## Contributing

1. Clone the repo
2. Create a virtual environment
3. Install dependencies: `pip install -e .[dev]`
4. Run `black . && ruff check . && pytest`

Pull requests are welcome! For major changes, please open an issue first to discuss what you’d like to change.
## License

MIT License—see the [LICENSE](LICENSE) file for details.
