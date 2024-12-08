# 🛠️ DuctTapeDB

DuctTapeDB is a lightweight, SQLite-powered, NoSQL-like database. It’s like a duct-tape fix for when you need simple JSON storage without the complexity of a full-blown database. Built with Python, it integrates neatly with Pydantic models for schema validation and object management.

Originally used for a hobby project of mine. Perfect for hobby projects or just experimenting with a "NoSQL but make it SQLite" approach. 🚀

---

## **Features**

- **JSON Storage**: Store and query JSON documents like you would in a NoSQL database.
- **Pydantic Integration**: Use Pydantic models to validate and manage your data, and auto-save the models to the database.
- **Lightweight**: Powered by SQLite—no server needed, works out-of-the-box!

---

### **TODO**

- **Relationships**: Simulate document relationships across tables.

---

## **Installation**

You can install DuctTapeDB using pip:

```bash
pip install ducttapedb
```

## Quickstart

https://pypi.org/project/ducttapedb/

```bash
pip install ducttapedb
```

### Here's how you can get started:

1. Create a Database

```python
from ducttapedb.ducttapedb import DuctTapeDB

# Create an in-memory database
db = DuctTapeDB.create_memory()
```

2. Define a Pydantic Model

```python
from ducttapedb.ducttapemodel import DuctTapeModel

class MyDocument(DuctTapeModel):
    name: str
    value: int
```

3. Save and Retrieve Data

```python
# Create an instance
doc = MyDocument(name="Slime", value=42)

# Save to the database
doc.save(db)

# Retrieve by ID
retrieved_doc = MyDocument.from_id(db, doc.id)
print(retrieved_doc)
```
