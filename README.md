# 🛠️ DuctTapeDB

DuctTapeDB is a lightweight, SQLite-powered solution designed for **quickly persisting and searching Pydantic models**. Whether you're working on **non-technical projects** or building **fast prototypes with FastAPI**, DuctTapeDB provides a simple and intuitive way to store and manage your data.

Originally created for a hobby project, DuctTapeDB has evolved into a powerful tool for **rapid development**, with a focus on ease of use and integration. 🚀

---

## **Why Use DuctTapeDB?**

- **Pydantic-Centric**: Effortlessly store and search Pydantic models without additional setup.
- **FastAPI-Ready**: Perfect for creating CRUD APIs in minutes.
- **Lightweight**: Powered by SQLite—works out-of-the-box, no server required.
- **Async and Sync Support**:
  - **HookLoopDB** (Async): Feature-rich and optimized for modern async workflows.
  - **DuctTapeDB** (Sync): A straightforward synchronous option, with plans to align features across both modes.

---

## **Features**

- **Simple Persistence**: Automatically save and retrieve Pydantic models with minimal code.
- **Advanced Querying**: Query data using JSON fields and SQL expressions.
- **Async and Sync Options**: Use what fits your project best.
- **FastAPI Integration**: Quickly build APIs with CRUD functionality.
- **SQLite-Powered**: Works anywhere—no need for additional infrastructure.

---

## **Installation**

Install DuctTapeDB using pip:

```bash
pip install ducttapedb
```

For examples using **FastAPI** and **FastUI**, ensure you also install the required dependencies:

```bash
pip install fastapi fastui pydantic
```

---

## **Quickstart**

### 1. Define Your Pydantic Model

```python
from ducttapedb.hookloopdb.model import HookLoopModel

class Item(HookLoopModel):
    name: str
    description: str
    price: float
    in_stock: bool
```

---

### 2. Create a Database

```python
from ducttapedb.hookloopdb.table import HookLoopTable

# Create an async SQLite database
async def setup_database():
    table = await HookLoopTable.create_file("items", "items.db")
    await table.initialize()
    Item.set_table(table)
```

---

### 3. Perform CRUD Operations

#### Create
```python
item = Item(name="Widget", description="A useful widget", price=19.99, in_stock=True)
await item.save()
```

#### Read
```python
retrieved_item = await Item.from_id(item.id)
print(retrieved_item)
```

#### Query
```python
items_in_stock = await Item.models_from_db(order_by="json_extract(data, '$.price') ASC")
print(items_in_stock)
```

#### Delete
```python
await item.delete()
```

---

## **Using with FastAPI**

You can quickly spin up a CRUD API using DuctTapeDB with FastAPI. Here's how:

1. **Run the Example API**:
   - Install dependencies:
     ```bash
     pip install fastapi fastui pydantic
     ```
   - Start the development server:
     ```bash
     uvicorn examples.api.main:app --reload
     ```

2. **Navigate to**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) for the interactive API documentation, or to [http://127.0.0.1:8000](http://127.0.0.1:8000) for a very simple FastUI table and a form to insert items.

---

## **Roadmap**

- Align features across **HookLoopDB** (Async) and **DuctTapeDB** (Sync).
- Add more advanced querying capabilities.
- Simplify relationships and data normalization.

---

## **Contributing**

Contributions are welcome! If you encounter bugs or have feature requests, feel free to open an issue on GitHub.

---

## **License**

DuctTapeDB is licensed under the MIT License. See the `LICENSE` file for more details.

---

Let me know if you'd like any additional tweaks! 🚀