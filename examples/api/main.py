from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from src.hookloopdb.table import HookLoopTable
from src.hookloopdb.model import HookLoopModel

# Example model
class Item(HookLoopModel):
    name: str
    description: str
    price: float
    in_stock: bool

# Global variable for table
table: HookLoopTable = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global table
    table = await HookLoopTable.create_file("items", "items.db")
    yield  # Application runs during this context
    if table and table.controller:
        await table.controller.close()

# Initialize FastAPI with lifespan
app = FastAPI(lifespan=lifespan)

# CRUD Endpoints

@app.post("/items/", response_model=dict)
async def create_item(item: Item):
    saved_id = await item.save()
    return {"id": saved_id, "status": "Item created successfully."}

@app.get("/items/{item_id}", response_model=dict)
async def get_item(item_id: int):
    try:
        item = await Item.from_id(item_id)
        return item.dict()
    except ValueError:
        raise HTTPException(status_code=404, detail="Item not found.")

@app.put("/items/{item_id}", response_model=dict)
async def update_item(item_id: int, item: Item):
    try:
        existing_item = await Item.from_id(item_id)
        for key, value in item.dict(exclude_unset=True).items():
            setattr(existing_item, key, value)
        await existing_item.save()
        return {"status": "Item updated successfully.", "id": item_id}
    except ValueError:
        raise HTTPException(status_code=404, detail="Item not found.")

@app.delete("/items/{item_id}", response_model=dict)
async def delete_item(item_id: int):
    try:
        item = await Item.from_id(item_id)
        await item.delete()
        return {"status": "Item deleted successfully.", "id": item_id}
    except ValueError:
        raise HTTPException(status_code=404, detail="Item not found.")
