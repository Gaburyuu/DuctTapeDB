from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import HTMLResponse
from fastui import FastUI, AnyComponent, prebuilt_html, components as c
from fastui.components.display import DisplayMode, DisplayLookup
from fastui.events import GoToEvent
from src import HookLoopTable, HookLoopModel
from contextlib import asynccontextmanager
from typing import Optional


#
# Example model
class Item(HookLoopModel):
    name: str
    description: Optional[str] = None
    price: float
    in_stock: bool


# Global variable for table
table: HookLoopTable = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global table
    table = await HookLoopTable.create_file("items", "_ducttapedb_example_items.db")
    await table.initialize()
    Item.set_table(table)
    yield
    if table and table.controller:
        await table.controller.close()


# Initialize FastAPI with lifespan
app = FastAPI(lifespan=lifespan)


# FastUI Endpoints
@app.get("/api/", response_model=FastUI, response_model_exclude_none=True)
async def items_table() -> list[AnyComponent]:
    """Display all items in a table."""
    items = await Item.models_from_db()
    return [
        c.Page(
            components=[
                c.Heading(text="Items", level=2),
                c.Table(
                    data=items,
                    data_model=Item,
                    columns=[
                        DisplayLookup(
                            field="name",
                            mode=DisplayMode.auto,
                            on_click=GoToEvent(url="/item/{id}/"),
                        ),
                        DisplayLookup(field="description"),
                        DisplayLookup(field="price", mode=DisplayMode.currency),
                        DisplayLookup(field="in_stock", table_width_percent=10),
                    ],
                ),
                c.Link(
                    components=[c.Text(text="Add Item")],
                    on_click=GoToEvent(url="/item/new"),
                ),
            ]
        ),
    ]


@app.get("/api/item/{item_id}/", response_model=FastUI)
async def item_detail(item_id: int):
    """Display details of a single item."""
    try:
        item = await Item.from_id(item_id)
        return [
            c.Page(
                components=[
                    c.Heading(text=item.name, level=2),
                    c.Link(text="Back to Items", url="/"),
                    c.Details(data=item.dict()),
                    c.Link(text="Edit Item", url=f"/api/item/{item_id}/edit"),
                ]
            ),
        ]
    except ValueError:
        raise HTTPException(status_code=404, detail="Item not found")


@app.get("/api/item/new", response_model=FastUI, response_model_exclude_none=True)
async def new_item_form():
    """Form to create a new item."""
    return [
        c.Page(
            components=[
                c.Heading(text="New Item", level=2),
                c.Form(
                    submit_url="/items/",
                    method="POST",
                    form_fields=[
                        c.FormFieldInput(
                            name="name",
                            required=True,
                            title="Name",
                        ),
                        c.forms.FormFieldTextarea(
                            rows=3,
                            name="description",
                            required=True,
                            title="description",
                        ),
                        c.forms.FormFieldInput(
                            name="price",
                            title="Price",
                            required=True,
                            html_type="number",
                        ),
                        c.forms.FormFieldBoolean(
                            name="in_stock",
                            title="In Stock",
                            initial=False,
                        ),
                    ],
                ),
            ]
        ),
    ]


@app.post("/items/", response_model_exclude_none=True)
async def create_item(
    name: str = Form(...),
    description: Optional[str] = Form(None),
    price: float = Form(...),
    in_stock: Optional[bool] = Form(False),
):
    """Create a new item."""
    item = Item(
        name=name,
        description=description,
        price=price,
        in_stock=in_stock,
    )
    await item.save()

    return [c.FireEvent(event=GoToEvent(url="/"))]


@app.get("/items/{item_id}", response_model=dict)
async def get_item(item_id: int):
    """Retrieve an item by ID."""
    try:
        item = await Item.from_id(item_id)
        return item.dict()
    except ValueError:
        raise HTTPException(status_code=404, detail="Item not found.")


@app.put("/items/{item_id}", response_model=dict)
async def update_item(item_id: int, item: Item):
    """Update an existing item."""
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
    """Delete an item."""
    try:
        item = await Item.from_id(item_id)
        await item.delete()
        return {"status": "Item deleted successfully.", "id": item_id}
    except ValueError:
        raise HTTPException(status_code=404, detail="Item not found.")


@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    """Serve the FastUI frontend."""
    return HTMLResponse(content=prebuilt_html(title="Items Manager"))
