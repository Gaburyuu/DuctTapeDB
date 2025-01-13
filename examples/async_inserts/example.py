import asyncio
import time
from ducttapedb import HookLoopTable, HookLoopModel
from ducttapedb.hookloopdb.controller import AsyncSQLiteController


# Define two models
class Item(HookLoopModel):
    name: str
    description: str
    price: float


class Order(HookLoopModel):
    item_id: int
    quantity: int
    total_price: float


async def main():
    start_time = time.perf_counter()

    # Step 1: Setup database and tables
    setup_start = time.perf_counter()
    controller = await AsyncSQLiteController.create_memory()
    items_table = HookLoopTable(controller, "items")
    orders_table = HookLoopTable(controller, "orders")
    await items_table.initialize()
    await orders_table.initialize()
    Item.set_table(items_table)
    Order.set_table(orders_table)
    setup_end = time.perf_counter()

    print(f"Setup time: {setup_end - setup_start:.2f} seconds")

    # Step 2: Insert items and orders concurrently
    insert_start = time.perf_counter()

    async def insert_items():
        for i in range(10000):
            item = Item(
                name=f"Item #{i}",
                description=f"This is item number {i}",
                price=i + (i / 100),
            )
            await item.save()

    async def insert_orders():
        for i in range(5000):
            order = Order(
                item_id=i,
                quantity=i % 10 + 1,
                total_price=(i % 10 + 1) * (i + (i / 100)),
            )
            await order.save()

    await asyncio.gather(insert_items(), insert_orders())
    insert_end = time.perf_counter()

    print(f"Insert time: {insert_end - insert_start:.2f} seconds")

    # Step 3: Retrieve a single item by ID
    retrieve_single_start = time.perf_counter()
    retrieved_item = await Item.from_id(5000)
    retrieve_single_end = time.perf_counter()

    print(f"Retrieved item: {retrieved_item}")
    print(
        f"Retrieve single item time: {retrieve_single_end - retrieve_single_start:.2f} seconds"
    )

    # Step 4: Retrieve all orders and count
    retrieve_orders_start = time.perf_counter()
    orders = await Order.models_from_db(order_by='json_extract(data, "$.item_id") ASC')
    retrieve_orders_end = time.perf_counter()

    print(f"Number of orders: {len(orders)}")
    print(
        f"Retrieve all orders time: {retrieve_orders_end - retrieve_orders_start:.2f} seconds"
    )

    # Close the shared database connection
    close_start = time.perf_counter()
    await controller.close()
    close_end = time.perf_counter()

    print(f"Close connection time: {close_end - close_start:.2f} seconds")

    total_time = time.perf_counter() - start_time
    print(f"Total execution time: {total_time:.2f} seconds")


# Run the async main function
asyncio.run(main())
