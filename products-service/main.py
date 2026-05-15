from fastapi import FastAPI
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager
from models import Product
from datetime import datetime
import asyncio
import json

producer = None

@asynccontextmanager
async def lifespan(app: FastAPI):

    global producer

    producer = AIOKafkaProducer(
        bootstrap_servers='kafka:9092'
    )

    await producer.start()

    consumer = AIOKafkaConsumer(
        "order-created",
        bootstrap_servers='kafka:9092',
        group_id="products-group",
        auto_offset_reset="earliest"
    )

    await consumer.start()

    task = asyncio.create_task(consume(consumer))

    yield

    task.cancel()

    await consumer.stop()
    await producer.stop()

app = FastAPI(
    title="Products Service",
    lifespan=lifespan
)

products_db = {
    1: Product(id=1, name="Laptop", price=1500.0, quantity=10),
    2: Product(id=2, name="Mouse", price=25.0, quantity=50)
}

async def consume(consumer: AIOKafkaConsumer):

    try:
        async for msg in consumer:

            order = json.loads(msg.value.decode('utf-8'))

            product = products_db.get(order['product_id'])

            # Product not found
            if not product:

                await producer.send_and_wait(
                    "product_not_found_events",
                    json.dumps({
                        "order_id": order['id'],
                        "product_id": order['product_id'],
                        "timestamp": datetime.utcnow().isoformat(),
                        "error_reason": "Proizvod ne postoji u katalogu"
                    }).encode('utf-8')
                )

            # Out of stock
            elif product.quantity < order['quantity']:

                await producer.send_and_wait(
                    "out_of_stock_events",
                    json.dumps({
                        "order_id": order['id'],
                        "product_id": order['product_id'],
                        "timestamp": datetime.utcnow().isoformat(),
                        "error_reason": "Nedovoljna količina na stanju"
                    }).encode('utf-8')
                )

            # Success
            else:

                product.quantity -= order['quantity']

                await producer.send_and_wait(
                    "order-confirmed",
                    json.dumps({
                        "order_id": order['id'],
                        "product_id": product.id
                    }).encode('utf-8')
                )

    except asyncio.CancelledError:
        pass

@app.get("/products")
def get_products():
    return products_db