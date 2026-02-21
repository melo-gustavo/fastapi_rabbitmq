import pika
from fastapi import APIRouter, HTTPException

try:
    from producer.services.rabbitmq import QUEUE_HELLO, create_channel
except ModuleNotFoundError:
    from services.rabbitmq import QUEUE_HELLO, create_channel

router = APIRouter(prefix="/rabbitmq")


@router.get("/publish")
async def publish_message(message: str):
    connection = None
    channel = None

    try:
        connection, channel = create_channel(QUEUE_HELLO)
        channel.basic_publish(exchange="", routing_key=QUEUE_HELLO, body=message)
        return {"message": "Message published"}
    except pika.exceptions.AMQPError as exc:
        raise HTTPException(status_code=503, detail="RabbitMQ unavailable") from exc
    finally:
        if channel and channel.is_open:
            channel.close()
        if connection and connection.is_open:
            connection.close()
