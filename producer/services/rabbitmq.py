import os

import pika

QUEUE_HELLO = "hello"


def get_connection_parameters() -> pika.ConnectionParameters:
    host = os.getenv("RABBITMQ_HOST", "localhost")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    connection_attempts = int(os.getenv("RABBITMQ_CONNECTION_ATTEMPTS", "1"))
    retry_delay = float(os.getenv("RABBITMQ_RETRY_DELAY", "0.2"))
    socket_timeout = float(os.getenv("RABBITMQ_SOCKET_TIMEOUT", "3"))

    return pika.ConnectionParameters(
        host=host,
        port=port,
        connection_attempts=connection_attempts,
        retry_delay=retry_delay,
        socket_timeout=socket_timeout,
    )


def create_channel(name: str) -> tuple[pika.BlockingConnection, pika.channel.Channel]:
    connection = pika.BlockingConnection(get_connection_parameters())
    channel = connection.channel()
    channel.queue_declare(queue=name)
    return connection, channel
