import os
from collections.abc import Callable
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel

from constants import (
    EXCHANGE_YAHOO_FINANCE,
    QUEUE_YAHOO_FINANCE,
    ROUTING_KEY_YAHOO_FINANCE,
)


class RabbitMQProducer:
    def __init__(self) -> None:
        self._host = os.getenv("RABBITMQ_HOST", "localhost")
        self._port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self._username = os.getenv("RABBITMQ_USERNAME", "guest")
        self._password = os.getenv("RABBITMQ_PASSWORD", "guest")

    def create_channel(
        self,
    ) -> tuple[pika.BlockingConnection, BlockingChannel]:
        connection_parameters = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            credentials=pika.PlainCredentials(
                username=self._username,
                password=self._password,
            ),
        )

        connection = pika.BlockingConnection(connection_parameters)
        channel = connection.channel()

        channel.exchange_declare(
            exchange=EXCHANGE_YAHOO_FINANCE,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(queue=QUEUE_YAHOO_FINANCE, durable=True)
        channel.queue_bind(
            exchange=EXCHANGE_YAHOO_FINANCE,
            queue=QUEUE_YAHOO_FINANCE,
            routing_key=ROUTING_KEY_YAHOO_FINANCE,
        )

        return connection, channel

    @staticmethod
    def close_channel(
        connection: pika.BlockingConnection,
        channel: BlockingChannel,
    ) -> None:
        if channel.is_open:
            channel.close()
        if connection.is_open:
            connection.close()

    def publish(self, message: str, routing_key: str = "") -> None:
        connection, channel = self.create_channel()
        try:
            channel.basic_publish(
                exchange=EXCHANGE_YAHOO_FINANCE,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        finally:
            self.close_channel(connection, channel)


class RabbitMQConsumer:
    def __init__(
        self,
        callback_function: Callable[[Any, Any, Any, bytes], Any],
        queue_name: str,
    ) -> None:
        self._host = os.getenv("RABBITMQ_HOST", "localhost")
        self._port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self._username = os.getenv("RABBITMQ_USERNAME", "guest")
        self._password = os.getenv("RABBITMQ_PASSWORD", "guest")
        self._queue = queue_name
        self._callback = callback_function

    def create_channel(
        self,
    ) -> tuple[pika.BlockingConnection, BlockingChannel]:
        connection_parameters = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            credentials=pika.PlainCredentials(
                username=self._username,
                password=self._password,
            ),
        )

        connection = pika.BlockingConnection(connection_parameters)
        channel = connection.channel()
        channel.exchange_declare(
            exchange=EXCHANGE_YAHOO_FINANCE,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(queue=self._queue, durable=True)
        channel.queue_bind(
            exchange=EXCHANGE_YAHOO_FINANCE,
            queue=self._queue,
            routing_key=ROUTING_KEY_YAHOO_FINANCE,
        )

        return connection, channel

    @staticmethod
    def close_channel(
        connection: pika.BlockingConnection,
        channel: BlockingChannel,
    ) -> None:
        if channel.is_open:
            channel.close()
        if connection.is_open:
            connection.close()

    def consume(self) -> None:
        connection, channel = self.create_channel()
        channel.basic_consume(
            queue=self._queue,
            on_message_callback=self._callback,
            auto_ack=True,
        )
        try:
            channel.start_consuming()
        finally:
            self.close_channel(connection, channel)
