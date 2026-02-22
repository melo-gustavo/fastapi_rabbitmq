import asyncio
import csv
import datetime
import io
import json
import os
from typing import Any

from fastapi import UploadFile
from sqlalchemy import insert

from constants import (
    EXCHANGE_YAHOO_FINANCE,
    QUEUE_YAHOO_FINANCE,
    ROUTING_KEY_YAHOO_FINANCE,
)
from db.db import SessionLocal
from models.yahoo_finance import YahooFinance
from services.rabbitmq import RabbitMQConsumer, RabbitMQProducer


class YahooFinanceProducer:
    def __init__(self) -> None:
        self._exchange = os.getenv(EXCHANGE_YAHOO_FINANCE, "yahoo_finance")

    async def publish_csv(
        self, body: UploadFile, country: str
    ) -> dict[str, str | int]:
        raw_content = await body.read()
        if not raw_content:
            raise ValueError("CSV vazio. Envie um arquivo com conteúdo.")

        try:
            csv_content = raw_content.decode("utf-8-sig")
        except UnicodeDecodeError as error:
            raise ValueError("CSV deve estar em UTF-8.") from error

        payload = json.dumps(
            {
                "filename": body.filename,
                "content": csv_content,
                "country": country,
            },
            ensure_ascii=False,
        )

        RabbitMQProducer().publish(
            message=payload,
            routing_key=ROUTING_KEY_YAHOO_FINANCE,
        )

        return {
            "status": "published",
            "exchange": self._exchange,
            "filename": body.filename or "unknown.csv",
            "bytes": len(raw_content),
        }


class YahooFinanceConsumer:
    def __init__(self) -> None:
        self._host = os.getenv("RABBITMQ_HOST", "localhost")
        self._port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self._queue = os.getenv(
            "QUEUE_YAHOO_FINANCE",
            QUEUE_YAHOO_FINANCE,
        )

    def callback_function(
        self,
        ch: Any,
        method: Any,
        properties: Any,
        body: bytes,
    ) -> str:
        print(
            f"Received data from {self._host}:{self._port} "
            f"on queue {self._queue}"
        )
        decoded_body = body.decode()

        try:
            payload = json.loads(decoded_body)
            filename = payload.get("filename", "unknown.csv")
            csv_content = payload.get("content", "")
            country = payload.get("country", "")
            rows = self._parse_csv_rows(csv_content)
            self.receive_csv_data(
                rows=rows,
                filename=filename,
                country=country,
            )
        except json.JSONDecodeError:
            print(f"Data: {decoded_body}")

        return decoded_body

    def start_consumer(self) -> None:
        yahoo_finance_consumer = RabbitMQConsumer(
            callback_function=self.callback_function,
            queue_name=self._queue,
        )

        print(
            f"Listening data from {self._host}:{self._port} "
            f"on queue {self._queue}"
        )

        yahoo_finance_consumer.consume()

    @staticmethod
    def _normalize_header(header: str) -> str:
        return header.strip().lower().replace(" ", "").replace("-", "_")

    @classmethod
    def _parse_csv_rows(cls, csv_content: str) -> list[dict[str, str]]:
        stream = io.StringIO(csv_content)
        reader = csv.DictReader(stream)
        normalized_rows: list[dict[str, str]] = []
        for row in reader:
            normalized_rows.append(
                {
                    cls._normalize_header(key): value
                    for key, value in row.items()
                }
            )
        return normalized_rows

    @staticmethod
    def _parse_price(price_raw: object) -> float | None:
        price_text = str(price_raw).strip()
        if price_text == "":
            return None

        normalized = price_text.replace(" ", "")
        if "," in normalized and "." not in normalized:
            normalized = normalized.replace(",", ".")
        else:
            normalized = normalized.replace(",", "")

        try:
            return float(normalized)
        except ValueError:
            return None

    def _extract_payload_rows(
        self,
        rows: list[dict[str, str]],
        country: str,
    ) -> list[dict[str, object | None]]:
        payload_rows: list[dict[str, object | None]] = []
        for row in rows:
            symbol = (row.get("symbol") or "").strip()
            name = (row.get("name") or "").strip()
            price_raw = row.get("price") or ""

            if not symbol:
                continue

            payload_rows.append(
                {
                    "symbol": symbol,
                    "name": name or None,
                    "price": self._parse_price(price_raw),
                    "country": country.title(),
                    "created_at": datetime.datetime.utcnow(),
                }
            )

        return payload_rows

    @staticmethod
    async def _bulk_insert(
        payload_rows: list[dict[str, object | None]],
    ) -> None:
        async with SessionLocal() as db:
            try:
                await db.execute(insert(YahooFinance), payload_rows)
                await db.commit()
            except Exception:
                await db.rollback()
                raise

        print(f"Insert realizado com {len(payload_rows)} linhas.")

    def receive_csv_data(
        self,
        rows: list[dict[str, str]],
        filename: str,
        country: str,
    ) -> None:
        print(f"CSV '{filename}' recebido com {len(rows)} linhas")
        if not rows:
            print("Nenhuma linha para inserir.")
            return

        payload_rows = self._extract_payload_rows(rows, country)
        if not payload_rows:
            print("Nenhuma linha válida para inserir.")
            return

        asyncio.run(self._bulk_insert(payload_rows))
