"""Kafka connector abstraction for producer/consumer patterns."""

from __future__ import annotations

import logging
import os
import uuid
from threading import Event, Thread
from time import sleep
from typing import Any, Generator, TypeVar

from kafka import KafkaConsumer, KafkaProducer
from kafka.producer.future import FutureRecordMetadata
from pydantic import BaseModel

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOGGING_LEVEL", "INFO"))

data_T = TypeVar("data_T")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def on_send_success(record_metadata: FutureRecordMetadata) -> None:
    """Callback for successful message send."""
    logger.info(f"Message sent: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")


def on_send_error(excp: Exception) -> None:
    """Callback for message send errors."""
    logger.error(f"Failed to send message: {excp}", exc_info=excp)


class KafkaConnector:
    """Low-level Kafka producer/consumer connector."""

    def __init__(self, broker_address: str = KAFKA_BOOTSTRAP):
        """Initialize connector with broker address.

        Args:
            broker_address: Kafka bootstrap servers address (default from env or localhost:9092)
        """
        self._consumer = None
        self._producer = None
        self.broker_address = broker_address
        logger.info(f"Initialized KafkaConnector with broker: {broker_address}")

    @property
    def consumer(self) -> KafkaConsumer:
        """Lazy-load consumer."""
        if self._consumer is None:
            logger.debug("Creating KafkaConsumer...")
            self._consumer = KafkaConsumer(
                bootstrap_servers=self.broker_address,
                client_id=str(uuid.uuid4()),
                fetch_max_wait_ms=200,
                request_timeout_ms=2000,
                group_id=None,
            )
        return self._consumer

    @consumer.setter
    def consumer(self, val):
        raise RuntimeError("Cannot set consumer directly. Use broker_address instead.")

    @property
    def producer(self) -> KafkaProducer:
        """Lazy-load producer."""
        if self._producer is None:
            logger.debug("Creating KafkaProducer...")
            self._producer = KafkaProducer(
                bootstrap_servers=self.broker_address,
                client_id=str(uuid.uuid4()),
                acks="all",
                retries=3,
            )
        return self._producer

    @producer.setter
    def producer(self, val):
        raise RuntimeError("Cannot set producer directly. Use broker_address instead.")

    def _listen_for_response(self, topic: str) -> Generator[tuple[str | None, str], Any, None]:
        """Listen for messages on a topic pattern.

        Args:
            topic: Topic name or pattern (supports * wildcard)

        Yields:
            Tuples of (key, value) from Kafka messages
        """
        self.consumer.subscribe(pattern=topic)
        logger.info(f"Listening on topic pattern: {topic}")

        for message in self.consumer:
            key = uuid.uuid4() if message.key is None else message.key.decode()
            value = message.value.decode()
            logger.info(f"Received message from {message.topic}: key={key}")
            yield key, value

    def send(self, topic: str, message: str, id: str) -> None:
        """Send a message to Kafka.

        Args:
            topic: Topic to send to
            message: Message payload (string)
            id: Message key (for partitioning)
        """
        logger.info(f"Sending message to {topic} with key {id}")
        meta = self.producer.send(
            topic=topic,
            key=str(id).encode(),
            value=message.encode(),
        ).add_callback(on_send_success).add_errback(on_send_error)

        self.producer.flush()
        logger.info(f"Message flushed to {topic}")

    def close(self) -> None:
        """Close producer and consumer connections."""
        logger.info("Closing KafkaConnector...")
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        if self._producer is not None:
            self._producer.close()
            self._producer = None


class Requester:
    """Request-response pattern using Kafka topics."""

    def __init__(self, base_topic: str = "internal-test"):
        """Initialize requester with base topic.

        Args:
            base_topic: Base topic name for request/response pattern
                       (e.g., "internal-test" creates "internal-test.data.*.response", etc.)
        """
        self.requests: dict[uuid.UUID, BaseModel] = {}
        self.response: dict[uuid.UUID, BaseModel] = {}
        self.base_topic = base_topic
        self._stop_thread_event = None
        self._listener_thread = None
        self._broker_address = KAFKA_BOOTSTRAP
        self.connector = KafkaConnector(self._broker_address)
        logger.info(f"Initialized Requester with base topic: {base_topic}")

    @property
    def broker_address(self) -> str:
        """Get broker address."""
        return self._broker_address

    @broker_address.setter
    def broker_address(self, val: str) -> None:
        """Set broker address (recreates connector)."""
        self.connector.close()
        self._broker_address = val
        self.connector = KafkaConnector(self._broker_address)
        logger.info(f"Updated broker address: {val}")

    def register_request(self, req: BaseModel) -> None:
        """Register a request to be sent."""
        self.requests[req.id] = req
        logger.debug(f"Registered request {req.id}")

    def get_response(self, id: uuid.UUID) -> BaseModel | None:
        """Get response for a request ID."""
        return self.response.get(id)

    def do_requests(self) -> None:
        """Send all registered requests and wait for responses."""
        if not self.requests:
            logger.warning("No requests registered")
            return

        logger.info(f"Processing {len(self.requests)} requests...")
        self.listen_for_response(threaded=True)
        sleep(1)
        self.send_request()
        logger.info("Requests sent, waiting for responses...")

        if self._listener_thread:
            self._listener_thread.join()

        logger.info("All responses received")

    def listen_for_response(self, threaded: bool = True) -> Generator | None:
        """Listen for responses.

        Args:
            threaded: If True, listen in background thread; otherwise block
        """
        if not threaded:
            return self._listen_for_response()

        if self._listener_thread and self._listener_thread.is_alive():
            raise RuntimeError("Listener thread is already running")

        self._stop_thread_event = Event()
        self._listener_thread = Thread(target=self._listen_for_response, daemon=False)
        self._listener_thread.start()
        logger.debug("Started listener thread")

    def _listen_for_response(self) -> None:
        """Internal response listener (runs in thread or blocking)."""
        logger.info(f"Listening for responses on pattern: {self.base_topic}.data.*.response")

        for id_str, message in self.connector._listen_for_response(topic=f"{self.base_topic}.data.*.response"):
            try:
                id = uuid.UUID(id_str) if id_str else None

                if id and id in self.requests:
                    Klass = self.requests[id].__class__._related_class
                    self.response[id] = Klass.model_validate_json(message)
                    logger.debug(f"Received response for request {id}")

                if len(self.requests) == len(self.response):
                    logger.info("All responses received, stopping listener")
                    return

                if self._stop_thread_event and self._stop_thread_event.is_set():
                    logger.info("Stop event set, stopping listener")
                    return

            except Exception as e:
                logger.error(f"Error processing response: {e}", exc_info=e)

    def send_request(self) -> None:
        """Send all registered requests."""
        for id, request in self.requests.items():
            self.connector.send(
                topic=f"{self.base_topic}.{request.type}.request",
                message=request.model_dump_json(),
                id=str(id),
            )
            logger.debug(f"Sent request {id} of type {request.type}")

    def stop_listening(self) -> None:
        """Stop the listener thread."""
        if self._stop_thread_event:
            self._stop_thread_event.set()
            logger.info("Requested listener to stop")

    def close(self) -> None:
        """Clean up resources."""
        self.stop_listening()
        if self._listener_thread:
            self._listener_thread.join(timeout=5)
        self.connector.close()
        logger.info("Requester closed")
