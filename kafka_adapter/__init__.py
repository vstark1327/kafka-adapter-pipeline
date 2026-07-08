"""Kafka adapter service package."""

__version__ = "0.1.0"

from kafka_adapter.kafka_adapter.connector import KafkaConnector, Requester
from kafka_adapter.kafka_adapter.exceptions import (
    KafkaAdapterError,
    ConnectorError,
)
from kafka_adapter.kafka_adapter.job_adapter import JobAdapter
from kafka_adapter.kafka_adapter.models import (
    JobResponse,
)

__all__ = [
    # Connector classes
    "KafkaConnector",
    "Requester",
    "JobAdapter",
    # Exceptions
    "KafkaAdapterError",
    "ConnectorError",
    # Models
    "JobResponse",
]
