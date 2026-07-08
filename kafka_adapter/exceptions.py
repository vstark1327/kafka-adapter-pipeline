"""Kafka adapter exceptions."""


class KafkaAdapterError(Exception):
    """Base exception for kafka adapter."""

    pass


class ConnectorError(KafkaAdapterError):
    """Raised when connector fails to initialize or communicate."""

    pass

