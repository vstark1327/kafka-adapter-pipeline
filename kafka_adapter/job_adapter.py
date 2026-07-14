"""Job processing adapter for Kafka."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Callable

from pydantic import BaseModel

from .connector import KafkaConnector, KAFKA_BOOTSTRAP
from .exceptions import ConnectorError
from .models import JobResponse

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOGGING_LEVEL", "INFO"))


class JobAdapter:
    """Process jobs from Kafka topics and send responses."""

    def __init__(
        self,
        job_topic: str | None = None,
        response_topic: str | list[str] | None = None,
        job_processor: Callable | None = None,
        message_parser: Callable | None = None,
        broker_address: str = KAFKA_BOOTSTRAP,
    ):
        """Initialize job adapter.

        Args:
            job_topic: Topic to consume jobs from
            response_topic: Topic(s) to publish responses to
            job_class: Pydantic model class for job validation
            job_processor: Callable that processes jobs and returns results
            message_parser: Callable that parses incoming messages
            broker_address: Kafka bootstrap servers
        """
        self.job_topic = job_topic
        self.response_topic = response_topic
        self.job_processor = job_processor
        self.message_parser = message_parser
        self.connector = KafkaConnector(broker_address)
        self._running = False
        logger.info(f"Initialized JobAdapter: job_topic={job_topic}, response_topic={response_topic}")

    def respond(self, key: str, result: BaseModel) -> None:
        """Send a response to the response topic.

        Args:
            key: Message key
            result: Response data (Pydantic model)
        """
        if not self.response_topic:
            logger.warning("No response topic configured, skipping response")
            return

        topics = self.response_topic if isinstance(self.response_topic, list) else [self.response_topic]

        result = result.model_dump_json() if isinstance(result, BaseModel) else json.dumps(result)

        for topic in topics:
            try:
                self.connector.send(topic=topic, message=result, id=key)
                logger.info(f"Sent response to {topic}")
            except Exception as e:
                logger.error(f"Failed to send response to {topic}: {e}", exc_info=e)

    def listen(self) -> None:
        """Listen for jobs and process them.

        Blocks indefinitely, consuming jobs from job_topic and calling job_processor.
        """
        if not self.job_topic:
            raise ConnectorError("job_topic must be set to listen")

        if not self.job_processor:
            raise ConnectorError("job_processor must be set to listen")

        self._running = True
        logger.info(f"Starting job listener on {self.job_topic}")

        while self._running:
            try:
                for key, message in self.connector._listen_for_response(self.job_topic):
                    if not self._running:
                        logger.info("Stopping job listener")
                        break

                    try:
                        if key is None:
                            logger.info(f"received no key....going to next message!")
                            continue
                        logger.info(f"Processing job {key}")
                        logger.info(f"Received msg: {message}")
                        
                        job = self.parse_job(message)
                        result = self.job_processor(job)
                        logger.info(f"Result: {result}")
                        logger.info(f"Sending response for job {key} to {self.response_topic}")
                        self.respond(key, result)

                    except Exception as e:
                        logger.error(f"Error processing job {key}: {e}", exc_info=e)

                        if self.response_topic and self.job_class:
                            
                            error_response = JobResponse(
                                job_id=key or "unknown",
                                status="failed",
                                error=str(e),
                            )
                            self.respond(key, error_response)

            except KeyboardInterrupt:
                logger.info("Job listener interrupted by user")
                self._running = False
                break
            except Exception as e:
                logger.error(f"Job listener error, retrying in 5s: {e}", exc_info=e)
                time.sleep(5)
        
        logger.info("Job listener stopped")
    

    def parse_job(self, message: str):
        """Parse incoming job message into Pydantic model."""
        if self.message_parser:
            message = self.message_parser(message)
        return message

    def stop(self) -> None:
        """Stop listening for jobs."""
        self._running = False
        logger.info("Requested job listener to stop")

    def close(self) -> None:
        """Close connections."""
        self.stop()
        self.connector.close()
        logger.info("JobAdapter closed")
