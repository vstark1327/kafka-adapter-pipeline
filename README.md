# Kafka Adapter Pipeline

This is a simple service to serve as an adapter pipeline for receiving, processing and sending messages via kafka to respective kafka-topics.

## Usage

```python
adapter = JobAdapter(
    job_topic="INPUT_TOPIC",
    response_topic="OUTPUT_TOPIC",   
    job_processor=job_processor_method,
    message_parser=message_parser_method,
    broker_address="kafka_broker_address"

try:
    adapter.listen()                 
finally:
    adapter.close()
)
```
