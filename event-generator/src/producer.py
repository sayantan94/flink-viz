from confluent_kafka import Producer


def build_producer_config(bootstrap: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
    }


def build_producer(bootstrap: str) -> Producer:
    return Producer(build_producer_config(bootstrap))
