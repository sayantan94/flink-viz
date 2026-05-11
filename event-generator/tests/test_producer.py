from src.producer import build_producer_config


def test_build_producer_config_has_bootstrap():
    cfg = build_producer_config("localhost:9092")
    assert cfg["bootstrap.servers"] == "localhost:9092"
    assert cfg["acks"] == "all"
    assert cfg["enable.idempotence"] is True
