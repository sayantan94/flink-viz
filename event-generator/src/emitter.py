import json
import random
import threading
import time


class EventEmitter:
    def __init__(self, producer, topic: str, num_products: int, zipf_s: float):
        self._producer = producer
        self._topic = topic
        self._weights = self._zipf_weights(num_products, zipf_s)
        self._product_ids = [f"product_{i}" for i in range(num_products)]
        self._rate = 0.0
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    @staticmethod
    def _zipf_weights(n: int, s: float) -> list[float]:
        raw = [1.0 / (i ** s) for i in range(1, n + 1)]
        total = sum(raw)
        return [w / total for w in raw]

    def set_rate(self, events_per_sec: float) -> None:
        self._rate = max(0.0, events_per_sec)

    def current_rate(self) -> float:
        return self._rate

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.is_set():
            rate = self._rate
            if rate <= 0:
                time.sleep(0.05)
                continue
            interval = 1.0 / rate
            self._emit_one()
            self._producer.poll(0)
            time.sleep(interval)

    def _emit_one(self) -> None:
        product = random.choices(self._product_ids, self._weights, k=1)[0]
        payload = {
            "user_id": f"user_{random.randint(0, 9999)}",
            "product_id": product,
            "ts_ms": int(time.time() * 1000),
        }
        self._producer.produce(
            self._topic, key=product, value=json.dumps(payload).encode()
        )
