import json
import random
import threading
import time


PAGE_VIEWS_TOPIC = "events.page_views"
PURCHASES_TOPIC = "events.purchases"


class EventEmitter:
    """
    Emits an e-commerce stream with tunable knobs.

    - Zipfian product-id skew so top-K is non-trivial.
    - `set_rate(n)`        – events/sec for page_views
    - `set_late_pct(p, d)` – emit p% of events with timestamps backdated up to d ms
    - `set_spike(d, m)`    – multiply rate by m for d seconds
    - `set_purchase_rate(n)` – purchases/sec (lower than views)
    - `trigger_fraud_burst()` – 1 fast burst of purchases from a 'new' user
    """

    def __init__(self, producer, num_products: int, zipf_s: float):
        self._producer = producer
        self._weights = self._zipf_weights(num_products, zipf_s)
        self._product_ids = [f"product_{i}" for i in range(num_products)]
        self._rate = 0.0
        self._purchase_rate = 0.0
        self._late_pct = 0.0
        self._late_max_ms = 0
        self._spike_until = 0.0
        self._spike_mult = 1.0
        self._fraud_user_until = 0.0
        self._fraud_user_id = ""
        self._fraud_target_product = ""
        self._fraud_rate = 0
        self._stats = {"sent": 0, "late_sent": 0, "purchases_sent": 0, "fraud_sent": 0}
        self._thread: threading.Thread | None = None
        self._purchase_thread: threading.Thread | None = None
        self._stop = threading.Event()

    @staticmethod
    def _zipf_weights(n: int, s: float) -> list[float]:
        raw = [1.0 / (i ** s) for i in range(1, n + 1)]
        total = sum(raw)
        return [w / total for w in raw]

    def set_rate(self, events_per_sec: float) -> None:
        self._rate = max(0.0, events_per_sec)

    def set_purchase_rate(self, events_per_sec: float) -> None:
        self._purchase_rate = max(0.0, events_per_sec)

    def set_late_pct(self, pct: float, max_delay_ms: int) -> None:
        self._late_pct = max(0.0, min(1.0, pct))
        self._late_max_ms = max(0, max_delay_ms)

    def set_spike(self, duration_s: float, multiplier: float) -> None:
        self._spike_until = time.time() + duration_s
        self._spike_mult = max(1.0, multiplier)

    def trigger_fraud_burst(self, rate: int = 20, duration_s: float = 3.0) -> None:
        self._fraud_user_id = f"fraud_user_{random.randint(1000, 9999)}"
        self._fraud_target_product = random.choices(
            self._product_ids, self._weights, k=1
        )[0]
        self._fraud_rate = rate
        self._fraud_user_until = time.time() + duration_s

    def stats(self) -> dict:
        now = time.time()
        return {
            **self._stats,
            "rate": self._rate,
            "purchase_rate": self._purchase_rate,
            "late_pct": self._late_pct,
            "late_max_ms": self._late_max_ms,
            "spike_active": now < self._spike_until,
            "spike_until": self._spike_until,
            "fraud_active": now < self._fraud_user_until,
        }

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_views, daemon=True)
        self._thread.start()
        self._purchase_thread = threading.Thread(
            target=self._run_purchases, daemon=True
        )
        self._purchase_thread.start()

    def stop(self) -> None:
        self._stop.set()
        for t in (self._thread, self._purchase_thread):
            if t:
                t.join(timeout=2)

    def _effective_rate(self) -> float:
        if time.time() < self._spike_until:
            return self._rate * self._spike_mult
        return self._rate

    def _run_views(self) -> None:
        while not self._stop.is_set():
            rate = self._effective_rate()
            if rate <= 0:
                time.sleep(0.05)
                continue
            interval = 1.0 / rate
            self._emit_view()
            self._producer.poll(0)
            time.sleep(interval)

    def _run_purchases(self) -> None:
        while not self._stop.is_set():
            if time.time() < self._fraud_user_until and self._fraud_rate > 0:
                # Fraud burst: rapid-fire purchases from one user
                self._emit_purchase(
                    user_id=self._fraud_user_id,
                    product_id=self._fraud_target_product,
                    fraud=True,
                )
                time.sleep(1.0 / self._fraud_rate)
                continue
            if self._purchase_rate <= 0:
                time.sleep(0.1)
                continue
            self._emit_purchase()
            self._producer.poll(0)
            time.sleep(1.0 / self._purchase_rate)

    def _pick_ts(self) -> tuple[int, bool]:
        now_ms = int(time.time() * 1000)
        if (
            self._late_pct > 0
            and self._late_max_ms > 0
            and random.random() < self._late_pct
        ):
            delay = random.randint(1, self._late_max_ms)
            return now_ms - delay, True
        return now_ms, False

    def _emit_view(self) -> None:
        product = random.choices(self._product_ids, self._weights, k=1)[0]
        ts_ms, late = self._pick_ts()
        payload = {
            "user_id": f"user_{random.randint(0, 9999)}",
            "product_id": product,
            "ts_ms": ts_ms,
            "late": late,
        }
        self._producer.produce(
            PAGE_VIEWS_TOPIC, key=product, value=json.dumps(payload).encode()
        )
        self._stats["sent"] += 1
        if late:
            self._stats["late_sent"] += 1

    def _emit_purchase(
        self,
        user_id: str | None = None,
        product_id: str | None = None,
        fraud: bool = False,
    ) -> None:
        product = product_id or random.choices(
            self._product_ids, self._weights, k=1
        )[0]
        user = user_id or f"user_{random.randint(0, 9999)}"
        amount = round(10 + random.random() * 90, 2)
        payload = {
            "user_id": user,
            "product_id": product,
            "amount_usd": amount,
            "ts_ms": int(time.time() * 1000),
            "fraud_hint": fraud,
        }
        self._producer.produce(
            PURCHASES_TOPIC, key=user, value=json.dumps(payload).encode()
        )
        self._stats["purchases_sent"] += 1
        if fraud:
            self._stats["fraud_sent"] += 1
