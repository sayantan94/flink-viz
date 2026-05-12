import time
from collections import Counter

from src.emitter import EventEmitter


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def poll(self, _t):
        pass

    def flush(self, _t=None):
        pass


def test_emitter_respects_rate():
    fake = FakeProducer()
    em = EventEmitter(producer=fake, num_products=10, zipf_s=1.2)
    em.set_rate(100)
    em.start()
    time.sleep(1.0)
    em.stop()
    n = len(fake.sent)
    assert 60 <= n <= 140, f"expected ~100 events/s, got {n}"


def test_emitter_zipfian_skew():
    fake = FakeProducer()
    em = EventEmitter(producer=fake, num_products=10, zipf_s=1.5)
    em.set_rate(500)
    em.start()
    time.sleep(1.0)
    em.stop()
    keys = Counter(s[1] for s in fake.sent)
    top = keys.most_common(1)[0][1]
    total = sum(keys.values())
    assert top / total > 0.2, f"expected skew, top key share={top/total}"


def test_late_events():
    fake = FakeProducer()
    em = EventEmitter(producer=fake, num_products=5, zipf_s=1.2)
    em.set_rate(500)
    em.set_late_pct(1.0, 5000)  # 100% late, up to 5s back
    em.start()
    time.sleep(0.6)
    em.stop()
    stats = em.stats()
    assert stats["late_sent"] > 50, f"expected many late events, got {stats}"


def test_fraud_burst():
    fake = FakeProducer()
    em = EventEmitter(producer=fake, num_products=5, zipf_s=1.2)
    em.start()
    em.trigger_fraud_burst(rate=30, duration_s=1.0)
    time.sleep(1.2)
    em.stop()
    purchases = [s for s in fake.sent if s[0] == "events.purchases"]
    fraud_keys = set(s[1] for s in purchases)
    # All fraud purchases should come from the SAME synthetic user
    assert any(k.startswith("fraud_user_") for k in fraud_keys), \
        f"expected fraud_user_ key, got {fraud_keys}"
    assert len(purchases) >= 10, f"expected fraud burst, got {len(purchases)}"
