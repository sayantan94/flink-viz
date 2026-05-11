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
    em = EventEmitter(
        producer=fake, topic="events.page_views", num_products=10, zipf_s=1.2
    )
    em.set_rate(100)
    em.start()
    time.sleep(1.0)
    em.stop()
    n = len(fake.sent)
    assert 60 <= n <= 140, f"expected ~100 events/s, got {n}"


def test_emitter_zipfian_skew():
    fake = FakeProducer()
    em = EventEmitter(producer=fake, topic="t", num_products=10, zipf_s=1.5)
    em.set_rate(500)
    em.start()
    time.sleep(1.0)
    em.stop()
    keys = Counter(s[1] for s in fake.sent)
    top = keys.most_common(1)[0][1]
    total = sum(keys.values())
    assert top / total > 0.2, f"expected skew, top key share={top/total}"
