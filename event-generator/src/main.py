import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.emitter import EventEmitter
from src.producer import build_producer


class RateBody(BaseModel):
    events_per_sec: float


class PurchaseRateBody(BaseModel):
    events_per_sec: float


class LateBody(BaseModel):
    percentage: float
    max_delay_ms: int


class SpikeBody(BaseModel):
    duration_s: float = 5.0
    multiplier: float = 5.0


class FraudBody(BaseModel):
    rate: int = 20
    duration_s: float = 3.0


BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
NUM_PRODUCTS = int(os.environ.get("NUM_PRODUCTS", "200"))
ZIPF_S = float(os.environ.get("ZIPF_S", "1.2"))

producer = build_producer(BOOTSTRAP)
emitter = EventEmitter(producer, NUM_PRODUCTS, ZIPF_S)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    emitter.start()
    yield
    emitter.stop()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/stats")
def stats() -> dict:
    return emitter.stats()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", **emitter.stats()}


@app.post("/rate")
def set_rate(body: RateBody) -> dict:
    emitter.set_rate(body.events_per_sec)
    return {"rate": body.events_per_sec}


@app.post("/purchase-rate")
def set_purchase_rate(body: PurchaseRateBody) -> dict:
    emitter.set_purchase_rate(body.events_per_sec)
    return {"purchase_rate": body.events_per_sec}


@app.post("/late-events")
def set_late(body: LateBody) -> dict:
    emitter.set_late_pct(body.percentage / 100.0, body.max_delay_ms)
    return {"late_pct": body.percentage, "max_delay_ms": body.max_delay_ms}


@app.post("/spike")
def trigger_spike(body: SpikeBody) -> dict:
    emitter.set_spike(body.duration_s, body.multiplier)
    return {"spike_until": emitter.stats()["spike_until"]}


@app.post("/fraud-burst")
def trigger_fraud(body: FraudBody | None = None) -> dict:
    b = body or FraudBody()
    emitter.trigger_fraud_burst(b.rate, b.duration_s)
    return {"fraud_active": True, "rate": b.rate, "duration_s": b.duration_s}
