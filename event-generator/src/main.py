import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from src.emitter import EventEmitter
from src.producer import build_producer


class RateBody(BaseModel):
    events_per_sec: float


BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("EVENTS_TOPIC", "events.page_views")
NUM_PRODUCTS = int(os.environ.get("NUM_PRODUCTS", "200"))
ZIPF_S = float(os.environ.get("ZIPF_S", "1.2"))

producer = build_producer(BOOTSTRAP)
emitter = EventEmitter(producer, TOPIC, NUM_PRODUCTS, ZIPF_S)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    emitter.start()
    yield
    emitter.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "rate": emitter.current_rate()}


@app.post("/rate")
def set_rate(body: RateBody) -> dict:
    emitter.set_rate(body.events_per_sec)
    return {"rate": body.events_per_sec}
