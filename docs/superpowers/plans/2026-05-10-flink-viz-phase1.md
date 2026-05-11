# Flink-Viz Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the "rails" of the flink-viz playground — a single `docker compose up` brings up Kafka, Flink, an event generator, a backend, and a UI showing a live Top-K product leaderboard end-to-end.

**Architecture:** Python event generator → Kafka → Flink Java job (Top-K via DataStream API) → Kafka results topic → Node.js backend (WebSocket multiplexer) → React UI (leaderboard). Everything orchestrated via Docker Compose.

**Tech Stack:** Apache Kafka 3.7, Apache Flink 1.18, Python 3.11 + FastAPI + confluent-kafka, Java 17 + Maven, Node.js 20 + TypeScript + Fastify + kafkajs, React 18 + Vite + TypeScript + Tailwind, Docker Compose, Vitest, JUnit 5.

---

## File Structure (Phase 1)

```
flink-viz/
├── docker-compose.yml
├── .gitignore
├── README.md
├── event-generator/
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── src/
│   │   ├── __init__.py
│   │   ├── main.py            # FastAPI app + control endpoints
│   │   ├── producer.py        # Kafka producer wrapper
│   │   ├── emitter.py         # Background event emission loop
│   │   └── models.py          # PageView Pydantic model
│   └── tests/
│       ├── test_emitter.py
│       └── test_producer.py
├── flink-jobs/
│   ├── pom.xml                # parent multi-module pom
│   ├── common/
│   │   ├── pom.xml
│   │   └── src/main/java/com/flinkviz/common/
│   │       ├── PageView.java
│   │       ├── PageViewDeserializer.java
│   │       └── TopKResult.java
│   └── topk-products/
│       ├── pom.xml
│       ├── Dockerfile
│       └── src/
│           ├── main/java/com/flinkviz/topk/TopKJob.java
│           └── test/java/com/flinkviz/topk/TopKJobTest.java
├── backend/
│   ├── package.json
│   ├── tsconfig.json
│   ├── Dockerfile
│   ├── src/
│   │   ├── server.ts          # Fastify + WebSocket
│   │   ├── kafka.ts           # consumer setup
│   │   └── types.ts           # shared message types
│   └── tests/
│       └── multiplexer.test.ts
└── ui/
    ├── package.json
    ├── tsconfig.json
    ├── vite.config.ts
    ├── tailwind.config.js
    ├── postcss.config.js
    ├── index.html
    ├── Dockerfile
    └── src/
        ├── main.tsx
        ├── App.tsx
        ├── hooks/useWebSocket.ts
        └── components/
            └── Leaderboard.tsx
```

**Responsibility per file:**
- `docker-compose.yml`: one-shot orchestration.
- `event-generator/src/main.py`: only HTTP routing + lifecycle.
- `event-generator/src/emitter.py`: rate-controlled event production loop.
- `event-generator/src/producer.py`: Kafka producer (only Kafka concerns).
- `flink-jobs/common`: shared types reused by every future job.
- `flink-jobs/topk-products/TopKJob.java`: only Top-K logic.
- `backend/src/kafka.ts`: only Kafka consumer concerns.
- `backend/src/server.ts`: only Fastify + WS concerns.
- `ui/src/hooks/useWebSocket.ts`: only WS lifecycle.
- `ui/src/components/Leaderboard.tsx`: only rendering.

---

## Task 1: Repo scaffolding & git init

**Files:**
- Create: `.gitignore`
- Create: `README.md`

- [ ] **Step 1: Initialize git repo**

Run:
```bash
cd /Users/sayantan/Documents/Workspace/personal-assist/flink-viz
git init -b main
```

Expected: `Initialized empty Git repository...`

- [ ] **Step 2: Create `.gitignore`**

```gitignore
# Java
target/
*.class
*.jar
.idea/
*.iml

# Python
__pycache__/
*.pyc
.venv/
.pytest_cache/
*.egg-info/

# Node
node_modules/
dist/
.vite/

# OS
.DS_Store

# Flink
flink-checkpoints/
flink-savepoints/

# Env
.env
.env.local
```

- [ ] **Step 3: Create minimal `README.md`**

```markdown
# flink-viz

Interactive playground for learning Apache Flink streaming patterns end-to-end.

## Quickstart

```bash
docker compose up
```

Then open http://localhost:5173 — top-K product leaderboard, updated live.

See `docs/superpowers/specs/2026-05-10-flink-viz-design.md` for the full design.
```

- [ ] **Step 4: Commit**

```bash
git add .gitignore README.md docs/
git commit -m "chore: scaffold repo with spec and plan"
```

---

## Task 2: Docker Compose with Kafka + Flink

**Files:**
- Create: `docker-compose.yml`

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on: [kafka]
    ports: ["8080:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092

  jobmanager:
    image: flink:1.18-java17
    ports: ["8081:8081"]
    command: jobmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
        rest.bind-address: 0.0.0.0

  taskmanager:
    image: flink:1.18-java17
    depends_on: [jobmanager]
    command: taskmanager
    scale: 2
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
```

- [ ] **Step 2: Bring up the stack**

Run:
```bash
docker compose up -d zookeeper kafka kafka-ui jobmanager taskmanager
```

Wait ~30s, then check:
```bash
docker compose ps
```

Expected: All 5 services healthy/running.

- [ ] **Step 3: Verify Kafka and Flink reachable**

Run:
```bash
curl -s http://localhost:8081/overview | head
curl -s http://localhost:8080/api/clusters | head
```

Expected: Flink JSON overview returned. Kafka-UI returns cluster JSON.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: docker compose with kafka, kafka-ui, flink"
```

---

## Task 3: Event generator — Kafka producer module

**Files:**
- Create: `event-generator/pyproject.toml`
- Create: `event-generator/src/__init__.py`
- Create: `event-generator/src/models.py`
- Create: `event-generator/src/producer.py`
- Create: `event-generator/tests/test_producer.py`

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "event-generator"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "fastapi==0.110.0",
  "uvicorn[standard]==0.29.0",
  "confluent-kafka==2.3.0",
  "pydantic==2.6.4",
]

[project.optional-dependencies]
dev = ["pytest==8.1.1", "pytest-asyncio==0.23.6"]
```

- [ ] **Step 2: Install dependencies**

Run:
```bash
cd event-generator
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Expected: install succeeds.

- [ ] **Step 3: Write the failing test**

Create `event-generator/tests/test_producer.py`:
```python
from src.producer import build_producer_config

def test_build_producer_config_has_bootstrap():
    cfg = build_producer_config("kafka:29092")
    assert cfg["bootstrap.servers"] == "kafka:29092"
    assert cfg["acks"] == "all"
    assert cfg["enable.idempotence"] is True
```

- [ ] **Step 4: Run test (should fail)**

Run:
```bash
cd event-generator
pytest tests/test_producer.py -v
```

Expected: FAIL with `ModuleNotFoundError: src.producer`.

- [ ] **Step 5: Write `models.py`**

```python
from pydantic import BaseModel

class PageView(BaseModel):
    user_id: str
    product_id: str
    ts_ms: int
```

- [ ] **Step 6: Write `producer.py`**

```python
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
```

- [ ] **Step 7: Run test (should pass)**

Run:
```bash
pytest tests/test_producer.py -v
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add event-generator/
git commit -m "feat(generator): pyproject, models, kafka producer wrapper"
```

---

## Task 4: Event generator — Zipfian emitter loop

**Files:**
- Create: `event-generator/src/emitter.py`
- Create: `event-generator/tests/test_emitter.py`

- [ ] **Step 1: Write the failing test**

Create `event-generator/tests/test_emitter.py`:
```python
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
    em = EventEmitter(producer=fake, topic="events.page_views",
                      num_products=10, zipf_s=1.2)
    em.set_rate(100)
    em.start()
    time.sleep(1.0)
    em.stop()
    n = len(fake.sent)
    assert 70 <= n <= 130, f"expected ~100 events/s, got {n}"

def test_emitter_zipfian_skew():
    fake = FakeProducer()
    em = EventEmitter(producer=fake, topic="t",
                      num_products=10, zipf_s=1.5)
    em.set_rate(500)
    em.start()
    time.sleep(1.0)
    em.stop()
    keys = Counter(s[1] for s in fake.sent)
    top = keys.most_common(1)[0][1]
    total = sum(keys.values())
    assert top / total > 0.2, f"expected skew, top key share={top/total}"
```

- [ ] **Step 2: Run tests (should fail)**

Run:
```bash
pytest tests/test_emitter.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write `emitter.py`**

```python
import json
import random
import threading
import time
from typing import Protocol

class _ProducerLike(Protocol):
    def produce(self, topic: str, key: str, value: bytes) -> None: ...
    def poll(self, t: float) -> None: ...
    def flush(self, t: float = ...) -> None: ...

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
        self._producer.produce(self._topic, key=product,
                               value=json.dumps(payload).encode())
```

- [ ] **Step 4: Run tests (should pass)**

Run:
```bash
pytest tests/test_emitter.py -v
```

Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add event-generator/src/emitter.py event-generator/tests/test_emitter.py
git commit -m "feat(generator): zipfian page-view emitter loop"
```

---

## Task 5: Event generator — FastAPI server

**Files:**
- Create: `event-generator/src/main.py`
- Create: `event-generator/Dockerfile`

- [ ] **Step 1: Write `main.py`**

```python
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel
from src.producer import build_producer
from src.emitter import EventEmitter

class RateBody(BaseModel):
    events_per_sec: float

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
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
    return {"status": "ok", "rate": emitter._rate}

@app.post("/rate")
def set_rate(body: RateBody) -> dict:
    emitter.set_rate(body.events_per_sec)
    return {"rate": body.events_per_sec}
```

- [ ] **Step 2: Write Dockerfile**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
RUN pip install --no-cache-dir .
COPY src ./src
EXPOSE 8000
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 3: Add service to `docker-compose.yml`**

Append under `services:`:

```yaml
  event-generator:
    build: ./event-generator
    depends_on: [kafka]
    ports: ["8000:8000"]
    environment:
      KAFKA_BOOTSTRAP: kafka:29092
      EVENTS_TOPIC: events.page_views
      NUM_PRODUCTS: "200"
      ZIPF_S: "1.2"
```

- [ ] **Step 4: Build and start generator**

Run:
```bash
docker compose up -d --build event-generator
sleep 5
curl -s -X POST http://localhost:8000/rate -H 'content-type: application/json' \
  -d '{"events_per_sec": 50}'
```

Expected: `{"rate":50.0}`.

- [ ] **Step 5: Verify events arrive in Kafka**

Run:
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic events.page_views --from-beginning --max-messages 5
```

Expected: 5 JSON lines with `user_id`, `product_id`, `ts_ms`.

- [ ] **Step 6: Commit**

```bash
git add event-generator/src/main.py event-generator/Dockerfile docker-compose.yml
git commit -m "feat(generator): fastapi server emitting page-views to kafka"
```

---

## Task 6: Flink common module — shared POJOs

**Files:**
- Create: `flink-jobs/pom.xml`
- Create: `flink-jobs/common/pom.xml`
- Create: `flink-jobs/common/src/main/java/com/flinkviz/common/PageView.java`
- Create: `flink-jobs/common/src/main/java/com/flinkviz/common/TopKResult.java`

- [ ] **Step 1: Write parent `flink-jobs/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.flinkviz</groupId>
  <artifactId>flink-jobs</artifactId>
  <version>0.1.0</version>
  <packaging>pom</packaging>

  <modules>
    <module>common</module>
    <module>topk-products</module>
  </modules>

  <properties>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
    <flink.version>1.18.1</flink.version>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>
</project>
```

- [ ] **Step 2: Write `common/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>com.flinkviz</groupId>
    <artifactId>flink-jobs</artifactId>
    <version>0.1.0</version>
  </parent>
  <artifactId>common</artifactId>
  <dependencies>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>2.16.1</version>
    </dependency>
  </dependencies>
</project>
```

- [ ] **Step 3: Write `PageView.java`**

```java
package com.flinkviz.common;

public class PageView {
    public String user_id;
    public String product_id;
    public long ts_ms;

    public PageView() {}
    public PageView(String user_id, String product_id, long ts_ms) {
        this.user_id = user_id; this.product_id = product_id; this.ts_ms = ts_ms;
    }
}
```

- [ ] **Step 4: Write `TopKResult.java`**

```java
package com.flinkviz.common;

import java.util.List;

public class TopKResult {
    public long window_start_ms;
    public long window_end_ms;
    public List<Entry> top;

    public static class Entry {
        public String product_id;
        public long count;
        public Entry() {}
        public Entry(String product_id, long count) {
            this.product_id = product_id; this.count = count;
        }
    }

    public TopKResult() {}
    public TopKResult(long start, long end, List<Entry> top) {
        this.window_start_ms = start; this.window_end_ms = end; this.top = top;
    }
}
```

- [ ] **Step 5: Build common module**

Run:
```bash
cd flink-jobs
mvn -pl common -am install -DskipTests
```

Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add flink-jobs/pom.xml flink-jobs/common/
git commit -m "feat(flink): common module with PageView and TopKResult"
```

---

## Task 7: Top-K Flink job — failing test first

**Files:**
- Create: `flink-jobs/topk-products/pom.xml`
- Create: `flink-jobs/topk-products/src/test/java/com/flinkviz/topk/TopKJobTest.java`

- [ ] **Step 1: Write `topk-products/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>com.flinkviz</groupId>
    <artifactId>flink-jobs</artifactId>
    <version>0.1.0</version>
  </parent>
  <artifactId>topk-products</artifactId>

  <dependencies>
    <dependency>
      <groupId>com.flinkviz</groupId>
      <artifactId>common</artifactId>
      <version>0.1.0</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-clients</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-kafka</artifactId>
      <version>3.1.0-1.18</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-test-utils</artifactId>
      <version>${flink.version}</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>5.10.2</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.1</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>com.flinkviz.topk.TopKJob</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>3.2.5</version>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Write the failing test**

Create `flink-jobs/topk-products/src/test/java/com/flinkviz/topk/TopKJobTest.java`:

```java
package com.flinkviz.topk;

import com.flinkviz.common.PageView;
import com.flinkviz.common.TopKResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class TopKJobTest {

    static final CopyOnWriteArrayList<TopKResult> COLLECTED = new CopyOnWriteArrayList<>();

    public static class CollectingSink implements SinkFunction<TopKResult> {
        @Override public void invoke(TopKResult value, Context ctx) {
            COLLECTED.add(value);
        }
    }

    @Test
    void topThreeProductsAcrossOneMinuteWindow() throws Exception {
        COLLECTED.clear();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<PageView> input = new ArrayList<>();
        long base = 0L;
        for (int i = 0; i < 10; i++) input.add(new PageView("u" + i, "A", base + i));
        for (int i = 0; i < 6; i++) input.add(new PageView("u" + i, "B", base + i));
        for (int i = 0; i < 3; i++) input.add(new PageView("u" + i, "C", base + i));
        for (int i = 0; i < 1; i++) input.add(new PageView("u" + i, "D", base + i));
        // marker far in the future to advance watermark and close the window
        input.add(new PageView("u-flush", "Z", base + 120_000L));

        DataStream<PageView> source = env.fromCollection(input)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<PageView>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner((e, ts) -> e.ts_ms));

        TopKJob.buildPipeline(source, 3, 60_000L).addSink(new CollectingSink());
        env.execute();

        assertFalse(COLLECTED.isEmpty(), "expected at least one TopK window result");
        TopKResult first = COLLECTED.get(0);
        assertEquals(3, first.top.size());
        assertEquals("A", first.top.get(0).product_id);
        assertEquals(10L, first.top.get(0).count);
        assertEquals("B", first.top.get(1).product_id);
        assertEquals(6L, first.top.get(1).count);
        assertEquals("C", first.top.get(2).product_id);
        assertEquals(3L, first.top.get(2).count);
    }
}
```

- [ ] **Step 3: Run test (should fail to compile)**

Run:
```bash
cd flink-jobs
mvn -pl topk-products test
```

Expected: BUILD FAILURE — `cannot find symbol: TopKJob.buildPipeline`.

- [ ] **Step 4: Commit the failing test**

```bash
git add flink-jobs/topk-products/pom.xml flink-jobs/topk-products/src/test/
git commit -m "test(topk): failing test for top-3 windowed pipeline"
```

---

## Task 8: Top-K Flink job — implement pipeline

**Files:**
- Create: `flink-jobs/topk-products/src/main/java/com/flinkviz/topk/TopKJob.java`

- [ ] **Step 1: Write `TopKJob.java`**

```java
package com.flinkviz.topk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkviz.common.PageView;
import com.flinkviz.common.TopKResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopKJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static DataStream<TopKResult> buildPipeline(
            DataStream<PageView> source, int k, long windowMs) {
        return source
            .keyBy(pv -> pv.product_id)
            .window(TumblingEventTimeWindows.of(Time.milliseconds(windowMs)))
            .aggregate(new CountAgg(), new TagWindow())
            .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(windowMs)))
            .process(new TopKWindow(k));
    }

    public static class CountAgg implements AggregateFunction<PageView, Long, Long> {
        public Long createAccumulator() { return 0L; }
        public Long add(PageView v, Long acc) { return acc + 1; }
        public Long getResult(Long acc) { return acc; }
        public Long merge(Long a, Long b) { return a + b; }
    }

    public static class TagWindow extends ProcessWindowFunction<Long, Tuple3, String, TimeWindow> {
        @Override
        public void process(String productId, Context ctx, Iterable<Long> counts, Collector<Tuple3> out) {
            long count = counts.iterator().next();
            out.collect(new Tuple3(ctx.window().getStart(), ctx.window().getEnd(), productId, count));
        }
    }

    public static class Tuple3 {
        public long start; public long end; public String productId; public long count;
        public Tuple3() {}
        public Tuple3(long s, long e, String p, long c) { start=s; end=e; productId=p; count=c; }
    }

    public static class TopKWindow extends ProcessAllWindowFunction<Tuple3, TopKResult, TimeWindow> {
        private final int k;
        public TopKWindow(int k) { this.k = k; }
        @Override
        public void process(Context ctx, Iterable<Tuple3> elements, Collector<TopKResult> out) {
            List<Tuple3> list = new ArrayList<>();
            elements.forEach(list::add);
            list.sort(Comparator.comparingLong((Tuple3 t) -> t.count).reversed());
            List<TopKResult.Entry> top = new ArrayList<>();
            for (int i = 0; i < Math.min(k, list.size()); i++) {
                top.add(new TopKResult.Entry(list.get(i).productId, list.get(i).count));
            }
            out.collect(new TopKResult(ctx.window().getStart(), ctx.window().getEnd(), top));
        }
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:29092");
        String inputTopic = System.getenv().getOrDefault("INPUT_TOPIC", "events.page_views");
        String outputTopic = System.getenv().getOrDefault("OUTPUT_TOPIC", "results.topk");
        int k = Integer.parseInt(System.getenv().getOrDefault("TOPK_K", "10"));
        long windowMs = Long.parseLong(System.getenv().getOrDefault("WINDOW_MS", "10000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inputTopic)
            .setGroupId("topk-products")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<PageView> pageViews = env
            .fromSource(kafkaSource,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((value, ts) -> {
                        try {
                            return MAPPER.readTree(value).get("ts_ms").asLong();
                        } catch (Exception e) { return ts; }
                    }),
                "page-views")
            .map(json -> MAPPER.readValue(json, PageView.class));

        DataStream<TopKResult> topk = buildPipeline(pageViews, k, windowMs);

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(outputTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .build();

        topk.map(MAPPER::writeValueAsString).sinkTo(sink);

        env.execute("topk-products");
    }
}
```

- [ ] **Step 2: Run unit test (should pass)**

Run:
```bash
cd flink-jobs
mvn -pl topk-products test
```

Expected: `Tests run: 1, Failures: 0, Errors: 0`.

- [ ] **Step 3: Package the job jar**

Run:
```bash
mvn -pl topk-products package -DskipTests
ls topk-products/target/*.jar
```

Expected: `topk-products/target/topk-products-0.1.0.jar` (uberjar via shade).

- [ ] **Step 4: Commit**

```bash
git add flink-jobs/topk-products/src/main/
git commit -m "feat(topk): implement keyed window + top-k pipeline"
```

---

## Task 9: Submit Top-K job to the Flink cluster

**Files:** None new.

- [ ] **Step 1: Copy jar into jobmanager and submit**

Run:
```bash
JM=$(docker compose ps -q jobmanager)
docker cp flink-jobs/topk-products/target/topk-products-0.1.0.jar "$JM":/opt/flink/topk.jar
docker compose exec -T jobmanager flink run -d /opt/flink/topk.jar
```

Expected: `Job has been submitted with JobID <uuid>`.

- [ ] **Step 2: Verify job is running**

Run:
```bash
curl -s http://localhost:8081/jobs/overview | python3 -m json.tool
```

Expected: a job with `state: "RUNNING"`.

- [ ] **Step 3: Verify results land in Kafka**

Run:
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic results.topk --from-beginning --max-messages 2
```

Expected: 2 JSON lines with `window_start_ms`, `window_end_ms`, `top` array of `{product_id, count}`.

- [ ] **Step 4: Commit (no code change, just verification — skip)**

No commit needed.

---

## Task 10: Backend — package scaffolding

**Files:**
- Create: `backend/package.json`
- Create: `backend/tsconfig.json`
- Create: `backend/src/types.ts`

- [ ] **Step 1: Write `package.json`**

```json
{
  "name": "flink-viz-backend",
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "build": "tsc",
    "start": "node dist/server.js",
    "dev": "tsx watch src/server.ts",
    "test": "vitest run"
  },
  "dependencies": {
    "fastify": "4.26.2",
    "@fastify/websocket": "10.0.1",
    "kafkajs": "2.2.4"
  },
  "devDependencies": {
    "@types/node": "20.11.30",
    "tsx": "4.7.1",
    "typescript": "5.4.3",
    "vitest": "1.4.0"
  }
}
```

- [ ] **Step 2: Write `tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "Bundler",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "outDir": "dist"
  },
  "include": ["src/**/*", "tests/**/*"]
}
```

- [ ] **Step 3: Write `src/types.ts`**

```typescript
export type TopKEntry = { product_id: string; count: number };
export type TopKMessage = {
  window_start_ms: number;
  window_end_ms: number;
  top: TopKEntry[];
};
export type WsMessage =
  | { type: "topk"; payload: TopKMessage };
```

- [ ] **Step 4: Install deps**

Run:
```bash
cd backend
npm install
```

Expected: `package-lock.json` created, no errors.

- [ ] **Step 5: Commit**

```bash
git add backend/package.json backend/package-lock.json backend/tsconfig.json backend/src/types.ts
git commit -m "chore(backend): scaffold typescript + fastify project"
```

---

## Task 11: Backend — Kafka consumer + WebSocket

**Files:**
- Create: `backend/src/kafka.ts`
- Create: `backend/src/server.ts`
- Create: `backend/Dockerfile`

- [ ] **Step 1: Write `kafka.ts`**

```typescript
import { Kafka, type Consumer } from "kafkajs";

export type Listener = (topic: string, value: string) => void;

export async function startConsumer(
  brokers: string[],
  topics: string[],
  listener: Listener,
): Promise<Consumer> {
  const kafka = new Kafka({ clientId: "flink-viz-backend", brokers });
  const consumer = kafka.consumer({ groupId: "flink-viz-backend" });
  await consumer.connect();
  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (message.value) listener(topic, message.value.toString());
    },
  });
  return consumer;
}
```

- [ ] **Step 2: Write `server.ts`**

```typescript
import Fastify from "fastify";
import websocket from "@fastify/websocket";
import { startConsumer } from "./kafka.js";
import type { WsMessage } from "./types.js";

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? "kafka:29092").split(",");
const TOPK_TOPIC = process.env.TOPK_TOPIC ?? "results.topk";

const app = Fastify({ logger: true });
await app.register(websocket);

const clients = new Set<import("ws").WebSocket>();

app.get("/health", async () => ({ status: "ok", clients: clients.size }));

app.get("/ws", { websocket: true }, (sock) => {
  clients.add(sock);
  sock.on("close", () => clients.delete(sock));
});

function broadcast(msg: WsMessage) {
  const data = JSON.stringify(msg);
  for (const c of clients) {
    if (c.readyState === c.OPEN) c.send(data);
  }
}

await startConsumer(KAFKA_BROKERS, [TOPK_TOPIC], (topic, value) => {
  if (topic === TOPK_TOPIC) {
    try {
      const payload = JSON.parse(value);
      broadcast({ type: "topk", payload });
    } catch (e) {
      app.log.warn({ err: e }, "bad topk payload");
    }
  }
});

await app.listen({ port: 3000, host: "0.0.0.0" });
```

- [ ] **Step 3: Write `Dockerfile`**

```dockerfile
FROM node:20-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
RUN npx tsc
EXPOSE 3000
CMD ["node", "dist/server.js"]
```

- [ ] **Step 4: Add backend service to `docker-compose.yml`**

Append:
```yaml
  backend:
    build: ./backend
    depends_on: [kafka]
    ports: ["3000:3000"]
    environment:
      KAFKA_BROKERS: kafka:29092
      TOPK_TOPIC: results.topk
```

- [ ] **Step 5: Build and start**

Run:
```bash
docker compose up -d --build backend
sleep 5
curl -s http://localhost:3000/health
```

Expected: `{"status":"ok","clients":0}`.

- [ ] **Step 6: Sanity-check WebSocket receives topk**

Run (in one shell):
```bash
npx -y wscat -c ws://localhost:3000/ws
```

Expected: every ~10s, a JSON line `{"type":"topk","payload":{...}}` arrives.

Press Ctrl+C to exit `wscat`.

- [ ] **Step 7: Commit**

```bash
git add backend/src/kafka.ts backend/src/server.ts backend/Dockerfile docker-compose.yml
git commit -m "feat(backend): kafka consumer + websocket broadcast of topk"
```

---

## Task 12: UI — Vite + React + Tailwind scaffold

**Files:**
- Create: `ui/package.json`
- Create: `ui/tsconfig.json`
- Create: `ui/vite.config.ts`
- Create: `ui/index.html`
- Create: `ui/tailwind.config.js`
- Create: `ui/postcss.config.js`
- Create: `ui/src/main.tsx`
- Create: `ui/src/index.css`

- [ ] **Step 1: Write `package.json`**

```json
{
  "name": "flink-viz-ui",
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite --host 0.0.0.0 --port 5173",
    "build": "tsc -b && vite build",
    "preview": "vite preview --host 0.0.0.0",
    "test": "vitest run"
  },
  "dependencies": {
    "react": "18.2.0",
    "react-dom": "18.2.0"
  },
  "devDependencies": {
    "@types/react": "18.2.74",
    "@types/react-dom": "18.2.24",
    "@vitejs/plugin-react": "4.2.1",
    "autoprefixer": "10.4.19",
    "postcss": "8.4.38",
    "tailwindcss": "3.4.3",
    "typescript": "5.4.3",
    "vite": "5.2.6",
    "vitest": "1.4.0"
  }
}
```

- [ ] **Step 2: Write `tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "jsx": "react-jsx",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true
  },
  "include": ["src"]
}
```

- [ ] **Step 3: Write `vite.config.ts`**

```typescript
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
});
```

- [ ] **Step 4: Write `index.html`**

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <title>flink-viz</title>
  </head>
  <body class="bg-zinc-950 text-zinc-100">
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 5: Write `tailwind.config.js`**

```javascript
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: { extend: {} },
  plugins: [],
};
```

- [ ] **Step 6: Write `postcss.config.js`**

```javascript
export default {
  plugins: { tailwindcss: {}, autoprefixer: {} },
};
```

- [ ] **Step 7: Write `src/index.css`**

```css
@tailwind base;
@tailwind components;
@tailwind utilities;
```

- [ ] **Step 8: Write `src/main.tsx`**

```tsx
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
```

- [ ] **Step 9: Install deps**

Run:
```bash
cd ui
npm install
```

Expected: no errors.

- [ ] **Step 10: Commit (App.tsx comes in next task)**

```bash
git add ui/package.json ui/package-lock.json ui/tsconfig.json \
        ui/vite.config.ts ui/index.html ui/tailwind.config.js \
        ui/postcss.config.js ui/src/index.css ui/src/main.tsx
git commit -m "chore(ui): vite + react + tailwind scaffold"
```

---

## Task 13: UI — WebSocket hook (TDD)

**Files:**
- Create: `ui/src/hooks/useWebSocket.ts`
- Create: `ui/src/hooks/useWebSocket.test.ts`

- [ ] **Step 1: Write the failing test**

Create `ui/src/hooks/useWebSocket.test.ts`:

```typescript
import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { useWebSocket } from "./useWebSocket";

class FakeWS {
  static last: FakeWS | null = null;
  onmessage: ((e: MessageEvent) => void) | null = null;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  readyState = 0;
  constructor(public url: string) { FakeWS.last = this; }
  close() { this.readyState = 3; this.onclose?.(); }
}

beforeEach(() => {
  vi.stubGlobal("WebSocket", FakeWS);
});

describe("useWebSocket", () => {
  it("collects messages by type", () => {
    const { result } = renderHook(() => useWebSocket("ws://x/ws"));
    act(() => {
      FakeWS.last!.onmessage!(new MessageEvent("message", {
        data: JSON.stringify({ type: "topk", payload: { top: [] } }),
      }));
    });
    expect(result.current.lastByType.topk).toEqual({ top: [] });
  });
});
```

Add `@testing-library/react` to devDeps:

Run:
```bash
cd ui
npm i -D @testing-library/react@14.2.2 jsdom@24.0.0
```

Add to `vite.config.ts` (replace existing file):
```typescript
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  test: { environment: "jsdom", globals: true } as any,
});
```

- [ ] **Step 2: Run test (should fail)**

Run:
```bash
npm test
```

Expected: FAIL — `Cannot find module './useWebSocket'`.

- [ ] **Step 3: Write `useWebSocket.ts`**

```typescript
import { useEffect, useState } from "react";

type AnyMsg = { type: string; payload: unknown };

export function useWebSocket(url: string) {
  const [lastByType, setLastByType] = useState<Record<string, unknown>>({});
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    const ws = new WebSocket(url);
    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onmessage = (e) => {
      try {
        const m = JSON.parse(e.data) as AnyMsg;
        setLastByType((prev) => ({ ...prev, [m.type]: m.payload }));
      } catch {
        /* ignore non-json frames */
      }
    };
    return () => ws.close();
  }, [url]);

  return { lastByType, connected };
}
```

- [ ] **Step 4: Run test (should pass)**

Run:
```bash
npm test
```

Expected: 1 PASS.

- [ ] **Step 5: Commit**

```bash
git add ui/src/hooks/ ui/vite.config.ts ui/package.json ui/package-lock.json
git commit -m "feat(ui): useWebSocket hook + tests"
```

---

## Task 14: UI — Leaderboard component & App

**Files:**
- Create: `ui/src/components/Leaderboard.tsx`
- Create: `ui/src/App.tsx`

- [ ] **Step 1: Write `Leaderboard.tsx`**

```tsx
type Entry = { product_id: string; count: number };
type Props = {
  windowStartMs?: number;
  windowEndMs?: number;
  top?: Entry[];
};

export function Leaderboard({ windowStartMs, windowEndMs, top }: Props) {
  if (!top) {
    return (
      <div className="p-6 text-zinc-500">Waiting for first window…</div>
    );
  }
  const startStr = windowStartMs ? new Date(windowStartMs).toLocaleTimeString() : "";
  const endStr = windowEndMs ? new Date(windowEndMs).toLocaleTimeString() : "";
  return (
    <div className="p-6">
      <div className="text-sm text-zinc-500 mb-3">
        Window: {startStr} → {endStr}
      </div>
      <ol className="space-y-2">
        {top.map((e, i) => (
          <li
            key={e.product_id}
            className="flex justify-between items-center
                       bg-zinc-900 rounded-lg px-4 py-3"
          >
            <div className="flex items-center gap-3">
              <span className="text-zinc-500 w-6">{i + 1}</span>
              <span className="font-mono">{e.product_id}</span>
            </div>
            <span className="font-mono text-emerald-400">{e.count}</span>
          </li>
        ))}
      </ol>
    </div>
  );
}
```

- [ ] **Step 2: Write `App.tsx`**

```tsx
import { useWebSocket } from "./hooks/useWebSocket";
import { Leaderboard } from "./components/Leaderboard";

type TopKMsg = {
  window_start_ms: number;
  window_end_ms: number;
  top: { product_id: string; count: number }[];
};

export default function App() {
  const wsUrl = `ws://${window.location.hostname}:3000/ws`;
  const { lastByType, connected } = useWebSocket(wsUrl);
  const topk = lastByType.topk as TopKMsg | undefined;

  return (
    <div className="min-h-screen flex flex-col">
      <header className="px-6 py-4 border-b border-zinc-800
                         flex justify-between items-center">
        <h1 className="text-lg font-semibold">flink-viz · Top-K Products</h1>
        <span
          className={connected ? "text-emerald-400" : "text-rose-400"}
        >
          {connected ? "● live" : "● disconnected"}
        </span>
      </header>
      <main className="flex-1">
        <Leaderboard
          windowStartMs={topk?.window_start_ms}
          windowEndMs={topk?.window_end_ms}
          top={topk?.top}
        />
      </main>
    </div>
  );
}
```

- [ ] **Step 3: Add UI service to `docker-compose.yml`**

Append:
```yaml
  ui:
    build: ./ui
    depends_on: [backend]
    ports: ["5173:5173"]
```

Create `ui/Dockerfile`:
```dockerfile
FROM node:20-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
EXPOSE 5173
CMD ["npm", "run", "dev"]
```

- [ ] **Step 4: Bring everything up**

Run:
```bash
docker compose up -d --build ui
sleep 5
curl -sI http://localhost:5173 | head -1
```

Expected: `HTTP/1.1 200 OK`.

- [ ] **Step 5: Set generator rate and open the UI**

Run:
```bash
curl -s -X POST http://localhost:8000/rate \
  -H 'content-type: application/json' -d '{"events_per_sec": 200}'
open http://localhost:5173
```

Expected: Browser opens. After ~10s, the leaderboard populates with the top products and updates each window.

- [ ] **Step 6: Commit**

```bash
git add ui/src/components/ ui/src/App.tsx ui/Dockerfile docker-compose.yml
git commit -m "feat(ui): leaderboard component wired to topk websocket"
```

---

## Task 15: End-to-end smoke test

**Files:**
- Create: `scripts/smoke.sh`

- [ ] **Step 1: Write `scripts/smoke.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "==> bringing stack down for a clean start"
docker compose down -v

echo "==> starting infra"
docker compose up -d zookeeper kafka kafka-ui jobmanager taskmanager
sleep 30

echo "==> starting generator + backend + ui"
docker compose up -d --build event-generator backend ui
sleep 15

echo "==> packaging and submitting topk job"
( cd flink-jobs && mvn -pl topk-products -am package -DskipTests )
JM=$(docker compose ps -q jobmanager)
docker cp flink-jobs/topk-products/target/topk-products-0.1.0.jar "$JM":/opt/flink/topk.jar
docker compose exec -T jobmanager flink run -d /opt/flink/topk.jar
sleep 5

echo "==> turning on event rate"
curl -fsS -X POST http://localhost:8000/rate \
  -H 'content-type: application/json' \
  -d '{"events_per_sec": 200}' > /dev/null

echo "==> waiting 30s for first window to close + arrive"
sleep 30

echo "==> reading one topk result"
docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic results.topk --from-beginning --max-messages 1 --timeout-ms 15000

echo "==> smoke OK"
```

- [ ] **Step 2: Make executable and run**

Run:
```bash
chmod +x scripts/smoke.sh
./scripts/smoke.sh
```

Expected: ends with `==> smoke OK` and a printed JSON top-K result.

- [ ] **Step 3: Commit**

```bash
git add scripts/smoke.sh
git commit -m "test: end-to-end smoke script for phase 1"
```

---

## Phase 1 Done Criteria

- [ ] `docker compose up` brings the stack up.
- [ ] `./scripts/smoke.sh` exits 0 and prints a Top-K result.
- [ ] http://localhost:5173 shows the live leaderboard.
- [ ] http://localhost:8081 shows the Top-K job running.
- [ ] `mvn -pl topk-products test` passes.
- [ ] `cd backend && npm test` (when added) and `cd ui && npm test` pass.

## Notes for Future Phases

- Phase 2 will introduce `flink-telemetry-lib` (instrumented operator wrapper) and the internals pane (watermark timeline, checkpoint strip, DAG).
- Phase 3 fans out to the remaining 6 patterns. Each new pattern reuses `flink-jobs/common` and `backend/src/types.ts`.
- Phase 4 layers in the per-pattern study docs and a "tour mode."
