# Flink-Viz: An Interview-Prep Playground for Apache Flink

**Date:** 2026-05-10
**Status:** Approved (brainstorm)
**Owner:** Sayantan

## Goal

Build a hands-on, visual playground for Apache Flink so that the author can confidently use Flink in system-design interviews — particularly streaming patterns (Top-K, real-time dashboards, windowed aggregations, joins, sessions, fraud/anomaly detection, stateful recovery).

The system must:
- Run real Flink (not a toy) on a local cluster.
- Use a mix of Flink SQL and the DataStream API (Java).
- Drive everything from a single coherent event domain (e-commerce).
- Provide a custom UI that visualizes both:
  - **Internals** — operator DAG, watermarks, checkpoints, state — beyond what the built-in Flink Web UI shows.
  - **Outputs** — per-pattern dashboards (Top-K leaderboard, time-series counters, fraud alerts, etc.).

The non-goal is production-readiness. This is a learning artifact.

## Streaming Patterns Covered

1. **Top-K Trending Products** — heavy hitters, two implementations:
   - Flink SQL `ROW_NUMBER() OVER (PARTITION BY window ORDER BY count DESC)`.
   - DataStream API: keyed state + `KeyedProcessFunction` with event-time timers.
2. **Windowed Aggregations** — tumbling and sliding windows over revenue / GMV / events per minute. Demonstrates watermarks and late-data handling.
3. **Stream-Stream Joins** — interval join between `page_views` and `purchases` for the impression → conversion funnel.
4. **Session Windows** — gap-based user sessions; emit session-level metrics on close.
5. **Fraud / Anomaly Detection** — Flink CEP pattern (rapid purchases from new account) plus a threshold-based anomaly job.
6. **Real-Time Dashboard Counters** — running counts/gauges fed to UI for global metrics.
7. **Stateful Recovery Demo** — keyed counter with savepoint, deliberate TaskManager kill, recovery, and exactly-once verification.

## Architecture

```
┌─────────────────────┐    ┌──────────────┐    ┌──────────────────┐
│  Event Generator    │───▶│    Kafka     │───▶│  Flink Cluster   │
│  (synthetic         │    │  (input      │    │  (JobManager +   │
│   e-commerce)       │    │   topics)    │    │   TaskManager)   │
└─────────────────────┘    └──────────────┘    └────────┬─────────┘
                                                         │
                                  ┌──────────────────────┼──────────────┐
                                  ▼                      ▼              ▼
                          ┌──────────────┐      ┌──────────────┐  ┌──────────┐
                          │ Output Kafka │      │ Flink REST   │  │ Custom   │
                          │ topics       │      │ /metrics API │  │ telemetry│
                          │ (per pattern)│      │              │  │ side-out │
                          └──────┬───────┘      └──────┬───────┘  └─────┬────┘
                                 │                     │                │
                                 └─────────┬───────────┴────────────────┘
                                           ▼
                                  ┌──────────────────┐
                                  │  Backend (Node)  │
                                  │  WebSocket       │
                                  └────────┬─────────┘
                                           ▼
                                  ┌──────────────────┐
                                  │  React UI        │
                                  │  Internals │ Out │
                                  └──────────────────┘
```

**Telemetry path is key**: the system reads operator state and watermarks two ways — (a) Flink's REST API for DAG/checkpoint metadata and (b) a custom side-output stream from instrumented operators for fine-grained per-key watermarks, state size, and late-drop counts. The UI joins both.

## Components

### 1. `event-generator/` — Python service
- Produces `page_views`, `add_to_cart`, `purchases` to Kafka. (Sessions are derived inside Flink, not emitted by the generator.)
- Zipfian product-id distribution so Top-K is non-trivial.
- REST control plane (FastAPI):
  - `POST /rate { events_per_sec }`
  - `POST /late-events { percentage, max_delay_ms }`
  - `POST /spike { duration_s, multiplier }`
  - `POST /fraud-burst` — emits a synthetic fraud pattern (rapid purchases from new accounts).
- Deterministic mode toggle (fixed seed) for reproducible demos.

### 2. `flink-jobs/` — Java + Maven multi-module
- One module per pattern (see Streaming Patterns Covered).
- `topk-products/` ships both a DataStream-API version and a Flink SQL version side-by-side so the user can read both and decide which to present in interviews.
- `common/` module: shared POJOs (`PageView`, `Purchase`, `Session`), watermark strategies, JSON serde. Depends on `flink-telemetry-lib` for telemetry emission.
- Each job is independently submittable to the cluster.

### 3. `flink-telemetry-lib/` — small Java library
- Operator wrapper that emits a `TelemetryEvent` side stream on every watermark advance and every N records:
  ```json
  {
    "job_id": "topk-products",
    "operator": "KeyedProcessFunction",
    "subtask": 2,
    "key": "product_42",
    "watermark_ms": 1715300000000,
    "state_size_bytes": 12345,
    "late_drops": 3,
    "ts": 1715300000123
  }
  ```
- Sinks to `flink.telemetry` Kafka topic.
- All jobs use this library uniformly so the UI doesn't need per-job adapters.

### 4. `backend/` — Node.js + TypeScript (Fastify)
- Kafka consumers for (a) pattern outputs and (b) `flink.telemetry`.
- Polls Flink REST (`/jobs`, `/jobs/<id>/vertices`, `/jobs/<id>/checkpoints`) at 1 Hz.
- Single WebSocket endpoint multiplexing message types: `topk`, `dashboard`, `fraud`, `sessions`, `join`, `telemetry`, `dag`, `checkpoints`.
- Proxies `event-generator` control endpoints so the UI talks to one origin.

### 5. `ui/` — React + Vite + TypeScript
- **Left pane (Internals):**
  - **DAG viewer** (React Flow) — vertices color-coded by throughput; backpressure shown as edge thickness.
  - **Watermark timeline** — horizontal time axis; one row per partition; advancing watermarks; red dots for late events below the line.
  - **Checkpoint strip** — most recent N checkpoints with size, duration, and barrier-flow animation.
  - **State inspector** — heatmap of keyed state size per key.
- **Right pane (Output):**
  - Pattern selector dropdown.
  - Per-pattern view: leaderboard / time-series / alert feed / session list / join results / recovery controls.
- **Top bar (Controls):** generator knobs (rate, late %, spike, fraud burst), and scenario buttons (trigger savepoint, kill TaskManager).
- Styling: Tailwind. Charts: Recharts. Graph: React Flow.

### 6. `infra/` — Docker Compose
- Services: Zookeeper, Kafka, Kafka UI (port 8080), Flink JobManager + 2 TaskManagers (Flink Web UI on 8081), event-generator, backend (port 3000), ui (port 5173).
- Single command bring-up: `docker compose up`.
- Volumes for Flink savepoints (so the recovery demo survives restarts).

### 7. `docs/patterns/` — Study material
- One markdown file per pattern.
- Each contains:
  - The interview prompt it answers (e.g., "Design real-time trending products").
  - The Flink SQL version with annotations.
  - The DataStream API version with annotations.
  - "What to point at in the UI" — which pane and what to do.
  - Common interviewer follow-ups and answers.
  - Gotchas (late events, key skew, state explosion, exactly-once nuances).

## Data Flow (Top-K example, end-to-end)

1. `event-generator` emits `page_views` to Kafka topic `events.page_views` with a Zipfian product-id distribution.
2. `topk-products` Flink job consumes the topic, keys by `product_id`, applies a sliding window (e.g., 1-minute window, 10-second slide), counts events, then emits a `Top-K` per window close.
3. The instrumented operator emits a `TelemetryEvent` per watermark advance to `flink.telemetry`.
4. Top-K results land in Kafka topic `results.topk`.
5. `backend` reads both topics, also polls Flink REST for DAG + checkpoints.
6. UI WebSocket receives multiplexed updates and renders the leaderboard (right pane) and watermark/state telemetry (left pane) simultaneously.
7. User clicks "Inject late events 20%" — generator backdates 20% of events by up to 30s. UI's watermark line stops advancing for affected partitions; red dots appear; the leaderboard shows the late-arrival impact.

## Phasing

The full system is large; phasing it makes the first useful artifact reachable in days, not weeks. Each phase is independently runnable and useful.

**Phase 1 — Scaffold (the rails):**
- Docker Compose with Kafka + Flink + UI shell.
- Event generator with `page_views` and rate knob.
- One pattern end-to-end: Top-K (DataStream version) + leaderboard UI.
- Backend WebSocket plumbing.
- Outcome: you can see a live leaderboard.

**Phase 2 — Internals viz:**
- `flink-telemetry-lib` + watermark timeline + checkpoint strip + DAG.
- Add late-event injection and traffic spike knobs.
- Outcome: you can *see* watermarks advance, checkpoints fire, late events drop.

**Phase 3 — Pattern fan-out:**
- Add windowed dashboard, joins, sessions, fraud, recovery.
- Pattern selector in UI; one job per pattern.
- Outcome: full coverage.

**Phase 4 — Study material:**
- Per-pattern docs written with screenshots of the UI.
- "Tour mode" — guided walkthrough that toggles knobs and highlights what to look at.

## Error Handling

- **Generator down:** UI shows "no data" banner. Backend WebSocket keeps connection.
- **Flink job fail:** Backend marks the pattern as "stalled." UI renders last-known data with a stale badge.
- **Kafka backpressure:** Generator self-throttles when topic lag exceeds threshold.
- **Backend disconnect:** UI auto-reconnects with exponential backoff; shows reconnecting toast.
- **Recovery demo expectations:** Killing a TaskManager should *not* lose state — the demo's whole point is to verify exactly-once via the savepoint/restore loop.

## Testing

- **Flink jobs:** unit tests with Flink MiniCluster (`flink-test-utils`). Verify outputs for canned input streams. Watermark and late-event behavior gets explicit tests.
- **Telemetry lib:** unit test that wrapped operators emit telemetry on every watermark advance.
- **Generator:** unit test the event distributions; integration test that knobs change rates.
- **Backend:** unit test multiplexer; integration test against a real Kafka container.
- **UI:** Vitest for hooks/state; Playwright smoke test for the leaderboard rendering after a generated event.
- **End-to-end:** Docker Compose-based smoke that starts everything, runs the generator for 30s, asserts the Top-K leaderboard has entries.

## Tech Choices

| Layer | Choice | Why |
|---|---|---|
| Event source | Apache Kafka 3.x | Industry-standard Flink source; offers partitioning and replay. |
| Stream engine | Apache Flink 1.18+ | The thing we're learning. |
| Job language | Java 17 (DataStream) + Flink SQL | Mix matches the design goal — interview-readable SQL plus deep DataStream control. |
| Build | Maven multi-module | Standard for Flink Java projects. |
| Generator | Python 3.11 + FastAPI + confluent-kafka | Fast to iterate; rich knobs. |
| Backend | Node.js 20 + TypeScript + Fastify + kafkajs | Tight WebSocket loop, type sharing with UI. |
| UI | React 18 + Vite + TypeScript + Tailwind + React Flow + Recharts | Modern, fast HMR, the right visualization primitives. |
| Orchestration | Docker Compose | Single-command local cluster. |

## Open Questions

None blocking. Below are explicit design choices made to avoid ambiguity:
- Single Flink cluster, all jobs co-tenant (not separate clusters per pattern).
- Telemetry library is a thin wrapper, not a Flink runtime patch.
- Backend in Node, not Python — to share TypeScript types with the UI.
- All UI state is server-driven via WebSocket; no client-side polling.

## Success Criteria

- All 7 patterns run on a single `docker compose up`.
- UI shows live internals (watermarks, checkpoints, state) and outputs (leaderboards, dashboards) side-by-side.
- Each pattern has a study doc the user can read and recite from.
- The user can, off-camera, give a coherent 5-minute Flink whiteboard answer for any of the 7 patterns after one week of using this system.
