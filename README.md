# flink-viz

Interactive playground for learning Apache Flink streaming patterns end-to-end.

Phase 1 ships: a live Top-K product leaderboard fed by a real Flink job consuming a synthetic Kafka stream, with a React UI at http://localhost:5173.

## Prerequisites

Installed via Homebrew:

```bash
brew install openjdk@17 maven kafka apache-flink
```

Plus Node.js 20+ and Python 3.11+ (system or pyenv).

## Quickstart

```bash
./scripts/start-all.sh   # starts kafka, flink, generator, backend, ui
./scripts/submit-job.sh  # builds and submits the top-K Flink job
open http://localhost:5273
```

Tear down:

```bash
./scripts/stop-all.sh
```

## Layout

- `event-generator/` — Python service producing synthetic e-commerce events to Kafka, with rate / late-event / spike controls.
- `flink-jobs/` — Java + Maven multi-module Flink jobs. Phase 1 has `topk-products`.
- `backend/` — Node.js + TypeScript WebSocket multiplexer between Kafka/Flink and the UI.
- `ui/` — React + Vite + Tailwind frontend.
- `infra/` — local scripts for managing native Kafka + Flink processes.
- `docs/superpowers/` — design spec and implementation plan.

See `docs/superpowers/specs/2026-05-10-flink-viz-design.md` for full design.
