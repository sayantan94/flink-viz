import Fastify from "fastify";
import websocket from "@fastify/websocket";
import cors from "@fastify/cors";
import type { WebSocket } from "ws";

import { startConsumer } from "./kafka.js";
import { startFlinkPoller } from "./flinkPoller.js";

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const FLINK_URL = process.env.FLINK_URL ?? "http://localhost:8081";
const GENERATOR_URL = process.env.GENERATOR_URL ?? "http://localhost:8000";
const PORT = Number(process.env.PORT ?? 3000);

const TOPIC_TO_TYPE: Record<string, string> = {
  "results.topk": "topk",
  "results.windows.tumbling": "tumbling",
  "results.windows.sliding": "sliding",
  "results.windows.sessions": "session",
  "results.joins": "join",
  "results.fraud": "fraud",
};

const app = Fastify({ logger: true });
await app.register(cors, { origin: true });
await app.register(websocket);

const clients = new Set<WebSocket>();

app.get("/health", async () => ({ status: "ok", clients: clients.size }));

const lastByType = new Map<string, unknown>();

app.get("/ws", { websocket: true }, (sock) => {
  clients.add(sock);
  // Send the last value of every type so a new client sees current state immediately.
  for (const [type, payload] of lastByType.entries()) {
    sock.send(JSON.stringify({ type, payload }));
  }
  sock.on("close", () => clients.delete(sock));
});

function broadcast(type: string, payload: unknown) {
  lastByType.set(type, payload);
  const data = JSON.stringify({ type, payload });
  for (const c of clients) {
    if (c.readyState === c.OPEN) c.send(data);
  }
}

// Recent buffers for stream-like message types (join, session, fraud).
const recents: Record<string, unknown[]> = {
  join: [],
  session: [],
  fraud: [],
};
const RECENT_CAP = 50;

await startConsumer(KAFKA_BROKERS, Object.keys(TOPIC_TO_TYPE), (topic, value) => {
  const type = TOPIC_TO_TYPE[topic];
  if (!type) return;
  let payload: unknown;
  try {
    payload = JSON.parse(value);
  } catch {
    return;
  }
  if (type in recents) {
    const arr = recents[type];
    arr.unshift({ ...(payload as object), _recv_ts: Date.now() });
    if (arr.length > RECENT_CAP) arr.length = RECENT_CAP;
    broadcast(type, { recent: [...arr] });
  } else {
    broadcast(type, payload);
  }
});

startFlinkPoller(FLINK_URL, broadcast);

// Poll generator stats and broadcast.
setInterval(async () => {
  try {
    const res = await fetch(`${GENERATOR_URL}/stats`);
    if (res.ok) broadcast("generator", await res.json());
  } catch {
    /* ignore */
  }
}, 1000);

// Proxy generator-control endpoints so the UI can hit the same origin.
app.post<{ Body: unknown }>("/control/:action", async (req, reply) => {
  const action = (req.params as { action: string }).action;
  const allow = new Set(["rate", "purchase-rate", "late-events", "spike", "fraud-burst"]);
  if (!allow.has(action)) return reply.code(404).send({ error: "unknown action" });
  const res = await fetch(`${GENERATOR_URL}/${action}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(req.body ?? {}),
  });
  return reply.code(res.status).send(await res.json());
});

await app.listen({ port: PORT, host: "0.0.0.0" });
