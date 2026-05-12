import Fastify from "fastify";
import websocket from "@fastify/websocket";
import cors from "@fastify/cors";
import type { WebSocket } from "ws";

import { startConsumer } from "./kafka.js";
import { startFlinkPoller } from "./flinkPoller.js";

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const TOPK_TOPIC = process.env.TOPK_TOPIC ?? "results.topk";
const FLINK_URL = process.env.FLINK_URL ?? "http://localhost:8081";
const PORT = Number(process.env.PORT ?? 3000);

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

await startConsumer(KAFKA_BROKERS, [TOPK_TOPIC], (topic, value) => {
  if (topic === TOPK_TOPIC) {
    try {
      broadcast("topk", JSON.parse(value));
    } catch (e) {
      app.log.warn({ err: e }, "bad topk payload");
    }
  }
});

startFlinkPoller(FLINK_URL, broadcast);

await app.listen({ port: PORT, host: "0.0.0.0" });
