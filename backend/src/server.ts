import Fastify from "fastify";
import websocket from "@fastify/websocket";
import cors from "@fastify/cors";
import type { WebSocket } from "ws";

import { startConsumer } from "./kafka.js";
import type { WsMessage } from "./types.js";

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const TOPK_TOPIC = process.env.TOPK_TOPIC ?? "results.topk";
const PORT = Number(process.env.PORT ?? 3000);

const app = Fastify({ logger: true });
await app.register(cors, { origin: true });
await app.register(websocket);

const clients = new Set<WebSocket>();

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

await app.listen({ port: PORT, host: "0.0.0.0" });
