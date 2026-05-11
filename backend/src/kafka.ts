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
