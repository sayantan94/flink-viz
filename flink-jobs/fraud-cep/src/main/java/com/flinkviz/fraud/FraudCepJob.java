package com.flinkviz.fraud;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkviz.common.Purchase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * CEP pattern: 3+ purchases from the same user within 10 seconds = fraud alert.
 * Sink: results.fraud
 */
public class FraudCepJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class FraudAlert {
        public String user_id;
        public int purchase_count;
        public double total_amount_usd;
        public long first_ts;
        public long last_ts;
        public long span_ms;
        public FraudAlert() {}
        public FraudAlert(String u, int n, double amt, long first, long last) {
            this.user_id = u;
            this.purchase_count = n;
            this.total_amount_usd = amt;
            this.first_ts = first;
            this.last_ts = last;
            this.span_ms = last - first;
        }
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String inputTopic = env("PURCHASES_TOPIC", "events.purchases");
        String outTopic = env("FRAUD_TOPIC", "results.fraud");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        KafkaSource<String> src = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inputTopic)
            .setGroupId("fraud-cep")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<Purchase> purchases = env
            .fromSource(src,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((value, ts) -> readTs(value)),
                "purchases")
            .map(json -> MAPPER.readValue(json, Purchase.class));

        // Pattern: any purchase, then ≥2 more within 10s by same user
        Pattern<Purchase, ?> pattern = Pattern.<Purchase>begin("first")
            .where(new SimpleCondition<Purchase>() {
                @Override public boolean filter(Purchase v) { return true; }
            })
            .followedBy("more")
            .where(new SimpleCondition<Purchase>() {
                @Override public boolean filter(Purchase v) { return true; }
            })
            .timesOrMore(2)
            .within(Time.seconds(10));

        PatternStream<Purchase> ps = CEP.pattern(
            purchases.keyBy(p -> p.user_id),
            pattern);

        DataStream<FraudAlert> alerts = ps.process(new PatternProcessFunction<Purchase, FraudAlert>() {
            @Override
            public void processMatch(Map<String, List<Purchase>> match, Context ctx, Collector<FraudAlert> out) {
                List<Purchase> all = new java.util.ArrayList<>();
                for (List<Purchase> ps2 : match.values()) all.addAll(ps2);
                if (all.isEmpty()) return;
                double total = 0;
                long first = Long.MAX_VALUE, last = Long.MIN_VALUE;
                for (Purchase p : all) {
                    total += p.amount_usd;
                    first = Math.min(first, p.ts_ms);
                    last = Math.max(last, p.ts_ms);
                }
                out.collect(new FraudAlert(all.get(0).user_id, all.size(), total, first, last));
            }
        });

        alerts.map(MAPPER::writeValueAsString).sinkTo(
            KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic(outTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
                .build()
        );

        env.execute("fraud-cep");
    }

    static long readTs(String json) {
        try { return MAPPER.readTree(json).get("ts_ms").asLong(); }
        catch (Exception e) { return System.currentTimeMillis(); }
    }

    static String env(String k, String d) {
        return System.getenv().getOrDefault(k, d);
    }
}
