package com.flinkviz.join;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkviz.common.PageView;
import com.flinkviz.common.Purchase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Interval join: PageView × Purchase (same user) within [-30s, +5min].
 * Emits one record per view → purchase that follows it within the window.
 * Sink: results.joins
 */
public class ViewPurchaseJoinJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class Match {
        public String user_id;
        public String product_id_viewed;
        public String product_id_purchased;
        public long view_ts;
        public long purchase_ts;
        public long delta_ms;
        public double amount_usd;
        public Match() {}
        public Match(String u, String pv, String pp, long vts, long pts, double amt) {
            this.user_id = u;
            this.product_id_viewed = pv;
            this.product_id_purchased = pp;
            this.view_ts = vts;
            this.purchase_ts = pts;
            this.delta_ms = pts - vts;
            this.amount_usd = amt;
        }
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String viewTopic = env("VIEWS_TOPIC", "events.page_views");
        String purchaseTopic = env("PURCHASES_TOPIC", "events.purchases");
        String outTopic = env("JOIN_TOPIC", "results.joins");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<PageView> views = kafkaJsonStream(env, bootstrap, viewTopic, "join-views")
            .map((MapFunction<String, PageView>) json -> MAPPER.readValue(json, PageView.class));

        DataStream<Purchase> purchases = kafkaJsonStream(env, bootstrap, purchaseTopic, "join-purchases")
            .map((MapFunction<String, Purchase>) json -> MAPPER.readValue(json, Purchase.class));

        DataStream<Match> matches = views
            .keyBy(pv -> pv.user_id)
            .intervalJoin(purchases.keyBy(p -> p.user_id))
            .between(Time.seconds(-30), Time.minutes(5))
            .process(new ProcessJoinFunction<PageView, Purchase, Match>() {
                @Override
                public void processElement(PageView v, Purchase p, Context ctx, Collector<Match> out) {
                    out.collect(new Match(v.user_id, v.product_id, p.product_id, v.ts_ms, p.ts_ms, p.amount_usd));
                }
            });

        matches.map(MAPPER::writeValueAsString).sinkTo(
            KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic(outTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
                .build()
        );

        env.execute("view-purchase-join");
    }

    static DataStream<String> kafkaJsonStream(
            StreamExecutionEnvironment env, String bootstrap, String topic, String groupId) {
        KafkaSource<String> src = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        return env.fromSource(src,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((value, recTs) -> readTs(value)),
            topic);
    }

    static long readTs(String json) {
        try { return MAPPER.readTree(json).get("ts_ms").asLong(); }
        catch (Exception e) { return System.currentTimeMillis(); }
    }

    static String env(String k, String d) {
        return System.getenv().getOrDefault(k, d);
    }
}
