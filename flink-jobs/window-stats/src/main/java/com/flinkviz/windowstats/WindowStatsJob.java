package com.flinkviz.windowstats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkviz.common.PageView;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * One job that fans page_views into three windowed streams:
 *   - tumbling 10s          → results.windows.tumbling
 *   - sliding 30s slide 10s → results.windows.sliding
 *   - sessions 30s gap      → results.windows.sessions (keyed by user)
 */
public class WindowStatsJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String inputTopic = env("INPUT_TOPIC", "events.page_views");
        String tumblingTopic = env("TUMBLING_TOPIC", "results.windows.tumbling");
        String slidingTopic = env("SLIDING_TOPIC", "results.windows.sliding");
        String sessionsTopic = env("SESSIONS_TOPIC", "results.windows.sessions");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        KafkaSource<String> src = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inputTopic)
            .setGroupId("window-stats")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<PageView> pageViews = env
            .fromSource(src,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((value, ts) -> readTs(value)),
                "page-views")
            .map(json -> MAPPER.readValue(json, PageView.class));

        // Tumbling: 10s buckets, count globally
        pageViews
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new Counter(), new TumblingTagger())
            .map(MAPPER::writeValueAsString)
            .sinkTo(sink(bootstrap, tumblingTopic));

        // Sliding: 30s windows sliding every 10s, count globally
        pageViews
            .windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
            .aggregate(new Counter(), new SlidingTagger())
            .map(MAPPER::writeValueAsString)
            .sinkTo(sink(bootstrap, slidingTopic));

        // Sessions: per-user, 30s inactivity gap
        pageViews
            .keyBy(pv -> pv.user_id)
            .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
            .process(new SessionAgg())
            .map(MAPPER::writeValueAsString)
            .sinkTo(sink(bootstrap, sessionsTopic));

        env.execute("window-stats");
    }

    static long readTs(String json) {
        try { return MAPPER.readTree(json).get("ts_ms").asLong(); }
        catch (Exception e) { return System.currentTimeMillis(); }
    }

    static KafkaSink<String> sink(String bootstrap, String topic) {
        return KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .build();
    }

    static String env(String k, String d) {
        return System.getenv().getOrDefault(k, d);
    }

    public static class Counter implements AggregateFunction<PageView, Long, Long> {
        public Long createAccumulator() { return 0L; }
        public Long add(PageView v, Long acc) { return acc + 1; }
        public Long getResult(Long acc) { return acc; }
        public Long merge(Long a, Long b) { return a + b; }
    }

    public static class WindowOut {
        public String kind;
        public long window_start_ms;
        public long window_end_ms;
        public long count;
        public String user_id;
        public WindowOut() {}
        public WindowOut(String kind, long s, long e, long c) {
            this.kind = kind; this.window_start_ms = s; this.window_end_ms = e; this.count = c;
        }
    }

    public static class TumblingTagger
            extends org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction<Long, WindowOut, TimeWindow> {
        @Override
        public void process(Context ctx, Iterable<Long> counts, Collector<WindowOut> out) {
            long c = counts.iterator().next();
            out.collect(new WindowOut("tumbling", ctx.window().getStart(), ctx.window().getEnd(), c));
        }
    }

    public static class SlidingTagger
            extends org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction<Long, WindowOut, TimeWindow> {
        @Override
        public void process(Context ctx, Iterable<Long> counts, Collector<WindowOut> out) {
            long c = counts.iterator().next();
            out.collect(new WindowOut("sliding", ctx.window().getStart(), ctx.window().getEnd(), c));
        }
    }

    public static class SessionAgg extends ProcessWindowFunction<PageView, WindowOut, String, TimeWindow> {
        @Override
        public void process(String user, Context ctx, Iterable<PageView> events, Collector<WindowOut> out) {
            long c = 0;
            for (PageView ignored : events) c++;
            WindowOut w = new WindowOut("session", ctx.window().getStart(), ctx.window().getEnd(), c);
            w.user_id = user;
            out.collect(w);
        }
    }
}
