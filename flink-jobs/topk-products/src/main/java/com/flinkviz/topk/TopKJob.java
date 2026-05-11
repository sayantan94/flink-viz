package com.flinkviz.topk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkviz.common.PageView;
import com.flinkviz.common.TopKResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopKJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class WindowedCount {
        public long window_start_ms;
        public long window_end_ms;
        public String product_id;
        public long count;

        public WindowedCount() {}

        public WindowedCount(long s, long e, String p, long c) {
            this.window_start_ms = s;
            this.window_end_ms = e;
            this.product_id = p;
            this.count = c;
        }
    }

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
        @Override public Long createAccumulator() { return 0L; }
        @Override public Long add(PageView v, Long acc) { return acc + 1; }
        @Override public Long getResult(Long acc) { return acc; }
        @Override public Long merge(Long a, Long b) { return a + b; }
    }

    public static class TagWindow extends ProcessWindowFunction<Long, WindowedCount, String, TimeWindow> {
        @Override
        public void process(String productId, Context ctx, Iterable<Long> counts,
                            Collector<WindowedCount> out) {
            long count = counts.iterator().next();
            out.collect(new WindowedCount(
                ctx.window().getStart(), ctx.window().getEnd(), productId, count));
        }
    }

    public static class TopKWindow extends ProcessAllWindowFunction<WindowedCount, TopKResult, TimeWindow> {
        private final int k;
        public TopKWindow(int k) { this.k = k; }

        @Override
        public void process(Context ctx, Iterable<WindowedCount> elements,
                            Collector<TopKResult> out) {
            List<WindowedCount> list = new ArrayList<>();
            elements.forEach(list::add);
            list.sort(Comparator.comparingLong((WindowedCount w) -> w.count).reversed());
            List<TopKResult.Entry> top = new ArrayList<>();
            for (int i = 0; i < Math.min(k, list.size()); i++) {
                top.add(new TopKResult.Entry(list.get(i).product_id, list.get(i).count));
            }
            out.collect(new TopKResult(ctx.window().getStart(), ctx.window().getEnd(), top));
        }
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
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

        WatermarkStrategy<String> watermarkStrategy =
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((value, ts) -> {
                    try {
                        return MAPPER.readTree(value).get("ts_ms").asLong();
                    } catch (Exception e) {
                        return ts;
                    }
                });

        DataStream<PageView> pageViews = env
            .fromSource(kafkaSource, watermarkStrategy, "page-views")
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
