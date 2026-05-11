package com.flinkviz.topk;

import com.flinkviz.common.PageView;
import com.flinkviz.common.TopKResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class TopKJobTest {

    static final CopyOnWriteArrayList<TopKResult> COLLECTED = new CopyOnWriteArrayList<>();

    public static class CollectingSink implements SinkFunction<TopKResult> {
        @Override
        public void invoke(TopKResult value, Context ctx) {
            COLLECTED.add(value);
        }
    }

    @Test
    void topThreeProductsAcrossOneMinuteWindow() throws Exception {
        COLLECTED.clear();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<PageView> input = new ArrayList<>();
        long base = 0L;
        for (int i = 0; i < 10; i++) input.add(new PageView("u" + i, "A", base + i));
        for (int i = 0; i < 6; i++) input.add(new PageView("u" + i, "B", base + i));
        for (int i = 0; i < 3; i++) input.add(new PageView("u" + i, "C", base + i));
        for (int i = 0; i < 1; i++) input.add(new PageView("u" + i, "D", base + i));
        input.add(new PageView("u-flush", "Z", base + 120_000L));

        DataStream<PageView> source = env.fromCollection(input)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<PageView>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner((e, ts) -> e.ts_ms));

        TopKJob.buildPipeline(source, 3, 60_000L).addSink(new CollectingSink());
        env.execute();

        assertFalse(COLLECTED.isEmpty(), "expected at least one TopK window result");
        TopKResult first = COLLECTED.get(0);
        assertEquals(3, first.top.size());
        assertEquals("A", first.top.get(0).product_id);
        assertEquals(10L, first.top.get(0).count);
        assertEquals("B", first.top.get(1).product_id);
        assertEquals(6L, first.top.get(1).count);
        assertEquals("C", first.top.get(2).product_id);
        assertEquals(3L, first.top.get(2).count);
    }
}
