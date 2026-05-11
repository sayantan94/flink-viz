package com.flinkviz.common;

import java.util.List;

public class TopKResult {
    public long window_start_ms;
    public long window_end_ms;
    public List<Entry> top;

    public static class Entry {
        public String product_id;
        public long count;

        public Entry() {}

        public Entry(String product_id, long count) {
            this.product_id = product_id;
            this.count = count;
        }
    }

    public TopKResult() {}

    public TopKResult(long window_start_ms, long window_end_ms, List<Entry> top) {
        this.window_start_ms = window_start_ms;
        this.window_end_ms = window_end_ms;
        this.top = top;
    }
}
