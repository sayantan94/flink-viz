package com.flinkviz.common;

public class PageView {
    public String user_id;
    public String product_id;
    public long ts_ms;

    public PageView() {}

    public PageView(String user_id, String product_id, long ts_ms) {
        this.user_id = user_id;
        this.product_id = product_id;
        this.ts_ms = ts_ms;
    }
}
