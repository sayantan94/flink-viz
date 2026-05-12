package com.flinkviz.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Purchase {
    public String user_id;
    public String product_id;
    public double amount_usd;
    public long ts_ms;
    public boolean fraud_hint;

    public Purchase() {}

    public Purchase(String user_id, String product_id, double amount_usd, long ts_ms) {
        this.user_id = user_id;
        this.product_id = product_id;
        this.amount_usd = amount_usd;
        this.ts_ms = ts_ms;
        this.fraud_hint = false;
    }
}
