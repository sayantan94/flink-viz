export type TopKEntry = { product_id: string; count: number };

export type TopKMessage = {
  window_start_ms: number;
  window_end_ms: number;
  top: TopKEntry[];
};

export type WsMessage = { type: "topk"; payload: TopKMessage };
