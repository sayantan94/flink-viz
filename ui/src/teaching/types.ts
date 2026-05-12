export type TopKEntry = { product_id: string; count: number };
export type TopKMsg = {
  window_start_ms: number;
  window_end_ms: number;
  top: TopKEntry[];
};

export type DagVertex = {
  id: string;
  name: string;
  parallelism: number;
  status: string;
  metrics: Record<string, number | undefined>;
};
export type DagMsg = {
  job_id: string;
  job_name: string;
  state: string;
  vertices: DagVertex[];
  edges: { source: string; target: string }[];
};

export type CheckpointSummary = {
  id: number;
  status: string;
  trigger_ts: number;
  duration_ms: number;
  state_size: number;
  is_savepoint: boolean;
};
export type CheckpointsMsg = {
  job_id: string;
  counts: { completed: number; failed: number; in_progress: number };
  history: CheckpointSummary[];
};

export type WatermarkSubtask = {
  vertex_id: string;
  vertex_name: string;
  subtask: number;
  watermark_ms: number;
};
export type WatermarkMsg = {
  job_id: string;
  ts: number;
  subtasks: WatermarkSubtask[];
};

export type WindowOut = {
  kind: "tumbling" | "sliding" | "session";
  window_start_ms: number;
  window_end_ms: number;
  count: number;
  user_id?: string | null;
  _recv_ts?: number;
};

export type TumblingMsg = { recent: WindowOut[] } | WindowOut;
export type SlidingMsg = { recent: WindowOut[] } | WindowOut;
export type SessionMsg = { recent: WindowOut[] };

export type JoinMatch = {
  user_id: string;
  product_id_viewed: string;
  product_id_purchased: string;
  view_ts: number;
  purchase_ts: number;
  delta_ms: number;
  amount_usd: number;
  _recv_ts?: number;
};
export type JoinMsg = { recent: JoinMatch[] };

export type FraudAlert = {
  user_id: string;
  purchase_count: number;
  total_amount_usd: number;
  first_ts: number;
  last_ts: number;
  span_ms: number;
  _recv_ts?: number;
};
export type FraudMsg = { recent: FraudAlert[] };

export type GeneratorStats = {
  sent: number;
  late_sent: number;
  purchases_sent: number;
  fraud_sent: number;
  rate: number;
  purchase_rate: number;
  late_pct: number;
  late_max_ms: number;
  spike_active: boolean;
  spike_until: number;
  fraud_active: boolean;
};
