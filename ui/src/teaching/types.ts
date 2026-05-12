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
