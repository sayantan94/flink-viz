type FlinkJobOverview = {
  jid: string;
  name: string;
  state: string;
  duration: number;
  tasks: Record<string, number>;
};

export type DagVertex = {
  id: string;
  name: string;
  parallelism: number;
  status: string;
  metrics: {
    "read-records"?: number;
    "write-records"?: number;
    "read-bytes"?: number;
    "write-bytes"?: number;
  };
};

export type DagMessage = {
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

export type CheckpointsMessage = {
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

export type WatermarkMessage = {
  job_id: string;
  ts: number;
  subtasks: WatermarkSubtask[];
};

type Broadcaster = (type: string, payload: unknown) => void;

async function getJson<T>(url: string): Promise<T | null> {
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

async function fetchDag(flinkUrl: string): Promise<DagMessage | null> {
  const overview = await getJson<{ jobs: FlinkJobOverview[] }>(
    `${flinkUrl}/jobs/overview`,
  );
  const running = overview?.jobs.find((j) => j.state === "RUNNING");
  if (!running) return null;

  const plan = await getJson<{
    jid: string;
    name: string;
    state: string;
    vertices: DagVertex[];
    plan: { nodes: { id: string; inputs?: { id: string }[] }[] };
  }>(`${flinkUrl}/jobs/${running.jid}`);
  if (!plan) return null;

  const edges: { source: string; target: string }[] = [];
  for (const node of plan.plan.nodes) {
    for (const input of node.inputs ?? []) {
      edges.push({ source: input.id, target: node.id });
    }
  }

  return {
    job_id: plan.jid,
    job_name: plan.name,
    state: plan.state,
    vertices: plan.vertices,
    edges,
  };
}

async function fetchCheckpoints(
  flinkUrl: string,
  jobId: string,
): Promise<CheckpointsMessage | null> {
  const c = await getJson<{
    counts: { completed: number; failed: number; in_progress: number };
    history: Array<{
      id: number;
      status: string;
      trigger_timestamp: number;
      end_to_end_duration: number;
      state_size: number;
      is_savepoint: boolean;
    }>;
  }>(`${flinkUrl}/jobs/${jobId}/checkpoints`);
  if (!c) return null;
  return {
    job_id: jobId,
    counts: c.counts,
    history: c.history.slice(0, 12).map((h) => ({
      id: h.id,
      status: h.status,
      trigger_ts: h.trigger_timestamp,
      duration_ms: h.end_to_end_duration,
      state_size: h.state_size,
      is_savepoint: h.is_savepoint,
    })),
  };
}

async function fetchWatermarks(
  flinkUrl: string,
  jobId: string,
  vertices: DagVertex[],
): Promise<WatermarkMessage | null> {
  const subtasks: WatermarkSubtask[] = [];
  for (const v of vertices) {
    // Flink 1.18 returns: [{"id":"<subtask>.currentInputWatermark","value":"<ms>"}]
    const m = await getJson<Array<{ id: string; value: string }>>(
      `${flinkUrl}/jobs/${jobId}/vertices/${v.id}/watermarks`,
    );
    if (!m) continue;
    for (const entry of m) {
      const subtask = Number(entry.id.split(".")[0]);
      const wm = Number(entry.value);
      if (!Number.isFinite(subtask) || !Number.isFinite(wm)) continue;
      subtasks.push({
        vertex_id: v.id,
        vertex_name: v.name,
        subtask,
        watermark_ms: wm,
      });
    }
  }
  return { job_id: jobId, ts: Date.now(), subtasks };
}

export function startFlinkPoller(
  flinkUrl: string,
  broadcast: Broadcaster,
  intervalMs = 1500,
) {
  let lastDag: DagMessage | null = null;
  const tick = async () => {
    try {
      const dag = await fetchDag(flinkUrl);
      if (dag) {
        lastDag = dag;
        broadcast("dag", dag);
        const cps = await fetchCheckpoints(flinkUrl, dag.job_id);
        if (cps) broadcast("checkpoints", cps);
        const wm = await fetchWatermarks(flinkUrl, dag.job_id, dag.vertices);
        if (wm) broadcast("watermarks", wm);
      } else if (lastDag) {
        broadcast("dag", { ...lastDag, state: "NOT_RUNNING" });
      }
    } catch {
      /* swallow — keep polling */
    }
  };
  tick();
  return setInterval(tick, intervalMs);
}
