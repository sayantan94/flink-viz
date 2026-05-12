import { useState } from "react";
import type { CheckpointsMsg } from "../types";

export function RecoveryViz({ checkpoints }: { checkpoints?: CheckpointsMsg }) {
  const [status, setStatus] = useState<string>("");

  const latest = checkpoints?.history[0];
  const completed = checkpoints?.counts.completed ?? 0;
  const inProgress = checkpoints?.counts.in_progress ?? 0;

  return (
    <div className="space-y-4">
      <div className="text-xs uppercase tracking-wider text-zinc-500">
        recovery story · what makes exactly-once possible
      </div>

      <div className="grid grid-cols-3 gap-3">
        <Stat label="checkpoints completed" value={completed.toLocaleString()} accent="emerald" />
        <Stat
          label="latest barrier age"
          value={latest ? fmtAgo(Date.now() - latest.trigger_ts) : "—"}
          accent="emerald"
        />
        <Stat
          label="in-flight checkpoints"
          value={inProgress.toString()}
          accent={inProgress > 0 ? "amber" : "zinc"}
        />
      </div>

      <div className="rounded border border-zinc-800 p-4 space-y-3">
        <div className="text-xs text-zinc-400">The recovery loop</div>
        <ol className="text-sm text-zinc-200 space-y-1.5 list-decimal list-inside">
          <li>
            JobManager schedules a <b>checkpoint</b> every 10s — a barrier
            flows from the Kafka source through every operator.
          </li>
          <li>
            Each operator <b>snapshots its state</b> when the barrier arrives,
            then forwards the barrier. (Asynchronously — the stream never
            stops.)
          </li>
          <li>
            When all operators ack, the checkpoint is <b>durable</b>. Kafka
            offsets are part of that snapshot.
          </li>
          <li>
            Kill a TaskManager → JobManager restarts the job from the{" "}
            <b>most recent completed checkpoint</b>. Kafka rewinds to the
            stored offset. <b>No data loss, no double counting</b>.
          </li>
        </ol>
      </div>

      <div className="rounded border border-zinc-800 p-4 space-y-3">
        <div className="text-xs text-zinc-400">Try it on the live cluster</div>
        <div className="flex flex-wrap gap-2">
          <CmdBtn
            label="Trigger savepoint"
            onClick={async () => {
              setStatus("triggering savepoint…");
              setStatus(
                "Run in shell:  ./scripts/savepoint.sh   (writes durable snapshot to disk)",
              );
            }}
          />
          <CmdBtn
            label="Show me how to kill a TM"
            onClick={() =>
              setStatus(
                "ps -ef | grep TaskManagerRunner   then  kill -9 <pid>   → watch this view, job auto-restarts from last checkpoint.",
              )
            }
          />
          <CmdBtn
            label="Show me how to restore"
            onClick={() =>
              setStatus(
                "$FLINK_HOME/bin/flink run -d -s <savepoint-path> <jar>   → restarts the job from a savepoint instead of the latest checkpoint.",
              )
            }
          />
        </div>
        {status && (
          <pre className="text-[11px] bg-zinc-900 border border-zinc-800 rounded p-2 whitespace-pre-wrap text-zinc-300">
            {status}
          </pre>
        )}
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
  accent,
}: {
  label: string;
  value: string;
  accent: "emerald" | "amber" | "zinc";
}) {
  const color =
    accent === "emerald"
      ? "text-emerald-400"
      : accent === "amber"
      ? "text-amber-400"
      : "text-zinc-300";
  return (
    <div className="rounded border border-zinc-800 p-3">
      <div className="text-[10px] uppercase tracking-widest text-zinc-500">
        {label}
      </div>
      <div className={"text-xl font-mono mt-1 " + color}>{value}</div>
    </div>
  );
}

function CmdBtn({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className="text-xs px-3 py-1.5 bg-zinc-900 hover:bg-zinc-800 border border-zinc-700 rounded text-zinc-200"
    >
      {label}
    </button>
  );
}

function fmtAgo(ms: number) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s ago`;
  return `${(ms / 60_000).toFixed(1)}m ago`;
}
