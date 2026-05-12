import type { CheckpointsMsg } from "../types";

export function CheckpointsViz({ data }: { data?: CheckpointsMsg }) {
  if (!data) {
    return (
      <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
        No checkpoint data yet.
      </div>
    );
  }
  return (
    <div className="space-y-4">
      <div className="text-xs uppercase tracking-wider text-zinc-500 flex justify-between">
        <span>checkpoint history (newest → oldest)</span>
        <span>
          <span className="text-emerald-400">{data.counts.completed} ✓</span>
          {" · "}
          <span className="text-rose-400">{data.counts.failed} ✗</span>
          {" · "}
          <span className="text-amber-400">{data.counts.in_progress} ⏳</span>
        </span>
      </div>
      <div className="grid grid-cols-12 gap-1.5">
        {data.history.map((c) => {
          const color =
            c.status === "COMPLETED" ? "bg-emerald-500/80" :
            c.status === "IN_PROGRESS" ? "bg-amber-400/80" :
            "bg-rose-500/80";
          const height = Math.max(8, Math.min(56, c.duration_ms / 20));
          return (
            <div key={c.id} className="flex flex-col items-center gap-1" title={
              `#${c.id} ${c.status}\n${(c.state_size / 1024).toFixed(1)} KiB · ${c.duration_ms}ms`
            }>
              <div className="text-[9px] text-zinc-500">{c.id}</div>
              <div className="h-14 w-full flex items-end">
                <div className={`${color} w-full rounded-sm`} style={{ height }} />
              </div>
              <div className="text-[9px] text-zinc-500 font-mono">
                {c.duration_ms}ms
              </div>
              {c.is_savepoint ? (
                <div className="text-[9px] text-violet-400">SP</div>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}
