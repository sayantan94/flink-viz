import { useEffect, useState } from "react";
import type { CheckpointsMsg } from "../types";

export function CheckpointsViz({ data }: { data?: CheckpointsMsg }) {
  const [lastSeenId, setLastSeenId] = useState<number>(-1);
  const [flashId, setFlashId] = useState<number | null>(null);

  useEffect(() => {
    if (!data?.history.length) return;
    const newest = data.history[0].id;
    if (newest !== lastSeenId) {
      setLastSeenId(newest);
      setFlashId(newest);
      const t = setTimeout(() => setFlashId(null), 1100);
      return () => clearTimeout(t);
    }
  }, [data?.history[0]?.id, lastSeenId]);

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
        <span className="tabular-nums">
          <span className="text-emerald-400">{data.counts.completed} ✓</span>
          {" · "}
          <span className="text-rose-400">{data.counts.failed} ✗</span>
          {" · "}
          <span className="text-amber-400">{data.counts.in_progress} ⏳</span>
        </span>
      </div>
      <div className="grid grid-cols-12 gap-1.5 items-end">
        {data.history.map((c) => {
          const color =
            c.status === "COMPLETED"
              ? "bg-emerald-500/80"
              : c.status === "IN_PROGRESS"
              ? "bg-amber-400/80"
              : "bg-rose-500/80";
          const height = Math.max(8, Math.min(56, c.duration_ms / 20));
          const flashing = c.id === flashId;
          return (
            <div
              key={c.id}
              className="flex flex-col items-center gap-1"
              title={`#${c.id} ${c.status}\n${(c.state_size / 1024).toFixed(
                1,
              )} KiB · ${c.duration_ms}ms`}
            >
              <div className="text-[9px] text-zinc-500 font-mono">{c.id}</div>
              <div className="h-14 w-full flex items-end relative">
                <div
                  className={
                    `${color} w-full rounded-sm transition-all duration-500 ` +
                    (flashing ? "animate-flash-ring" : "")
                  }
                  style={{ height }}
                />
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
      <div className="text-[10px] text-zinc-600">
        Bar height = checkpoint duration. Newest checkpoint flashes violet
        as a fresh barrier completes.
      </div>
    </div>
  );
}
