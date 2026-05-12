import { useEffect, useRef, useState } from "react";
import type { GeneratorStats } from "../types";

type Drop = { id: number; x: number; ts: number };

/**
 * Visualizes late events as red drops falling below a horizontal timeline
 * representing the watermark frontier. Driven by the generator stats counter.
 */
export function LateEventsViz({
  data,
  onLatePct,
}: {
  data?: GeneratorStats;
  onLatePct?: (pct: number) => void;
}) {
  const [drops, setDrops] = useState<Drop[]>([]);
  const lastCount = useRef(0);
  const nextId = useRef(0);

  useEffect(() => {
    if (!data) return;
    const delta = data.late_sent - lastCount.current;
    lastCount.current = data.late_sent;
    if (delta > 0) {
      const toAdd: Drop[] = [];
      const n = Math.min(delta, 12);
      for (let i = 0; i < n; i++) {
        toAdd.push({
          id: nextId.current++,
          x: 5 + Math.random() * 90,
          ts: Date.now(),
        });
      }
      setDrops((d) => [...d, ...toAdd].slice(-40));
    }
  }, [data?.late_sent]);

  // GC old drops
  useEffect(() => {
    const t = setInterval(
      () =>
        setDrops((d) =>
          d.filter((dr) => Date.now() - dr.ts < 3500),
        ),
      500,
    );
    return () => clearInterval(t);
  }, []);

  return (
    <div className="space-y-3">
      <div className="text-xs uppercase tracking-wider text-zinc-500 flex justify-between">
        <span>late events stream</span>
        <span className="tabular-nums">
          late: <span className="text-rose-400">{data?.late_sent ?? 0}</span> ·
          pct: {((data?.late_pct ?? 0) * 100).toFixed(1)}%
        </span>
      </div>
      <div className="relative h-44 bg-zinc-900/40 rounded border border-zinc-800 overflow-hidden">
        {/* watermark line */}
        <div className="absolute left-0 right-0 top-1/3 border-t-2 border-emerald-500/60" />
        <div className="absolute top-[calc(33%+4px)] left-2 text-[10px] text-emerald-400">
          watermark frontier (events above are "in time")
        </div>
        <div className="absolute bottom-1 left-2 text-[10px] text-rose-400">
          late drops · would be discarded without allowedLateness or side-output
        </div>
        {drops.map((d) => {
          const age = Math.min(1, (Date.now() - d.ts) / 3000);
          return (
            <span
              key={d.id}
              className="absolute w-1.5 h-1.5 bg-rose-400 rounded-full transition-all"
              style={{
                left: `${d.x}%`,
                top: `${33 + age * 60}%`,
                opacity: 1 - age,
              }}
            />
          );
        })}
      </div>
      {onLatePct && (
        <div className="flex items-center gap-3">
          <label className="text-[11px] text-zinc-400">
            Inject % late events:
          </label>
          {[0, 5, 20, 50].map((p) => (
            <button
              key={p}
              onClick={() => onLatePct(p)}
              className="text-[11px] px-2 py-1 bg-zinc-900 hover:bg-zinc-800 border border-zinc-700 rounded"
            >
              {p}%
            </button>
          ))}
        </div>
      )}
      <div className="text-[10px] text-zinc-600">
        Each red dot = one event whose timestamp is older than the watermark.
        Flink's default policy drops them; with{" "}
        <code className="bg-zinc-900 px-1 rounded">allowedLateness</code> the
        window state is kept and re-fired, or routed to a side output.
      </div>
    </div>
  );
}
