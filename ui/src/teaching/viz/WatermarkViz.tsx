import type { WatermarkMsg } from "../types";

const NO_WM = -9223372036854775808;

export function WatermarkViz({ data }: { data?: WatermarkMsg }) {
  if (!data || data.subtasks.length === 0) {
    return (
      <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
        No watermarks reported yet. They only emit once enough events have
        arrived.
      </div>
    );
  }
  const valid = data.subtasks.filter((s) => s.watermark_ms > NO_WM + 1);
  if (valid.length === 0) {
    return (
      <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
        All watermarks are still Long.MIN_VALUE. Drive more events to advance
        them.
      </div>
    );
  }
  const maxWm = Math.max(...valid.map((s) => s.watermark_ms));
  const minWm = Math.min(...valid.map((s) => s.watermark_ms));
  const span = Math.max(maxWm - minWm, 1);
  const lag = (wm: number) =>
    wm === NO_WM ? Infinity : Math.max(0, data.ts - wm);

  const byVertex = new Map<string, typeof valid>();
  for (const s of valid) {
    const arr = byVertex.get(s.vertex_name) ?? [];
    arr.push(s);
    byVertex.set(s.vertex_name, arr);
  }

  return (
    <div className="space-y-4">
      <div className="text-xs uppercase tracking-wider text-zinc-500 flex justify-between">
        <span>watermark progress per subtask</span>
        <span className="tabular-nums">
          now {new Date(data.ts).toLocaleTimeString()}
        </span>
      </div>
      <div className="space-y-4">
        {[...byVertex.entries()].map(([name, subs]) => (
          <div key={name}>
            <div className="text-xs text-zinc-400 mb-1.5 font-mono">
              {truncate(name, 50)}
            </div>
            <div className="space-y-1.5">
              {subs.map((s) => {
                const widthPct = Math.max(
                  3,
                  ((s.watermark_ms - minWm) / span) * 95 + 5,
                );
                const lagMs = lag(s.watermark_ms);
                const color =
                  lagMs < 5000
                    ? "bg-emerald-500"
                    : lagMs < 30000
                    ? "bg-amber-400"
                    : "bg-rose-500";
                return (
                  <div key={s.subtask} className="flex items-center gap-2">
                    <span className="text-[10px] text-zinc-500 w-6 font-mono">
                      #{s.subtask}
                    </span>
                    <div className="flex-1 h-6 bg-zinc-900 rounded relative overflow-hidden border border-zinc-800">
                      <div
                        className={`absolute inset-y-0 left-0 ${color} opacity-70 transition-all duration-700 ease-out`}
                        style={{ width: `${widthPct}%` }}
                      />
                      <div
                        className="absolute inset-y-0 w-0.5 bg-white/40"
                        style={{ left: `${widthPct}%` }}
                      />
                      <span className="absolute inset-0 flex items-center justify-end pr-2 text-[10px] font-mono">
                        {new Date(s.watermark_ms).toLocaleTimeString()} · lag{" "}
                        {fmtMs(lagMs)}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
      <div className="text-[10px] text-zinc-600">
        Bar width = relative watermark position. White edge = current
        watermark frontier. Color = lag from wall clock.
      </div>
    </div>
  );
}

function truncate(s: string, n: number) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}
function fmtMs(ms: number) {
  if (!isFinite(ms)) return "—";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}
