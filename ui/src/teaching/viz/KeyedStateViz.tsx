import type { TopKMsg } from "../types";

/**
 * Renders a heatmap of keyed-state distribution. We don't have direct state
 * size metrics today (a custom telemetry side output is planned), so we use
 * the per-product counts from the most recent top-K window as a proxy for
 * "this key has more state right now". It's a real artifact of the keyed
 * state machinery — just measured by event-count instead of byte-size.
 */
export function KeyedStateViz({ data }: { data?: TopKMsg }) {
  if (!data) {
    return (
      <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
        Waiting for first window…
      </div>
    );
  }
  const entries = data.top;
  const max = Math.max(...entries.map((e) => e.count), 1);
  return (
    <div className="space-y-3">
      <div className="text-xs uppercase tracking-wider text-zinc-500 flex justify-between">
        <span>state heatmap by key (proxied by recent count)</span>
        <span className="tabular-nums">
          {entries.length} keys in window
        </span>
      </div>
      <div className="grid grid-cols-5 gap-1.5">
        {entries.map((e) => {
          const intensity = Math.min(1, e.count / max);
          const bg = `rgba(167, 139, 250, ${0.15 + 0.7 * intensity})`;
          return (
            <div
              key={e.product_id}
              className="rounded p-2 border border-zinc-800 transition-all duration-500"
              style={{ backgroundColor: bg }}
              title={`${e.product_id}: ${e.count}`}
            >
              <div className="text-[10px] font-mono text-zinc-300 truncate">
                {e.product_id}
              </div>
              <div className="text-xs font-mono text-zinc-100">
                {e.count}
              </div>
            </div>
          );
        })}
      </div>
      <div className="text-[10px] text-zinc-600">
        Darker violet = more events for that key = more keyed state on this
        operator. Skew here = skew in your state partitioning.
      </div>
    </div>
  );
}
