import type { TopKMsg } from "../types";

export function TopKViz({ data }: { data?: TopKMsg }) {
  if (!data) {
    return <Waiting label="Waiting for first window to close…" />;
  }
  const max = Math.max(...data.top.map((e) => e.count), 1);
  const startStr = new Date(data.window_start_ms).toLocaleTimeString();
  const endStr = new Date(data.window_end_ms).toLocaleTimeString();
  return (
    <div className="space-y-4">
      <div className="text-xs uppercase tracking-wider text-zinc-500">
        window {startStr} → {endStr}
      </div>
      <ol className="space-y-1.5">
        {data.top.map((e, i) => {
          const w = (e.count / max) * 100;
          return (
            <li key={e.product_id} className="relative">
              <div
                className="absolute inset-y-0 left-0 bg-emerald-900/40 rounded"
                style={{ width: `${w}%` }}
              />
              <div className="relative flex justify-between items-center bg-zinc-900/70 rounded px-3 py-2 border border-zinc-800">
                <div className="flex items-center gap-3">
                  <span className="text-zinc-500 w-5 text-right text-sm">
                    {i + 1}
                  </span>
                  <span className="font-mono text-sm">{e.product_id}</span>
                </div>
                <span className="font-mono text-sm text-emerald-400">
                  {e.count.toLocaleString()}
                </span>
              </div>
            </li>
          );
        })}
      </ol>
    </div>
  );
}

function Waiting({ label }: { label: string }) {
  return (
    <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
      {label}
    </div>
  );
}
