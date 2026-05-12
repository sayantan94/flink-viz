import { useEffect, useRef, useState } from "react";
import type { TopKMsg } from "../types";

export function TopKViz({ data }: { data?: TopKMsg }) {
  const prevRanks = useRef<Map<string, number>>(new Map());
  const [flashKey, setFlashKey] = useState(0);

  useEffect(() => {
    if (!data) return;
    setFlashKey((k) => k + 1);
    const next = new Map<string, number>();
    data.top.forEach((e, i) => next.set(e.product_id, i));
    prevRanks.current = next;
  }, [data?.window_end_ms]);

  if (!data) {
    return (
      <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
        Waiting for first window to close…
      </div>
    );
  }

  const max = Math.max(...data.top.map((e) => e.count), 1);
  const startStr = new Date(data.window_start_ms).toLocaleTimeString();
  const endStr = new Date(data.window_end_ms).toLocaleTimeString();

  return (
    <div className="space-y-4" key={flashKey}>
      <div className="text-xs uppercase tracking-wider text-zinc-500">
        window {startStr} → {endStr}
      </div>
      <ol className="space-y-1.5 relative">
        {data.top.map((e, i) => {
          const w = (e.count / max) * 100;
          const prev = prevRanks.current.get(e.product_id);
          const moved = prev !== undefined && prev !== i;
          return (
            <li
              key={e.product_id}
              className={
                "relative transition-transform duration-500 ease-out " +
                (moved ? "animate-rank-bounce" : "")
              }
              style={{ transform: "translateY(0)" }}
            >
              <div
                className="absolute inset-y-0 left-0 bg-emerald-900/40 rounded transition-all duration-700 ease-out"
                style={{ width: `${w}%` }}
              />
              <div className="relative flex justify-between items-center bg-zinc-900/70 rounded px-3 py-2 border border-zinc-800">
                <div className="flex items-center gap-3">
                  <span className="text-zinc-500 w-5 text-right text-sm">
                    {i + 1}
                  </span>
                  <span className="font-mono text-sm">{e.product_id}</span>
                  {moved && prev !== undefined && (
                    <span
                      className={
                        "text-[10px] " +
                        (prev > i ? "text-emerald-400" : "text-rose-400")
                      }
                    >
                      {prev > i ? `▲${prev - i}` : `▼${i - prev}`}
                    </span>
                  )}
                </div>
                <span className="font-mono text-sm text-emerald-400 tabular-nums">
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
