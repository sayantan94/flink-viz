import { useEffect, useRef } from "react";
import type { WindowOut, GeneratorStats } from "../types";

/**
 * Side-by-side comparison: event-time tumbling window counts (from
 * window-stats job) vs a "what processing time would have looked like"
 * approximation derived from the live generator rate during the same span.
 *
 * The point of the viz is to make obvious that event-time results are stable
 * even under bursts/late events, whereas processing-time counts spike with
 * wall-clock arrival patterns.
 */
export function EventTimeViz({
  tumbling,
  generator,
}: {
  tumbling?: { recent?: WindowOut[] };
  generator?: GeneratorStats;
}) {
  const procSeries = useRef<Array<{ t: number; rate: number }>>([]);
  useEffect(() => {
    if (!generator) return;
    procSeries.current.push({ t: Date.now(), rate: generator.rate });
    if (procSeries.current.length > 30) procSeries.current.shift();
  }, [generator?.rate, generator?.sent]);

  const eventWins = (tumbling?.recent ?? []).slice(0, 12).reverse();
  if (!eventWins.length) {
    return (
      <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
        Waiting for first tumbling window…
      </div>
    );
  }

  const maxCount = Math.max(...eventWins.map((w) => w.count), 1);

  return (
    <div className="space-y-4">
      <div className="text-xs uppercase tracking-wider text-zinc-500">
        event time · stable, reproducible
      </div>
      <div className="grid grid-cols-12 gap-1.5 items-end h-32">
        {eventWins.map((w) => {
          const h = (w.count / maxCount) * 100;
          return (
            <div
              key={w.window_start_ms}
              className="flex flex-col items-center gap-1"
              title={`${w.count} events in [${new Date(w.window_start_ms).toLocaleTimeString()}, ${new Date(w.window_end_ms).toLocaleTimeString()})`}
            >
              <div className="flex-1 w-full flex items-end">
                <div
                  className="w-full bg-emerald-500/70 rounded-sm transition-all duration-500"
                  style={{ height: `${h}%` }}
                />
              </div>
              <div className="text-[9px] text-zinc-500 font-mono">
                {new Date(w.window_start_ms).toLocaleTimeString().slice(0, 5)}
              </div>
            </div>
          );
        })}
      </div>

      <div className="text-xs uppercase tracking-wider text-zinc-500 pt-2">
        processing time · jittery, depends on arrival
      </div>
      <div className="grid grid-cols-12 gap-1.5 items-end h-20">
        {procSeries.current.slice(-12).map((p, i) => {
          const max = Math.max(
            ...procSeries.current.slice(-12).map((x) => x.rate),
            1,
          );
          const h = (p.rate / max) * 100;
          return (
            <div key={`${p.t}-${i}`} className="flex flex-col items-center gap-1">
              <div className="flex-1 w-full flex items-end">
                <div
                  className="w-full bg-amber-500/60 rounded-sm transition-all duration-200"
                  style={{ height: `${Math.max(8, h)}%` }}
                />
              </div>
            </div>
          );
        })}
      </div>

      <div className="text-[10px] text-zinc-600">
        Top row: event-time counts per 10s window, sourced from the watermark-driven Flink job.
        Bottom row: instantaneous processing-time rate sampled every second.
        In a spike the processing-time bars jump immediately, but the event-time bars only
        reflect the actual event timestamps — independent of arrival order.
      </div>
    </div>
  );
}
