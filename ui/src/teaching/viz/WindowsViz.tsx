import { useEffect, useRef } from "react";
import type { WindowOut } from "../types";

/**
 * Generic horizontal time-bar viz for tumbling/sliding/session windows.
 * Each rect is one closed window: x = window time range, width = duration,
 * fill alpha = relative count.
 */
export function WindowsTimelineViz({
  data,
  kind,
  history,
}: {
  data?: { recent?: WindowOut[] } | WindowOut;
  kind: "tumbling" | "sliding" | "session";
  history?: WindowOut[];
}) {
  // accept either a single message or a {recent: [...]} buffer
  const all =
    history ??
    (data && "recent" in data && Array.isArray(data.recent)
      ? data.recent
      : data
      ? [data as WindowOut]
      : []);
  const buf = useRef<WindowOut[]>([]);

  useEffect(() => {
    if (!all.length) return;
    const seen = new Set(
      buf.current.map((w) => `${w.kind}:${w.window_start_ms}:${w.user_id ?? ""}`),
    );
    for (const w of all) {
      const k = `${w.kind}:${w.window_start_ms}:${w.user_id ?? ""}`;
      if (!seen.has(k)) {
        buf.current.unshift(w);
        seen.add(k);
      }
    }
    if (buf.current.length > 30) buf.current.length = 30;
  }, [all]);

  const recent = buf.current.length ? buf.current : all;
  if (!recent.length) {
    return (
      <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
        Waiting for first {kind} window to close…
      </div>
    );
  }

  const ordered = [...recent].sort(
    (a, b) => a.window_start_ms - b.window_start_ms,
  );
  const tStart = ordered[0].window_start_ms;
  const tEnd = Math.max(...ordered.map((w) => w.window_end_ms));
  const span = Math.max(tEnd - tStart, 1);
  const maxCount = Math.max(...ordered.map((w) => w.count), 1);

  const W = 760;
  const H = 200;
  const trackH = 22;
  const padY = 12;

  // Stack overlapping windows in tracks for sliding/sessions.
  const tracks: WindowOut[][] = [];
  for (const w of ordered) {
    let placed = false;
    for (const tr of tracks) {
      if (tr[tr.length - 1].window_end_ms <= w.window_start_ms) {
        tr.push(w);
        placed = true;
        break;
      }
    }
    if (!placed) tracks.push([w]);
  }
  const totalH = Math.max(H, padY * 2 + tracks.length * (trackH + 4));

  return (
    <div className="space-y-3">
      <div className="text-xs uppercase tracking-wider text-zinc-500 flex justify-between">
        <span>recent {kind} windows · {ordered.length}</span>
        <span className="tabular-nums">
          {new Date(tStart).toLocaleTimeString()} →{" "}
          {new Date(tEnd).toLocaleTimeString()}
        </span>
      </div>
      <svg viewBox={`0 0 ${W} ${totalH}`} className="w-full">
        {tracks.map((tr, ti) =>
          tr.map((w) => {
            const x = ((w.window_start_ms - tStart) / span) * (W - 10);
            const wd = Math.max(
              4,
              ((w.window_end_ms - w.window_start_ms) / span) * (W - 10),
            );
            const y = padY + ti * (trackH + 4);
            const intensity = Math.min(1, w.count / maxCount);
            const fill = `rgba(52, 211, 153, ${0.25 + 0.6 * intensity})`;
            return (
              <g
                key={`${w.window_start_ms}-${ti}-${w.user_id ?? ""}`}
                transform={`translate(${x},${y})`}
              >
                <rect
                  width={wd}
                  height={trackH}
                  rx={3}
                  fill={fill}
                  stroke="#3f3f46"
                  strokeWidth={0.5}
                >
                  <animate
                    attributeName="opacity"
                    from="0"
                    to="1"
                    dur="0.5s"
                    fill="freeze"
                  />
                </rect>
                {wd > 36 && (
                  <text
                    x={wd / 2}
                    y={trackH / 2 + 4}
                    textAnchor="middle"
                    className="fill-zinc-100 text-[10px] font-mono"
                  >
                    {w.count}
                    {w.user_id ? ` · ${w.user_id.slice(0, 10)}` : ""}
                  </text>
                )}
              </g>
            );
          }),
        )}
      </svg>
      <div className="text-[10px] text-zinc-600">
        x-axis = wall-clock time. Each rect = one closed window. Color
        intensity = event count.{" "}
        {kind === "session"
          ? "Stacked rows = concurrent user sessions."
          : kind === "sliding"
          ? "Overlap shows sliding cadence."
          : "Non-overlapping = tumbling."}
      </div>
    </div>
  );
}
