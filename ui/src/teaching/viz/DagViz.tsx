import type { DagMsg } from "../types";

const W = 200;
const H = 70;
const GAP_X = 60;
const Y = 24;

export function DagViz({ data }: { data?: DagMsg }) {
  if (!data) {
    return (
      <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
        No running job. Submit one:{" "}
        <code className="ml-2 bg-zinc-900 px-2 py-0.5 rounded">
          ./scripts/submit-job.sh
        </code>
      </div>
    );
  }
  const vertices = [...data.vertices];
  const total = vertices.length || 1;
  const width = total * W + (total - 1) * GAP_X + 32;
  const running = data.state === "RUNNING";

  return (
    <div className="space-y-3">
      <div className="text-xs uppercase tracking-wider text-zinc-500 flex justify-between">
        <span>job · {data.job_name}</span>
        <span className={running ? "text-emerald-400" : "text-rose-400"}>
          {data.state}
        </span>
      </div>
      <div className="overflow-x-auto">
        <svg width={width} height={H + 48} className="block">
          <defs>
            <marker
              id="arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto"
            >
              <path d="M0,0 L10,5 L0,10 z" fill="#52525b" />
            </marker>
            <radialGradient id="dotGlow">
              <stop offset="0%" stopColor="#34d399" stopOpacity="1" />
              <stop offset="100%" stopColor="#34d399" stopOpacity="0" />
            </radialGradient>
          </defs>

          {data.edges.map((e, i) => {
            const si = vertices.findIndex((v) => v.id === e.source);
            const ti = vertices.findIndex((v) => v.id === e.target);
            if (si < 0 || ti < 0) return null;
            const x1 = 16 + si * (W + GAP_X) + W;
            const x2 = 16 + ti * (W + GAP_X);
            const y = Y + H / 2;
            return (
              <g key={i}>
                <line
                  x1={x1}
                  y1={y}
                  x2={x2 - 8}
                  y2={y}
                  stroke="#52525b"
                  strokeWidth={1.5}
                  markerEnd="url(#arrow)"
                />
                {running && (
                  <>
                    <FlowDot x1={x1} y={y} x2={x2 - 12} delay={0} />
                    <FlowDot x1={x1} y={y} x2={x2 - 12} delay={0.6} />
                    <FlowDot x1={x1} y={y} x2={x2 - 12} delay={1.2} />
                  </>
                )}
              </g>
            );
          })}

          {vertices.map((v, i) => {
            const x = 16 + i * (W + GAP_X);
            const reads = v.metrics["read-records"] ?? 0;
            const writes = v.metrics["write-records"] ?? 0;
            return (
              <g key={v.id} transform={`translate(${x},${Y})`}>
                <rect
                  width={W}
                  height={H}
                  rx={8}
                  className="fill-zinc-900 stroke-zinc-700"
                  strokeWidth={1}
                />
                <text x={12} y={20} className="fill-zinc-200 text-xs font-mono">
                  {truncate(v.name, 24)}
                </text>
                <text x={12} y={38} className="fill-zinc-500 text-[10px]">
                  parallelism {v.parallelism}
                </text>
                <text
                  x={12}
                  y={56}
                  className="fill-emerald-400 text-[10px] font-mono"
                >
                  in {fmt(reads)} · out {fmt(writes)}
                </text>
                <g transform={`translate(${W - 14},14)`}>
                  {v.status === "RUNNING" && (
                    <circle
                      r={6}
                      className="fill-emerald-400/30"
                    >
                      <animate
                        attributeName="r"
                        from="4"
                        to="9"
                        dur="1.4s"
                        repeatCount="indefinite"
                      />
                      <animate
                        attributeName="opacity"
                        from="0.6"
                        to="0"
                        dur="1.4s"
                        repeatCount="indefinite"
                      />
                    </circle>
                  )}
                  <circle
                    r={4}
                    className={
                      v.status === "RUNNING"
                        ? "fill-emerald-400"
                        : "fill-zinc-600"
                    }
                  />
                </g>
              </g>
            );
          })}
        </svg>
      </div>
      <div className="text-[10px] text-zinc-600">
        Green dots = records flowing between operators. Pulse ring = operator
        running.
      </div>
    </div>
  );
}

function FlowDot({
  x1,
  y,
  x2,
  delay,
}: {
  x1: number;
  y: number;
  x2: number;
  delay: number;
}) {
  return (
    <circle r={3} fill="#34d399">
      <animate
        attributeName="cx"
        from={x1}
        to={x2}
        dur="1.6s"
        begin={`${delay}s`}
        repeatCount="indefinite"
      />
      <animate
        attributeName="cy"
        from={y}
        to={y}
        dur="1.6s"
        begin={`${delay}s`}
        repeatCount="indefinite"
      />
      <animate
        attributeName="opacity"
        values="0;1;1;0"
        keyTimes="0;0.1;0.9;1"
        dur="1.6s"
        begin={`${delay}s`}
        repeatCount="indefinite"
      />
    </circle>
  );
}

function truncate(s: string, n: number) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}
function fmt(n: number) {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(1) + "k";
  return String(n);
}
