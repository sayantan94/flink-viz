import type { DagMsg } from "../types";

const W = 200;
const H = 70;
const GAP_X = 60;
const Y = 24;

export function DagViz({ data }: { data?: DagMsg }) {
  if (!data) {
    return (
      <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
        No running job. Submit one: <code className="ml-2 bg-zinc-900 px-2 py-0.5 rounded">./scripts/submit-job.sh</code>
      </div>
    );
  }
  // Topological-ish: sort by edge count so sources come first.
  const vertices = [...data.vertices];
  const total = vertices.length || 1;
  const width = total * W + (total - 1) * GAP_X + 32;

  return (
    <div className="space-y-3">
      <div className="text-xs uppercase tracking-wider text-zinc-500 flex justify-between">
        <span>job · {data.job_name}</span>
        <span className={data.state === "RUNNING" ? "text-emerald-400" : "text-rose-400"}>
          {data.state}
        </span>
      </div>
      <div className="overflow-x-auto">
        <svg width={width} height={H + 48} className="block">
          <defs>
            <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5"
                    markerWidth="6" markerHeight="6" orient="auto">
              <path d="M0,0 L10,5 L0,10 z" fill="#52525b" />
            </marker>
          </defs>
          {data.edges.map((e, i) => {
            const si = vertices.findIndex((v) => v.id === e.source);
            const ti = vertices.findIndex((v) => v.id === e.target);
            if (si < 0 || ti < 0) return null;
            const x1 = 16 + si * (W + GAP_X) + W;
            const x2 = 16 + ti * (W + GAP_X);
            const y = Y + H / 2;
            return (
              <line key={i} x1={x1} y1={y} x2={x2 - 8} y2={y}
                    stroke="#52525b" strokeWidth={1.5} markerEnd="url(#arrow)" />
            );
          })}
          {vertices.map((v, i) => {
            const x = 16 + i * (W + GAP_X);
            const reads = v.metrics["read-records"] ?? 0;
            const writes = v.metrics["write-records"] ?? 0;
            return (
              <g key={v.id} transform={`translate(${x},${Y})`}>
                <rect width={W} height={H} rx={8}
                      className="fill-zinc-900 stroke-zinc-700" strokeWidth={1} />
                <text x={12} y={20} className="fill-zinc-200 text-xs font-mono">
                  {truncate(v.name, 24)}
                </text>
                <text x={12} y={38} className="fill-zinc-500 text-[10px]">
                  parallelism {v.parallelism}
                </text>
                <text x={12} y={56} className="fill-emerald-400 text-[10px] font-mono">
                  in {fmt(reads)}  ·  out {fmt(writes)}
                </text>
                <circle cx={W - 14} cy={14} r={4}
                        className={v.status === "RUNNING" ? "fill-emerald-400" : "fill-zinc-600"} />
              </g>
            );
          })}
        </svg>
      </div>
    </div>
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
