import type { FraudMsg } from "../types";

export function FraudViz({
  data,
  onTrigger,
}: {
  data?: FraudMsg;
  onTrigger?: () => void;
}) {
  const alerts = data?.recent ?? [];
  return (
    <div className="space-y-3">
      <div className="text-xs uppercase tracking-wider text-zinc-500 flex justify-between items-center">
        <span>fraud alerts ({alerts.length})</span>
        {onTrigger && (
          <button
            onClick={onTrigger}
            className="text-[11px] px-3 py-1 bg-rose-900/40 hover:bg-rose-900/70 border border-rose-700 rounded text-rose-200"
          >
            ⚡ Inject fraud burst
          </button>
        )}
      </div>
      {alerts.length === 0 ? (
        <div className="h-32 flex items-center justify-center text-zinc-500 text-sm border border-dashed border-zinc-800 rounded">
          No fraud detected yet. Click "Inject fraud burst" — the CEP pattern
          (≥3 purchases by same user within 10s) fires within seconds.
        </div>
      ) : (
        <ol className="space-y-1.5">
          {alerts.slice(0, 8).map((a, i) => (
            <li
              key={`${a.user_id}-${a.first_ts}-${i}`}
              className="grid grid-cols-12 gap-2 items-center text-xs bg-rose-950/30 border border-rose-900/60 rounded px-3 py-2 font-mono"
            >
              <span className="col-span-3 text-rose-300 truncate">
                ⚠ {a.user_id}
              </span>
              <span className="col-span-2 text-center">
                <span className="text-rose-200 font-bold">
                  {a.purchase_count}
                </span>{" "}
                <span className="text-zinc-500">buys</span>
              </span>
              <span className="col-span-3 text-zinc-400 text-right">
                in {fmtMs(a.span_ms)}
              </span>
              <span className="col-span-2 text-right text-rose-300">
                ${a.total_amount_usd.toFixed(0)}
              </span>
              <span className="col-span-2 text-zinc-600 text-right">
                {a._recv_ts
                  ? new Date(a._recv_ts).toLocaleTimeString()
                  : ""}
              </span>
            </li>
          ))}
        </ol>
      )}
      <div className="text-[10px] text-zinc-600">
        CEP pattern: <code className="bg-zinc-900 px-1 rounded">begin →
        followedBy times(2+) within 10s</code>. Keyed by user_id.
      </div>
    </div>
  );
}

function fmtMs(ms: number) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}
