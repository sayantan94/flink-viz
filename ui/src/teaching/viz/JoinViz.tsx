import type { JoinMsg } from "../types";

export function JoinViz({ data }: { data?: JoinMsg }) {
  if (!data?.recent?.length) {
    return (
      <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
        Waiting for first view → purchase match…
      </div>
    );
  }
  const matches = data.recent.slice(0, 12);
  const sameProductPct =
    (matches.filter((m) => m.product_id_viewed === m.product_id_purchased)
      .length /
      matches.length) *
    100;
  return (
    <div className="space-y-3">
      <div className="text-xs uppercase tracking-wider text-zinc-500 flex justify-between">
        <span>recent join matches</span>
        <span className="tabular-nums">
          same-product conversion: {sameProductPct.toFixed(0)}%
        </span>
      </div>
      <ol className="space-y-1.5">
        {matches.map((m, i) => {
          const same = m.product_id_viewed === m.product_id_purchased;
          return (
            <li
              key={`${m.view_ts}-${m.purchase_ts}-${i}`}
              className="grid grid-cols-12 gap-2 items-center text-xs bg-zinc-900/50 border border-zinc-800 rounded px-3 py-1.5 font-mono"
            >
              <span className="col-span-2 text-zinc-500 truncate">
                {m.user_id}
              </span>
              <span className="col-span-3 truncate">{m.product_id_viewed}</span>
              <span className="col-span-1 text-zinc-600">→</span>
              <span className={"col-span-3 truncate " + (same ? "text-emerald-400" : "text-amber-400")}>
                {m.product_id_purchased}
              </span>
              <span className="col-span-2 text-right text-zinc-400">
                {fmtMs(m.delta_ms)} later
              </span>
              <span className="col-span-1 text-right text-emerald-400">
                ${m.amount_usd.toFixed(0)}
              </span>
            </li>
          );
        })}
      </ol>
      <div className="text-[10px] text-zinc-600">
        Green = same product viewed and bought. Amber = bought a different
        product after the view.
      </div>
    </div>
  );
}

function fmtMs(ms: number) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}
