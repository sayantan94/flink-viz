type Entry = { product_id: string; count: number };

type Props = {
  windowStartMs?: number;
  windowEndMs?: number;
  top?: Entry[];
};

export function Leaderboard({ windowStartMs, windowEndMs, top }: Props) {
  if (!top) {
    return (
      <div className="p-6 text-zinc-500">
        Waiting for first window to close…
      </div>
    );
  }
  const startStr = windowStartMs
    ? new Date(windowStartMs).toLocaleTimeString()
    : "";
  const endStr = windowEndMs
    ? new Date(windowEndMs).toLocaleTimeString()
    : "";
  const maxCount = Math.max(...top.map((e) => e.count), 1);

  return (
    <div className="p-6 max-w-2xl mx-auto">
      <div className="text-sm text-zinc-500 mb-3">
        Window: {startStr} → {endStr}
      </div>
      <ol className="space-y-2">
        {top.map((e, i) => {
          const width = (e.count / maxCount) * 100;
          return (
            <li key={e.product_id} className="relative">
              <div
                className="absolute inset-y-0 left-0 bg-emerald-900/40 rounded-lg"
                style={{ width: `${width}%` }}
              />
              <div className="relative flex justify-between items-center bg-zinc-900/60 rounded-lg px-4 py-3 border border-zinc-800">
                <div className="flex items-center gap-3">
                  <span className="text-zinc-500 w-6 text-right">{i + 1}</span>
                  <span className="font-mono">{e.product_id}</span>
                </div>
                <span className="font-mono text-emerald-400">{e.count}</span>
              </div>
            </li>
          );
        })}
      </ol>
    </div>
  );
}
