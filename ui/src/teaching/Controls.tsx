import { useState } from "react";
import type { GeneratorStats } from "./types";

const BACKEND = `http://${window.location.hostname}:3000`;

async function post(action: string, body: Record<string, unknown>) {
  await fetch(`${BACKEND}/control/${action}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function Controls({ stats }: { stats?: GeneratorStats }) {
  const [open, setOpen] = useState(false);

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="text-[11px] px-2.5 py-1 rounded bg-zinc-900 hover:bg-zinc-800 border border-zinc-800 text-zinc-300"
      >
        ⚙ controls
      </button>
    );
  }

  return (
    <div className="absolute right-4 top-12 z-50 w-80 bg-zinc-900 border border-zinc-800 rounded-lg shadow-xl p-4 space-y-3 text-xs">
      <div className="flex justify-between items-center">
        <span className="text-zinc-300 font-semibold">controls</span>
        <button
          onClick={() => setOpen(false)}
          className="text-zinc-500 hover:text-zinc-300"
        >
          ✕
        </button>
      </div>

      <Group label="page-view rate (ev/s)">
        {[0, 100, 300, 1000].map((r) => (
          <Btn
            key={r}
            active={Math.round(stats?.rate ?? 0) === r}
            onClick={() => post("rate", { events_per_sec: r })}
          >
            {r}
          </Btn>
        ))}
      </Group>

      <Group label="purchase rate (ev/s)">
        {[0, 10, 30, 100].map((r) => (
          <Btn
            key={r}
            active={Math.round(stats?.purchase_rate ?? 0) === r}
            onClick={() => post("purchase-rate", { events_per_sec: r })}
          >
            {r}
          </Btn>
        ))}
      </Group>

      <Group label="late events %">
        {[0, 5, 20, 50].map((p) => (
          <Btn
            key={p}
            active={Math.round((stats?.late_pct ?? 0) * 100) === p}
            onClick={() =>
              post("late-events", { percentage: p, max_delay_ms: 30000 })
            }
          >
            {p}%
          </Btn>
        ))}
      </Group>

      <Group label="scenario">
        <Btn
          onClick={() => post("spike", { duration_s: 5, multiplier: 5 })}
        >
          ⚡ traffic spike
        </Btn>
        <Btn
          onClick={() => post("fraud-burst", { rate: 30, duration_s: 3 })}
        >
          🚨 fraud burst
        </Btn>
      </Group>

      <div className="pt-2 border-t border-zinc-800 text-[10px] text-zinc-500 space-y-0.5 font-mono">
        <div>sent: {stats?.sent.toLocaleString() ?? 0}</div>
        <div>late: {stats?.late_sent.toLocaleString() ?? 0}</div>
        <div>purchases: {stats?.purchases_sent.toLocaleString() ?? 0}</div>
        <div>spike: {stats?.spike_active ? "active" : "off"}</div>
        <div>fraud: {stats?.fraud_active ? "active" : "off"}</div>
      </div>
    </div>
  );
}

function Group({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-widest text-zinc-500 mb-1">
        {label}
      </div>
      <div className="flex flex-wrap gap-1.5">{children}</div>
    </div>
  );
}

function Btn({
  active,
  onClick,
  children,
}: {
  active?: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={
        "px-2.5 py-1 rounded border text-xs " +
        (active
          ? "bg-emerald-900/50 border-emerald-700 text-emerald-200"
          : "bg-zinc-950 border-zinc-700 text-zinc-300 hover:bg-zinc-800")
      }
    >
      {children}
    </button>
  );
}
