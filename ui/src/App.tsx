import { useMemo, useState } from "react";
import { useWebSocket } from "./hooks/useWebSocket";
import { ConceptTree } from "./teaching/ConceptTree";
import { ExplainPane } from "./teaching/ExplainPane";
import { GROUPS, FIRST_CONCEPT_ID } from "./teaching/concepts";
import type {
  TopKMsg,
  DagMsg,
  WatermarkMsg,
  CheckpointsMsg,
} from "./teaching/types";

export default function App() {
  const wsUrl = `ws://${window.location.hostname}:3000/ws`;
  const { lastByType, connected } = useWebSocket(wsUrl);
  const [activeId, setActiveId] = useState(FIRST_CONCEPT_ID);
  const setActive = (id: string) => {
    setActiveId(id);
    window.location.hash = id;
  };

  const allConcepts = useMemo(
    () => GROUPS.flatMap((g) => g.concepts),
    [],
  );
  const active = allConcepts.find((c) => c.id === activeId) ?? allConcepts[0];

  const ctx = {
    topk: lastByType.topk as TopKMsg | undefined,
    dag: lastByType.dag as DagMsg | undefined,
    watermarks: lastByType.watermarks as WatermarkMsg | undefined,
    checkpoints: lastByType.checkpoints as CheckpointsMsg | undefined,
  };

  const jobRunning = ctx.dag?.state === "RUNNING";

  return (
    <div className="h-screen flex flex-col bg-zinc-950 text-zinc-200">
      <header className="px-6 py-3 border-b border-zinc-800 flex justify-between items-center">
        <div className="flex items-baseline gap-3">
          <h1 className="text-base font-semibold">flink-viz</h1>
          <span className="text-xs text-zinc-500">
            · teach me Flink end-to-end
          </span>
        </div>
        <div className="flex items-center gap-4 text-xs">
          <span className={jobRunning ? "text-emerald-400" : "text-rose-400"}>
            ● {jobRunning ? `job ${ctx.dag?.job_name}` : "no job"}
          </span>
          <span className={connected ? "text-emerald-400" : "text-rose-400"}>
            ● {connected ? "ws live" : "ws disconnected"}
          </span>
          <a
            href="http://localhost:8081"
            target="_blank"
            rel="noreferrer"
            className="text-zinc-500 hover:text-zinc-300"
          >
            flink UI ↗
          </a>
        </div>
      </header>

      <div className="flex-1 flex min-h-0">
        <aside className="w-72 border-r border-zinc-800 p-4 overflow-y-auto">
          <ConceptTree activeId={active.id} onPick={setActive} />
        </aside>
        <main className="flex-1 flex flex-col min-w-0">
          <section className="flex-1 p-6 overflow-y-auto min-h-0">
            <div className="mb-4">
              <h2 className="text-xl font-semibold">{active.title}</h2>
              <p className="text-sm text-zinc-500">{active.oneLiner}</p>
            </div>
            <div className="bg-zinc-900/30 border border-zinc-800 rounded-lg p-5 min-h-[280px]">
              {active.viz(ctx)}
            </div>
          </section>
          <ExplainPane concept={active} />
        </main>
      </div>
    </div>
  );
}
