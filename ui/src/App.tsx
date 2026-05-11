import { useWebSocket } from "./hooks/useWebSocket";
import { Leaderboard } from "./components/Leaderboard";

type TopKMsg = {
  window_start_ms: number;
  window_end_ms: number;
  top: { product_id: string; count: number }[];
};

export default function App() {
  const wsUrl = `ws://${window.location.hostname}:3000/ws`;
  const { lastByType, connected } = useWebSocket(wsUrl);
  const topk = lastByType.topk as TopKMsg | undefined;

  return (
    <div className="min-h-screen flex flex-col">
      <header className="px-6 py-4 border-b border-zinc-800 flex justify-between items-center">
        <h1 className="text-lg font-semibold">flink-viz · Top-K Products</h1>
        <span className={connected ? "text-emerald-400" : "text-rose-400"}>
          {connected ? "● live" : "● disconnected"}
        </span>
      </header>
      <main className="flex-1">
        <Leaderboard
          windowStartMs={topk?.window_start_ms}
          windowEndMs={topk?.window_end_ms}
          top={topk?.top}
        />
      </main>
    </div>
  );
}
