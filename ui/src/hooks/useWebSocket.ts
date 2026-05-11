import { useEffect, useRef, useState } from "react";

type AnyMsg = { type: string; payload: unknown };

export function useWebSocket(url: string) {
  const [lastByType, setLastByType] = useState<Record<string, unknown>>({});
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let cancelled = false;
    let retry = 0;

    const connect = () => {
      if (cancelled) return;
      const ws = new WebSocket(url);
      wsRef.current = ws;
      ws.onopen = () => {
        retry = 0;
        setConnected(true);
      };
      ws.onclose = () => {
        setConnected(false);
        if (cancelled) return;
        const delay = Math.min(1000 * 2 ** retry, 10000);
        retry += 1;
        setTimeout(connect, delay);
      };
      ws.onmessage = (e) => {
        try {
          const m = JSON.parse(e.data) as AnyMsg;
          setLastByType((prev) => ({ ...prev, [m.type]: m.payload }));
        } catch {
          /* ignore */
        }
      };
    };

    connect();
    return () => {
      cancelled = true;
      wsRef.current?.close();
    };
  }, [url]);

  return { lastByType, connected };
}
