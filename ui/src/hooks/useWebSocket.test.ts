import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { useWebSocket } from "./useWebSocket";

class FakeWS {
  static last: FakeWS | null = null;
  onmessage: ((e: MessageEvent) => void) | null = null;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  readyState = 0;
  constructor(public url: string) {
    FakeWS.last = this;
  }
  close() {
    this.readyState = 3;
    this.onclose?.();
  }
}

beforeEach(() => {
  vi.stubGlobal("WebSocket", FakeWS);
});

describe("useWebSocket", () => {
  it("collects messages by type", () => {
    const { result } = renderHook(() => useWebSocket("ws://x/ws"));
    act(() => {
      FakeWS.last!.onmessage!(
        new MessageEvent("message", {
          data: JSON.stringify({ type: "topk", payload: { top: [] } }),
        }),
      );
    });
    expect(result.current.lastByType.topk).toEqual({ top: [] });
  });
});
