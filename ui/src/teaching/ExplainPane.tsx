import { useState } from "react";
import type { Concept } from "./concepts";

export function ExplainPane({ concept }: { concept: Concept }) {
  const [tab, setTab] = useState<"explain" | "code" | "qs">("explain");

  const hasCode = Boolean(concept.sql || concept.java);

  return (
    <div className="border-t border-zinc-800 bg-zinc-950">
      <div className="flex gap-1 px-6 pt-4">
        <Tab active={tab === "explain"} onClick={() => setTab("explain")}>
          Explanation
        </Tab>
        {hasCode && (
          <Tab active={tab === "code"} onClick={() => setTab("code")}>
            Code
          </Tab>
        )}
        {concept.interviewQs.length > 0 && (
          <Tab active={tab === "qs"} onClick={() => setTab("qs")}>
            Interview Qs ({concept.interviewQs.length})
          </Tab>
        )}
      </div>
      <div className="px-6 pb-6 pt-4 max-h-[40vh] overflow-y-auto">
        {tab === "explain" && (
          <div className="prose prose-invert prose-sm max-w-3xl text-zinc-300 leading-relaxed">
            {concept.explanation}
          </div>
        )}
        {tab === "code" && hasCode && (
          <div className="space-y-4 max-w-3xl">
            {concept.sql && (
              <div>
                <div className="text-[10px] uppercase tracking-widest text-zinc-500 mb-1">
                  Flink SQL
                </div>
                <pre className="text-xs bg-zinc-900 border border-zinc-800 rounded p-3 overflow-x-auto">
                  <code>{concept.sql}</code>
                </pre>
              </div>
            )}
            {concept.java && (
              <div>
                <div className="text-[10px] uppercase tracking-widest text-zinc-500 mb-1">
                  DataStream API (Java)
                </div>
                <pre className="text-xs bg-zinc-900 border border-zinc-800 rounded p-3 overflow-x-auto">
                  <code>{concept.java}</code>
                </pre>
              </div>
            )}
          </div>
        )}
        {tab === "qs" && (
          <ol className="space-y-3 max-w-3xl text-sm">
            {concept.interviewQs.map((qa, i) => (
              <li key={i} className="border-l-2 border-zinc-800 pl-3">
                <div className="text-zinc-200">{qa.q}</div>
                <div className="text-zinc-400 mt-1 leading-relaxed">{qa.a}</div>
              </li>
            ))}
          </ol>
        )}
      </div>
    </div>
  );
}

function Tab({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={
        "px-3 py-1.5 text-xs rounded-t " +
        (active
          ? "bg-zinc-950 text-zinc-100 border border-b-0 border-zinc-800"
          : "text-zinc-500 hover:text-zinc-300")
      }
    >
      {children}
    </button>
  );
}
