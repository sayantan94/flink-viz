import { GROUPS } from "./concepts";

export function ConceptTree({
  activeId,
  onPick,
}: {
  activeId: string;
  onPick: (id: string) => void;
}) {
  return (
    <nav className="space-y-5 text-sm overflow-y-auto">
      {GROUPS.map((g) => (
        <div key={g.id}>
          <div className="text-[10px] uppercase tracking-widest text-zinc-500 mb-2">
            {g.title}
          </div>
          <ul className="space-y-0.5">
            {g.concepts.map((c) => {
              const active = c.id === activeId;
              const dot =
                c.status === "live" ? "text-emerald-400" :
                c.status === "preview" ? "text-amber-400" :
                "text-zinc-600";
              return (
                <li key={c.id}>
                  <button
                    onClick={() => onPick(c.id)}
                    className={
                      "w-full text-left px-2 py-1.5 rounded flex items-start gap-2 " +
                      (active
                        ? "bg-zinc-800 text-zinc-100"
                        : "text-zinc-400 hover:bg-zinc-900 hover:text-zinc-200")
                    }
                  >
                    <span className={dot}>●</span>
                    <span className="flex-1">
                      <span className="block leading-tight">{c.title}</span>
                      <span className="block text-[11px] text-zinc-500 leading-tight">
                        {c.oneLiner}
                      </span>
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        </div>
      ))}
    </nav>
  );
}
