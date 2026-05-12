export function PlaceholderViz({ label }: { label: string }) {
  return (
    <div className="h-full flex flex-col items-center justify-center text-zinc-500 gap-2">
      <div className="text-3xl">⌛</div>
      <div className="text-sm">{label}</div>
      <div className="text-xs">Coming as part of a later phase.</div>
    </div>
  );
}
