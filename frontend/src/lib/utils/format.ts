/** Formatting utilities. */

export function formatCost(cost: number): string {
  if (cost < 0.01) return "$" + cost.toFixed(4);
  return "$" + cost.toFixed(2);
}

export function formatTimestamp(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

export function pluralize(count: number, singular: string): string {
  return `${count} ${singular}${count !== 1 ? "s" : ""}`;
}
