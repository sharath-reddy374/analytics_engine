import React from "react";

type Props = {
  label: string;
  value: number | string;
  hint?: string;
};

export function KpiCard({ label, value, hint }: Props) {
  return (
    <div className="rounded-lg border border-border bg-card text-foreground p-4 shadow-sm">
      <div className="text-sm text-muted-foreground">{label}</div>
      <div className="mt-1 text-2xl font-semibold">{value}</div>
      {hint ? <div className="mt-1 text-xs text-muted-foreground">{hint}</div> : null}
    </div>
  );
}
