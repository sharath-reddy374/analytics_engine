import React from "react";

type Props = {
  label: string;
  value: number | string;
  hint?: string;
};

export function KpiCard({ label, value, hint }: Props) {
  return (
    <div className="rounded-lg border border-border bg-card text-foreground p-4 shadow-sm">
      <div className="text-sm text-slate-600">{label}</div>
      <div className="mt-1 text-3xl font-semibold text-blue-700">{value}</div>
      {hint ? <div className="mt-1 text-xs text-slate-500">{hint}</div> : null}
    </div>
  );
}
