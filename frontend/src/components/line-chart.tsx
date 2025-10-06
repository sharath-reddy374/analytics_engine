"use client";

import React from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  Legend,
} from "recharts";
import type { SeriesPoint } from "@/lib/metrics";

type Props = {
  title: string;
  data: SeriesPoint[];
  color?: string;
};

export function LineSeriesChart({ title, data, color = "#3b82f6" }: Props) {
  return (
    <div className="rounded-lg border border-border bg-card text-foreground p-4 shadow-sm">
      <div className="mb-2 text-sm font-medium text-muted-foreground">{title}</div>
      <div className="h-[260px]">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
            <XAxis
              dataKey="day"
              tick={{ fontSize: 12, fill: "currentColor" }}
              tickMargin={8}
            />
            <YAxis
              tick={{ fontSize: 12, fill: "currentColor" }}
              tickMargin={8}
              allowDecimals={false}
            />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="c" name="count" stroke={color} strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
