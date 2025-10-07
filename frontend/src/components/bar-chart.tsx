"use client";

import React from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  Legend,
} from "recharts";

type SimpleDatum = { name: string; value: number };

type Props = {
  title: string;
  data: SimpleDatum[];
  color?: string;
  yLabel?: string;
};

export function BarSimpleChart({ title, data, color = "#10b981", yLabel = "count" }: Props) {
  return (
    <div className="rounded-lg border border-border bg-card text-foreground p-4 shadow-sm">
      <div className="mb-2 text-sm font-medium text-slate-700">{title}</div>
      <div className="h-[260px]">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
            <XAxis
              dataKey="name"
              tick={{ fontSize: 12, fill: "currentColor" }}
              tickMargin={8}
              interval={0}
              height={50}
            />
            <YAxis
              tick={{ fontSize: 12, fill: "currentColor" }}
              tickMargin={8}
              allowDecimals={false}
            />
            <Tooltip />
            <Legend />
            <Bar dataKey="value" name={yLabel} fill={color} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
