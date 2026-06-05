"use client";

import { useMemo, useState } from "react";
import type { EChartsOption, SeriesOption } from "echarts";
import type { Row } from "@/lib/data";
import { ChartCard } from "./ChartCard";

const palette = ["#087f5b", "#1677a8", "#2f9e44", "#339af0", "#0b7285", "#74b816", "#1864ab"];

export function SeriesChart({
  rows,
  series,
  title,
  subtitle,
  type = "line",
  stacked = false,
  suffix = "",
}: {
  rows: Row[];
  series: Record<string, string>;
  title: string;
  subtitle: string;
  type?: "line" | "bar";
  stacked?: boolean;
  suffix?: string;
}) {
  const keys = Object.keys(series);
  const [selected, setSelected] = useState(keys);
  const [years, setYears] = useState("10");

  const visible = useMemo(() => {
    if (years === "all") return rows;
    const last = new Date(String(rows.at(-1)?.date ?? rows.at(-1)?.Date));
    const cutoff = new Date(last);
    cutoff.setFullYear(last.getFullYear() - Number(years));
    return rows.filter((row) => new Date(String(row.date ?? row.Date)) >= cutoff);
  }, [rows, years]);

  const option: EChartsOption = {
    color: palette,
    animationDuration: 700,
    tooltip: {
      trigger: "axis",
      valueFormatter: (value) => `${Number(value).toLocaleString("pt-BR", { maximumFractionDigits: 2 })}${suffix}`,
    },
    legend: { bottom: 0, icon: "roundRect", textStyle: { color: "#52606d" } },
    grid: { left: 20, right: 22, top: 24, bottom: 58, containLabel: true },
    xAxis: {
      type: "category",
      boundaryGap: type === "bar",
      data: visible.map((row) => String(row.date ?? row.Date ?? "")),
      axisLabel: { color: "#718096", hideOverlap: true, formatter: (value: string) => value.slice(0, 7) },
      axisLine: { lineStyle: { color: "#dbe4ea" } },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#718096" },
      splitLine: { lineStyle: { color: "#edf2f5" } },
    },
    dataZoom: [{ type: "inside" }],
    series: selected.map((key, index) => ({
      name: series[key],
      type,
      data: visible.map((row) => row[key]),
      smooth: type === "line",
      showSymbol: false,
      stack: stacked ? "total" : undefined,
      areaStyle: stacked ? { opacity: 0.18 } : undefined,
      lineStyle: { width: index === 0 ? 3 : 2 },
      emphasis: { focus: "series" },
    })) as SeriesOption[],
  };

  return (
    <div>
      <div className="chart-controls">
        <div className="chips">
          {keys.map((key) => (
            <button key={key} className={selected.includes(key) ? "chip active" : "chip"} onClick={() => setSelected((current) => current.includes(key) ? current.filter((item) => item !== key) : [...current, key])}>
              {series[key]}
            </button>
          ))}
        </div>
        <select value={years} onChange={(event) => setYears(event.target.value)} aria-label="Período">
          <option value="5">5 anos</option><option value="10">10 anos</option><option value="15">15 anos</option><option value="all">Todo período</option>
        </select>
      </div>
      <ChartCard title={title} subtitle={subtitle} option={option} tall />
    </div>
  );
}
