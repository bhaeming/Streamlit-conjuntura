"use client";

import { useMemo, useState } from "react";
import type { EChartsOption, SeriesOption } from "echarts";
import type { Row } from "@/lib/data";
import { ChartCard } from "./ChartCard";

const palette = ["#087f5b", "#1677a8", "#2f9e44", "#339af0", "#0b7285", "#74b816", "#1864ab"];

function numeric(value: Row[string] | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatValue(value: number | null, suffix: string) {
  if (value == null || Number.isNaN(value)) return "n/d";
  return `${value.toLocaleString("pt-BR", { maximumFractionDigits: 2 })}${suffix}`;
}

export function SeriesChart({
  rows,
  series,
  title,
  subtitle,
  type = "line",
  stacked = false,
  suffix = "",
  source,
}: {
  rows: Row[];
  series: Record<string, string>;
  title: string;
  subtitle: string;
  type?: "line" | "bar";
  stacked?: boolean;
  suffix?: string;
  source?: string;
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

  const highlights = useMemo(() => {
    const items = selected.map((key) => {
      const validRows = visible.filter((row) => numeric(row[key]) != null);
      const current = numeric(validRows.at(-1)?.[key]);
      const previous = numeric(validRows.at(-2)?.[key]);
      return {
        key,
        label: series[key],
        value: current,
        change: current != null && previous != null ? current - previous : null,
      };
    }).filter((item) => item.value != null);

    const highest = [...items].sort((a, b) => (b.value ?? -Infinity) - (a.value ?? -Infinity))[0];
    const fastest = [...items].filter((item) => item.change != null).sort((a, b) => (b.change ?? 0) - (a.change ?? 0))[0];
    const weakest = [...items].filter((item) => item.change != null).sort((a, b) => (a.change ?? 0) - (b.change ?? 0))[0];
    return { highest, fastest, weakest };
  }, [selected, visible, series]);

  const insight = (
    <article className="insight-card chart-side-insight">
      <h2>Insights Econômicos</h2>
      <p>
        A leitura recente coloca {highlights.highest?.label ?? "n/d"} como a série de maior nível entre as opções selecionadas,
        com {formatValue(highlights.highest?.value ?? null, suffix)}. Esse dado deve ser lido como uma indicação de posição
        relativa: ele mostra onde a pressão, o ritmo de atividade ou o volume observado está mais elevado no recorte atual.
      </p>
      <p>
        A variação mais forte no período recente aparece em {highlights.fastest?.label ?? "n/d"} ({formatValue(highlights.fastest?.change ?? null, suffix)}),
        enquanto {highlights.weakest?.label ?? "n/d"} mostra a menor mudança ({formatValue(highlights.weakest?.change ?? null, suffix)}).
        Em uma leitura de conjuntura, essa comparação separa nível e direção: uma série pode estar alta, mas perdendo força,
        ou ainda estar baixa, mas em processo de recuperação.
      </p>
      <div className="highlight-boxes">
        <div><span>Maior leitura</span><strong>{highlights.highest?.label ?? "n/d"}</strong><small>{formatValue(highlights.highest?.value ?? null, suffix)}</small></div>
        <div><span>Maior alta recente</span><strong>{highlights.fastest?.label ?? "n/d"}</strong><small>{formatValue(highlights.fastest?.change ?? null, suffix)}</small></div>
        <div><span>Menor variação recente</span><strong>{highlights.weakest?.label ?? "n/d"}</strong><small>{formatValue(highlights.weakest?.change ?? null, suffix)}</small></div>
      </div>
    </article>
  );

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
      <ChartCard title={title} subtitle={subtitle} option={option} tall source={source} insight={insight} />
    </div>
  );
}
