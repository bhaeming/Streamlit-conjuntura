"use client";

import { useMemo } from "react";
import type { EChartsOption } from "echarts";
import type { Row } from "@/lib/data";
import { ChartCard } from "./ChartCard";

const ufSeries: Record<string, string> = {
  ibc_am_dessaz: "AM",
  ibc_pa_dessaz: "PA",
  ibc_ce_dessaz: "CE",
  ibc_pe_dessaz: "PE",
  ibc_ba_dessaz: "BA",
  ibc_es_dessaz: "ES",
  ibc_mg_dessaz: "MG",
  ibc_rj_dessaz: "RJ",
  ibc_sp_dessaz: "SP",
  ibc_pr_dessaz: "PR",
  ibc_sc_dessaz: "SC",
  ibc_rs_dessaz: "RS",
  ibc_go_dessaz: "GO",
};

function numeric(value: Row[string]) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function pctChange(current: number | null, base: number | null) {
  if (current == null || base == null || base === 0) return null;
  return ((current / base) - 1) * 100;
}

function formatPct(value: number | null) {
  if (value == null || Number.isNaN(value)) return "n/d";
  return `${value.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
}

export function UfRadarChart({ rows }: { rows: Row[] }) {
  const tableData = useMemo(() => {
    const sorted = [...rows].sort((a, b) => String(a.date).localeCompare(String(b.date)));

    return Object.entries(ufSeries)
      .map(([key, label]) => {
        const validRows = sorted.filter((row) => numeric(row[key]) != null);
        const lastIndex = validRows.length - 1;
        const current = lastIndex >= 0 ? numeric(validRows[lastIndex][key]) : null;
        const monthBase = lastIndex >= 1 ? numeric(validRows[lastIndex - 1][key]) : null;
        const threeBase = lastIndex >= 3 ? numeric(validRows[lastIndex - 3][key]) : null;
        const twelveBase = lastIndex >= 12 ? numeric(validRows[lastIndex - 12][key]) : null;

        return {
          label,
          mes: pctChange(current, monthBase),
          tresMeses: pctChange(current, threeBase),
          dozeMeses: pctChange(current, twelveBase),
        };
      })
      .filter((item) => item.dozeMeses != null)
      .sort((a, b) => a.label.localeCompare(b.label));
  }, [rows]);

  const radarData = tableData
    .filter((item): item is { label: string; mes: number | null; tresMeses: number | null; dozeMeses: number } => item.dozeMeses != null)
    .map((item) => ({ label: item.label, growth: item.dozeMeses }));

  const maxAbs = Math.max(5, ...radarData.map((item) => Math.abs(item.growth)));
  const axisLimit = Math.ceil(maxAbs + 1);
  const highestUf = [...tableData].filter((row) => row.dozeMeses != null).sort((a, b) => (b.dozeMeses ?? -Infinity) - (a.dozeMeses ?? -Infinity))[0];
  const lowestUf = [...tableData].filter((row) => row.dozeMeses != null).sort((a, b) => (a.dozeMeses ?? Infinity) - (b.dozeMeses ?? Infinity))[0];
  const average = radarData.length ? radarData.reduce((sum, row) => sum + row.growth, 0) / radarData.length : null;

  const radarInsight = (
    <article className="insight-card chart-side-insight">
      <h2>Insights Econômicos</h2>
      <p>
        O radar compara o crescimento regional do IBC nas UFs acompanhadas. O maior avanço em 12 meses aparece em
        <strong> {highestUf?.label ?? "n/d"}</strong> ({formatPct(highestUf?.dozeMeses ?? null)}), enquanto a menor leitura está em
        <strong> {lowestUf?.label ?? "n/d"}</strong> ({formatPct(lowestUf?.dozeMeses ?? null)}). A média do grupo está em {formatPct(average)}.
      </p>
      <p>
        Em uma análise de conjuntura, dispersão regional elevada indica que o ciclo econômico não está homogêneo. Isso pode refletir
        diferenças de composição produtiva, exposição a commodities, comércio, serviços, indústria e condições locais de renda e crédito.
        Por isso, o radar complementa o IBC-Br nacional: ele mostra onde a atividade ganha tração e onde ainda há fragilidade relativa.
      </p>
    </article>
  );

  const option: EChartsOption = {
    color: ["#087f5b"],
    tooltip: {
      trigger: "item",
      valueFormatter: (value) => `${Number(value).toLocaleString("pt-BR", { maximumFractionDigits: 2 })}%`,
    },
    radar: {
      radius: "68%",
      indicator: radarData.map((item) => ({ name: item.label, min: -axisLimit, max: axisLimit })),
      splitNumber: 4,
      axisName: { color: "#52606d", fontWeight: 700 },
      splitLine: { lineStyle: { color: "#dfe8ec" } },
      splitArea: { areaStyle: { color: ["#f8fbfa", "#eef7f4"] } },
      axisLine: { lineStyle: { color: "#dfe8ec" } },
    },
    series: [{
      name: "Crescimento em 12 meses",
      type: "radar",
      data: [{
        value: radarData.map((item) => Number(item.growth.toFixed(2))),
        name: "UFs",
        areaStyle: { color: "rgba(8, 127, 91, 0.18)" },
        lineStyle: { width: 3 },
        symbolSize: 5,
      }],
    }],
  };

  return (
    <section className="chart-table-grid">
      <ChartCard
        title="Crescimento regional do IBC"
        subtitle="Variação das UFs nos últimos 12 meses, série dessazonalizada"
        option={option}
        source="BCB (2026)"
        insight={radarInsight}
        tall
      />
      <article className="table-card">
        <div className="table-card-header">
          <div>
            <h2>IBC por UF</h2>
            <p>Variação da série dessazonalizada.</p>
          </div>
        </div>
        <div className="table-scroll">
          <table>
            <thead><tr><th>UF</th><th>Mês</th><th>3 meses</th><th>12 meses</th></tr></thead>
            <tbody>
              {[...tableData].sort((a, b) => (b.dozeMeses ?? -999) - (a.dozeMeses ?? -999)).map((row) => (
                <tr key={row.label}>
                  <td>{row.label}</td>
                  <td>{formatPct(row.mes)}</td>
                  <td>{formatPct(row.tresMeses)}</td>
                  <td>{formatPct(row.dozeMeses)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <small className="source-label">Fonte: BCB (2026)</small>
      </article>
    </section>
  );
}
