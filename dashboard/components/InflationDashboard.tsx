"use client";

import { useMemo, useState } from "react";
import type { EChartsOption, SeriesOption } from "echarts";
import type { Row } from "@/lib/data";
import { ChartCard } from "./ChartCard";

type GroupRow = {
  grupo: string;
  mes: number | null;
  seisMeses: number | null;
  dozeMeses: number | null;
};

const palette = ["#087f5b", "#1677a8", "#2f9e44", "#339af0", "#0b7285", "#74b816"];
const groupPalette = ["#74c0fc", "#1971c2", "#ffa8a8", "#ff3b3f", "#69db7c", "#38b2ac", "#ffd166", "#ff8a00", "#7048e8", "#344054"];
const inflationSource = "IBGE/BCB (2026)";
const groupSource = "IBGE (2026)";

function rowDate(row: Row) {
  return String(row.date ?? row.Date ?? "");
}

function formatPct(value: number | null) {
  if (value == null || Number.isNaN(value)) return "n/d";
  return `${value.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
}

function accumulated(values: number[]) {
  if (!values.length) return null;
  return (values.reduce((acc, value) => acc * (1 + value / 100), 1) - 1) * 100;
}

function isGeneralIndex(value: unknown) {
  return String(value).toLowerCase().includes("ndice geral");
}

export function InflationDashboard({ ipcaRows, groupRows }: { ipcaRows: Row[]; groupRows: Row[] }) {
  const [mode, setMode] = useState<"12m" | "mensal">("12m");
  const [expanded, setExpanded] = useState(false);

  const chartConfig = mode === "12m"
    ? {
        subtitle: "Variação acumulada em 12 meses",
        series: {
          ipca_12m: "IPCA total",
          ipca_livres_12m_calc: "Preços livres",
          ipca_administrados_12m_calc: "Administrados",
        },
        type: "line" as const,
      }
    : {
        subtitle: "Variação mensal",
        series: {
          ipca: "IPCA total",
          ipca_livres: "Preços livres",
          ipca_administrados: "Administrados",
        },
        type: "bar" as const,
      };

  const visibleIpca = useMemo(() => {
    const last = new Date(rowDate(ipcaRows.at(-1) ?? {}));
    const cutoff = new Date(last);
    cutoff.setFullYear(last.getFullYear() - 10);
    return ipcaRows.filter((row) => new Date(rowDate(row)) >= cutoff);
  }, [ipcaRows]);

  const inflationOption: EChartsOption = {
    color: palette,
    tooltip: {
      trigger: "axis",
      valueFormatter: (value) => `${Number(value).toLocaleString("pt-BR", { maximumFractionDigits: 2 })}%`,
    },
    legend: { bottom: 0, icon: "roundRect", textStyle: { color: "#52606d" } },
    grid: { left: 20, right: 22, top: 24, bottom: 58, containLabel: true },
    xAxis: {
      type: "category",
      boundaryGap: chartConfig.type === "bar",
      data: visibleIpca.map(rowDate),
      axisLabel: { color: "#718096", hideOverlap: true, formatter: (value: string) => value.slice(0, 7) },
      axisLine: { lineStyle: { color: "#dbe4ea" } },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#718096" },
      splitLine: { lineStyle: { color: "#edf2f5" } },
    },
    dataZoom: [{ type: "inside" }],
    series: Object.entries(chartConfig.series).map(([key, label], index) => ({
      name: label,
      type: chartConfig.type,
      data: visibleIpca.map((row) => row[key]),
      smooth: chartConfig.type === "line",
      showSymbol: false,
      lineStyle: { width: index === 0 ? 3 : 2 },
      emphasis: { focus: "series" },
    })) as SeriesOption[],
  };

  const groupSummary = useMemo<GroupRow[]>(() => {
    const monthly = groupRows
      .filter((row) => row.indicador === "variacao_mensal" && !isGeneralIndex(row.grupo))
      .sort((a, b) => rowDate(a).localeCompare(rowDate(b)));
    const yearly = groupRows.filter((row) => row.indicador === "variacao_12m");
    const lastDate = monthly.at(-1)?.date;
    const groups = Array.from(new Set(monthly.map((row) => String(row.grupo)))).sort();

    return groups.map((grupo) => {
      const groupMonthly = monthly.filter((row) => row.grupo === grupo);
      const current = groupMonthly.find((row) => row.date === lastDate)?.value;
      const last6 = groupMonthly.slice(-6).map((row) => Number(row.value)).filter(Number.isFinite);
      const last12 = groupMonthly.slice(-12).map((row) => Number(row.value)).filter(Number.isFinite);
      const yearRow = yearly.find((row) => row.grupo === grupo && row.date === lastDate);
      return {
        grupo,
        mes: typeof current === "number" ? current : null,
        seisMeses: accumulated(last6),
        dozeMeses: typeof yearRow?.value === "number" ? yearRow.value : accumulated(last12),
      };
    });
  }, [groupRows]);

  const sortedRows = useMemo(
    () => [...groupSummary].sort((a, b) => (b.mes ?? -999) - (a.mes ?? -999)),
    [groupSummary],
  );

  const contributionData = useMemo(() => {
    const monthly = groupRows
      .filter((row) => row.indicador === "variacao_mensal")
      .sort((a, b) => rowDate(a).localeCompare(rowDate(b)));
    const weights = groupRows.filter((row) => row.indicador === "peso_mensal");
    const lastDate = rowDate(monthly.at(-1) ?? {});
    const cutoff = new Date(lastDate);
    cutoff.setFullYear(cutoff.getFullYear() - 7);

    const weightByDateGroup = new Map<string, number>();
    weights.forEach((row) => {
      if (typeof row.value === "number") {
        weightByDateGroup.set(`${rowDate(row)}|${String(row.grupo)}`, row.value);
      }
    });

    const dates = Array.from(new Set(monthly.map(rowDate)))
      .filter((date) => new Date(date) >= cutoff)
      .sort();
    const groups = Array.from(new Set(monthly.filter((row) => !isGeneralIndex(row.grupo)).map((row) => String(row.grupo)))).sort();
    const contributionByDateGroup = new Map<string, number>();
    const generalByDate = new Map<string, number>();

    monthly.forEach((row) => {
      const date = rowDate(row);
      const group = String(row.grupo);
      if (isGeneralIndex(group)) {
        if (typeof row.value === "number") generalByDate.set(date, row.value);
        return;
      }

      const weight = weightByDateGroup.get(`${date}|${group}`);
      if (typeof row.value === "number" && typeof weight === "number") {
        contributionByDateGroup.set(`${date}|${group}`, (row.value * weight) / 100);
      }
    });

    return { dates, groups, contributionByDateGroup, generalByDate };
  }, [groupRows]);

  const groupOption: EChartsOption = {
    color: groupPalette,
    tooltip: {
      trigger: "axis",
      valueFormatter: (value) => `${Number(value).toLocaleString("pt-BR", { maximumFractionDigits: 2 })}`,
    },
    legend: { bottom: 0, type: "scroll", icon: "roundRect", textStyle: { color: "#52606d", fontSize: 10 } },
    grid: { left: 20, right: 48, top: 20, bottom: 78, containLabel: true },
    xAxis: {
      type: "category",
      data: contributionData.dates,
      axisLabel: { color: "#718096", hideOverlap: true, formatter: (value: string) => value.slice(0, 7) },
      axisLine: { lineStyle: { color: "#dbe4ea" } },
    },
    yAxis: [
      {
        type: "value",
        name: "Contribuição (p.p.)",
        axisLabel: { color: "#718096" },
        splitLine: { lineStyle: { color: "#edf2f5" } },
      },
      {
        type: "value",
        name: "Índice geral (%)",
        axisLabel: { color: "#718096" },
        splitLine: { show: false },
      },
    ],
    dataZoom: [{ type: "inside" }],
    series: [
      ...contributionData.groups.map((group) => ({
        name: group,
        type: "bar" as const,
        stack: "contribuicoes",
        data: contributionData.dates.map((date) => contributionData.contributionByDateGroup.get(`${date}|${group}`) ?? null),
        emphasis: { focus: "series" as const },
      })),
      {
        name: "Índice geral (mensal)",
        type: "line" as const,
        yAxisIndex: 1,
        data: contributionData.dates.map((date) => contributionData.generalByDate.get(date) ?? null),
        symbolSize: 5,
        lineStyle: { width: 2, color: "#344054" },
        itemStyle: { color: "#344054" },
      },
    ] as SeriesOption[],
  };

  const tableRows = expanded ? sortedRows : sortedRows.slice(0, 5);
  const latestInflationRow = visibleIpca.at(-1);
  const latestInflationItems = Object.entries(chartConfig.series).map(([key, label]) => ({
    label,
    value: typeof latestInflationRow?.[key] === "number" ? latestInflationRow[key] as number : null,
  })).filter((item) => item.value != null);
  const highestInflation = [...latestInflationItems].sort((a, b) => (b.value ?? -Infinity) - (a.value ?? -Infinity))[0];
  const lowestInflation = [...latestInflationItems].sort((a, b) => (a.value ?? Infinity) - (b.value ?? Infinity))[0];

  const inflationInsight = (
    <article className="insight-card chart-side-insight">
      <h2>Insights Econômicos</h2>
      <p>
        Na referência mais recente, {highestInflation?.label ?? "n/d"} apresenta a maior leitura ({formatPct(highestInflation?.value ?? null)}),
        enquanto {lowestInflation?.label ?? "n/d"} registra a menor ({formatPct(lowestInflation?.value ?? null)}).
        A comparação entre IPCA total, preços livres e administrados ajuda a identificar a origem da pressão inflacionária.
      </p>
      <p>
        Quando os preços livres aceleram, o movimento costuma refletir demanda, alimentos, serviços e bens industriais. Quando os administrados
        ganham peso, a pressão pode estar mais ligada a tarifas, combustíveis e itens regulados. Essa separação é central em relatórios de
        conjuntura porque indica se a inflação exige atenção sobre demanda, custos, choques específicos ou difusão entre grupos.
      </p>
    </article>
  );

  return (
    <>
      <div className="mode-switch" role="group" aria-label="Modo de variação da inflação">
        <button className={mode === "12m" ? "active" : ""} onClick={() => setMode("12m")}>12 meses</button>
        <button className={mode === "mensal" ? "active" : ""} onClick={() => setMode("mensal")}>Mensal</button>
      </div>
      <ChartCard title="Inflação ao consumidor" subtitle={chartConfig.subtitle} option={inflationOption} tall source={inflationSource} insight={inflationInsight} />

      <section className="inflation-groups">
        <ChartCard title="Componentes da inflação mensal" subtitle="Contribuições por grupo em p.p. e índice geral mensal" option={groupOption} source={groupSource} />
        <article className={expanded ? "table-card expanded" : "table-card"}>
          <div className="table-card-header">
            <div>
              <h2>Preços por grupo</h2>
              <p>Mês corrente, últimos 6 meses e 12 meses.</p>
            </div>
            <button onClick={() => setExpanded((value) => !value)}>{expanded ? "Recolher" : "Expandir"}</button>
          </div>
          <div className="table-scroll">
            <table>
              <thead><tr><th>Grupo</th><th>Mês</th><th>6 meses</th><th>12 meses</th></tr></thead>
              <tbody>
                {tableRows.map((row) => (
                  <tr key={row.grupo}>
                    <td>{row.grupo}</td>
                    <td>{formatPct(row.mes)}</td>
                    <td>{formatPct(row.seisMeses)}</td>
                    <td>{formatPct(row.dozeMeses)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <small className="source-label">Fonte: {groupSource}</small>
        </article>
      </section>
    </>
  );
}
