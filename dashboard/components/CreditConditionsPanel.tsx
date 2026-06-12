"use client";

import { useMemo, useState } from "react";
import type { EChartsOption, SeriesOption } from "echarts";
import type { Row } from "@/lib/data";
import { ChartCard } from "./ChartCard";

const palette = ["#087f5b", "#1677a8", "#2f9e44"];

const configs = {
  juros: {
    title: "Taxas de juros",
    subtitle: "Taxa média de juros das operações de crédito com recursos livres",
    unit: "% a.a.",
    series: { taxa_juros_pf: "PF", taxa_juros_pj: "PJ", taxa_juros_total: "Total" },
  },
  inad: {
    title: "Inadimplência",
    subtitle: "Inadimplência da carteira de crédito com recursos livres",
    unit: "%",
    series: { inadimplencia_pf: "PF", inadimplencia_pj: "PJ", inadimplencia_total: "Total" },
  },
} as const;

type Mode = keyof typeof configs;

function rowDate(row: Row) {
  return String(row.date ?? row.Date ?? "");
}

function numeric(value: Row[string] | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatValue(value: number | null, unit: string) {
  if (value == null || Number.isNaN(value)) return "n/d";
  return `${value.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${unit === "% a.a." ? "% a.a." : "%"}`;
}

function formatPp(value: number | null) {
  if (value == null || Number.isNaN(value)) return "n/d";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} p.p.`;
}

export function CreditConditionsPanel({ rows, source }: { rows: Row[]; source: string }) {
  const [mode, setMode] = useState<Mode>("juros");
  const config = configs[mode];
  const entries = Object.entries(config.series);
  const visible = rows.slice(-120);

  const latest = useMemo(() => {
    const last = rows.at(-1);
    const prev = rows.at(-2);
    const items = entries.map(([key, label]) => {
      const value = numeric(last?.[key]);
      const previous = numeric(prev?.[key]);
      return {
        key,
        label,
        value,
        change: value != null && previous != null ? value - previous : null,
      };
    });
    const highest = [...items].filter((item) => item.value != null).sort((a, b) => (b.value ?? 0) - (a.value ?? 0))[0];
    const fastest = [...items].filter((item) => item.change != null).sort((a, b) => (b.change ?? 0) - (a.change ?? 0))[0];
    const reference = rowDate(last ?? {}).slice(0, 7);
    return { items, highest, fastest, reference };
  }, [rows, entries]);

  const option: EChartsOption = {
    color: palette,
    tooltip: {
      trigger: "axis",
      valueFormatter: (value) => `${Number(value).toLocaleString("pt-BR", { maximumFractionDigits: 2 })}%`,
    },
    legend: { bottom: 0, icon: "roundRect", textStyle: { color: "#52606d" } },
    grid: { left: 20, right: 22, top: 24, bottom: 58, containLabel: true },
    xAxis: {
      type: "category",
      data: visible.map(rowDate),
      axisLabel: { color: "#718096", hideOverlap: true, formatter: (value: string) => value.slice(0, 7) },
      axisLine: { lineStyle: { color: "#dbe4ea" } },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#718096" },
      splitLine: { lineStyle: { color: "#edf2f5" } },
    },
    dataZoom: [{ type: "inside" }],
    series: entries.map(([key, label], index) => ({
      name: label,
      type: "line",
      data: visible.map((row) => row[key]),
      smooth: true,
      showSymbol: false,
      lineStyle: { width: index === 0 ? 3 : 2 },
      emphasis: { focus: "series" },
    })) as SeriesOption[],
  };

  const firstParagraph = mode === "juros"
    ? `Em ${latest.reference || "n/d"}, a taxa mais elevada está em ${latest.highest?.label ?? "n/d"}, com ${formatValue(latest.highest?.value ?? null, config.unit)}. Juros de crédito altos encarecem o financiamento de consumo e capital de giro, reduzem a disposição para novas operações e costumam aparecer com defasagem no ritmo da atividade.`
    : `Em ${latest.reference || "n/d"}, a maior inadimplência está em ${latest.highest?.label ?? "n/d"}, com ${formatValue(latest.highest?.value ?? null, config.unit)}. A inadimplência mede a parcela da carteira com atraso relevante e funciona como termômetro da capacidade de pagamento de famílias e empresas.`;

  const secondParagraph = mode === "juros"
    ? `A maior variação frente ao mês anterior ocorre em ${latest.fastest?.label ?? "n/d"} (${formatPp(latest.fastest?.change ?? null)}). Em uma leitura de conjuntura, a comparação entre PF, PJ e Total ajuda a identificar se o aperto financeiro está mais concentrado no orçamento das famílias, nas empresas ou disseminado pela carteira de crédito.`
    : `A maior variação frente ao mês anterior ocorre em ${latest.fastest?.label ?? "n/d"} (${formatPp(latest.fastest?.change ?? null)}). Quando a inadimplência avança junto com juros elevados, o crédito tende a ficar mais seletivo, o que pode restringir consumo, investimento e renegociação de dívidas.`;

  return (
    <section className="credit-conditions-section">
      <div className="credit-toggle">
        <div className="mode-switch" role="group" aria-label="Indicador de crédito">
          <button className={mode === "juros" ? "active" : ""} onClick={() => setMode("juros")}>Taxa média de juros</button>
          <button className={mode === "inad" ? "active" : ""} onClick={() => setMode("inad")}>Inadimplência</button>
        </div>
      </div>
      <div className="credit-conditions-grid">
        <ChartCard title={config.title} subtitle={config.subtitle} option={option} tall source={source} />
        <article className="insight-card">
          <h2>Insights Econômicos</h2>
          <p>{firstParagraph}</p>
          <p>{secondParagraph}</p>
          <div className="highlight-boxes">
            {latest.items.map((item) => (
              <div key={item.key}>
                <span>{item.label}</span>
                <strong>{formatValue(item.value, config.unit)}</strong>
                <small>{formatPp(item.change)}</small>
              </div>
            ))}
          </div>
          <p>
            O ponto central é acompanhar nível e direção ao mesmo tempo. Um indicador ainda alto, mas em queda, sugere alívio gradual;
            uma leitura baixa, mas em aceleração, pode antecipar deterioração das condições financeiras.
          </p>
        </article>
      </div>
    </section>
  );
}
