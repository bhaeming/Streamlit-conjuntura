"use client";

import dynamic from "next/dynamic";
import { useMemo, useState } from "react";
import type { EChartsOption } from "echarts";
import * as echarts from "echarts";
import type { Row } from "@/lib/data";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

const variables = {
  taxa_desemprego: { label: "Desemprego", unit: "%", description: "taxa de desemprego" },
  taxa_ocupacao: { label: "Ocupação", unit: "%", description: "taxa de ocupação" },
  informalidade: { label: "Informalidade", unit: "%", description: "taxa de informalidade" },
  renda_media: { label: "Renda real média", unit: "R$", description: "rendimento real médio" },
} as const;

type VariableKey = keyof typeof variables;

function formatValue(value: number | null, unit: string) {
  if (value == null || Number.isNaN(value)) return "n/d";
  if (unit === "R$") return `R$ ${value.toLocaleString("pt-BR", { maximumFractionDigits: 0 })}`;
  return `${value.toLocaleString("pt-BR", { minimumFractionDigits: 1, maximumFractionDigits: 1 })}%`;
}

function numeric(value: Row[string] | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

export function BrazilUnemploymentMap({ rows, geoJson }: { rows: Row[]; geoJson: unknown }) {
  const [variable, setVariable] = useState<VariableKey>("taxa_desemprego");
  const variableConfig = variables[variable];

  const analysis = useMemo(() => {
    const sorted = [...rows].sort((a, b) => String(a.date).localeCompare(String(b.date)));
    const latestDate = String(sorted.at(-1)?.date ?? "");
    const previousDate = Array.from(new Set(sorted.map((row) => String(row.date)))).sort().at(-2) ?? "";
    const latestRows = sorted.filter((row) => row.date === latestDate);
    const previousRows = sorted.filter((row) => row.date === previousDate);
    const previousByUf = new Map(previousRows.map((row) => [String(row.uf), numeric(row[variable])]));
    const values = latestRows
      .map((row) => ({ name: String(row.uf), value: numeric(row[variable]), previous: previousByUf.get(String(row.uf)) ?? null }))
      .filter((row): row is { name: string; value: number; previous: number | null } => row.value != null)
      .sort((a, b) => b.value - a.value);
    const average = values.length ? values.reduce((acc, row) => acc + row.value, 0) / values.length : null;
    const highest = values.at(0) ?? null;
    const lowest = values.at(-1) ?? null;
    const rising = values.filter((row) => row.previous != null && row.value > row.previous).length;
    const falling = values.filter((row) => row.previous != null && row.value < row.previous).length;
    return { latestDate, values, average, highest, lowest, rising, falling };
  }, [rows, variable]);

  echarts.registerMap("BR_UF", geoJson as Parameters<typeof echarts.registerMap>[1]);

  const min = Math.min(...analysis.values.map((row) => row.value));
  const max = Math.max(...analysis.values.map((row) => row.value));

  const option: EChartsOption = {
    tooltip: {
      trigger: "item",
      formatter: (params) => {
        const item = Array.isArray(params) ? params[0] : params;
        const value = typeof item.value === "number" ? formatValue(item.value, variableConfig.unit) : "n/d";
        return `<strong>${item.name}</strong><br/>${variableConfig.label}: ${value}`;
      },
    },
    visualMap: {
      min,
      max,
      left: 8,
      bottom: 8,
      calculable: true,
      orient: "horizontal",
      inRange: { color: ["#d8f3dc", "#74c69d", "#168aad", "#184e77"] },
      textStyle: { color: "#52606d" },
    },
    series: [{
      name: variableConfig.label,
      type: "map",
      map: "BR_UF",
      nameProperty: "NM_UF",
      roam: true,
      data: analysis.values,
      emphasis: {
        label: { color: "#102a35", fontWeight: "bold" },
        itemStyle: { areaColor: "#ffd166" },
      },
      itemStyle: { borderColor: "#ffffff", borderWidth: 1 },
    }],
  };

  return (
    <section className="map-insights-grid">
      <article className="chart-card">
        <div className="chart-title">
          <div>
            <h2>{variableConfig.label} por UF</h2>
            <p>Mapa de calor da {variableConfig.description} no último trimestre disponível.</p>
          </div>
          <span className="source-info" title="Fonte: IBGE (2026)" aria-label="Fonte: IBGE (2026)">i</span>
        </div>
        <div className="map-variable-control">
          <label>
            <span>Indicador do mapa</span>
            <select value={variable} onChange={(event) => setVariable(event.target.value as VariableKey)}>
              {Object.entries(variables).map(([key, item]) => <option key={key} value={key}>{item.label}</option>)}
            </select>
          </label>
        </div>
        <ReactECharts option={option} style={{ height: 520 }} notMerge lazyUpdate />
      </article>
      <article className="insight-card">
        <h2>Insights Econômicos</h2>
        <p>
          Na referência mais recente ({analysis.latestDate.slice(0, 7) || "n/d"}), a média entre UFs para {variableConfig.description}
          ficou em <strong>{formatValue(analysis.average, variableConfig.unit)}</strong>. A maior leitura foi observada em
          <strong> {analysis.highest?.name ?? "n/d"}</strong> ({formatValue(analysis.highest?.value ?? null, variableConfig.unit)}),
          enquanto <strong>{analysis.lowest?.name ?? "n/d"}</strong> registrou a menor leitura ({formatValue(analysis.lowest?.value ?? null, variableConfig.unit)}).
        </p>
        <p>
          A leitura regional é útil porque o mercado de trabalho brasileiro não se ajusta de forma uniforme. Estados com desemprego
          ou informalidade acima da média tendem a sinalizar maior ociosidade, menor proteção trabalhista e renda mais vulnerável.
          Já ocupação e rendimento real mais elevados costumam indicar maior dinamismo local, embora devam ser avaliados junto com
          inflação, composição setorial e qualidade dos postos criados.
        </p>
        <div className="insight-metrics">
          <div><strong>{analysis.rising}</strong><span>UFs com alta frente ao trimestre anterior</span></div>
          <div><strong>{analysis.falling}</strong><span>UFs com queda frente ao trimestre anterior</span></div>
        </div>
        <p>
          Em termos conjunturais, altas de renda e ocupação são sinais positivos quando ocorrem sem avanço simultâneo da informalidade.
          Quando o desemprego cai, mas a informalidade sobe, a melhora pode ser mais frágil, pois depende de vínculos de menor
          estabilidade e menor capacidade de sustentação do consumo.
        </p>
      </article>
    </section>
  );
}
