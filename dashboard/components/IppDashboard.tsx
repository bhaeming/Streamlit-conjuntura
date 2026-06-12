"use client";

import { useMemo, useState } from "react";
import type { EChartsOption, SeriesOption } from "echarts";
import type { Row } from "@/lib/data";
import { ChartCard } from "./ChartCard";

const source = "IBGE (2026)";
const palette = ["#1677a8", "#087f5b", "#2f9e44", "#339af0", "#0b7285", "#74b816", "#12b886", "#4dabf7"];

type Perspective = "gce" | "cnae";
type MetricRow = {
  category: string;
  date: string;
  value: number;
  deltaMonth: number | null;
  delta3m: number | null;
  delta12m: number | null;
};

function rowDate(row: Row) {
  return String(row.date ?? row.Date ?? "");
}

function numeric(value: Row[string] | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatPct(value: number | null) {
  if (value == null || Number.isNaN(value)) return "n/d";
  return `${value.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
}

function formatPp(value: number | null) {
  if (value == null || Number.isNaN(value)) return "n/d";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} p.p.`;
}

function categoriesFrom(rows: Row[], key: string) {
  return Array.from(new Set(rows.map((row) => String(row[key] ?? "")).filter(Boolean))).sort();
}

function defaultCnaeSelection(categories: string[]) {
  const general = categories.find((category) => category.toLowerCase().includes("geral"));
  const transformation = categories.find((category) => category.toLowerCase().includes("transforma"));
  const extractive = categories.find((category) => category.toLowerCase().includes("extrativa"));
  return Array.from(new Set([general, transformation, extractive, ...categories.slice(0, 2)].filter(Boolean))) as string[];
}

function metricsForCategory(sortedRows: Row[], categoryKey: string, category: string): MetricRow | null {
  const categoryRows = sortedRows.filter((row) => String(row[categoryKey] ?? "") === category);
  const latest = categoryRows.at(-1);
  const latestValue = latest ? numeric(latest.value) : null;
  if (!latest || latestValue == null) return null;

  const valueAt = (offset: number) => numeric(categoryRows.at(-1 - offset)?.value);
  const previous = valueAt(1);
  const three = valueAt(3);
  const twelve = valueAt(12);

  return {
    category,
    date: rowDate(latest),
    value: latestValue,
    deltaMonth: previous == null ? null : latestValue - previous,
    delta3m: three == null ? null : latestValue - three,
    delta12m: twelve == null ? null : latestValue - twelve,
  };
}

function buildCsv(rows: Row[], categoryKey: string, label: string) {
  const headers = ["perspectiva", "categoria", "codigo_ipp", "date", "ipp_12m"];
  const body = rows.map((row) => [
    label,
    String(row[categoryKey] ?? ""),
    String(row.codigo_ipp ?? ""),
    rowDate(row),
    String(row.value ?? ""),
  ]);
  return [headers, ...body]
    .map((line) => line.map((cell) => `"${String(cell).replace(/"/g, '""')}"`).join(";"))
    .join("\n");
}

function downloadCsv(filename: string, csv: string) {
  const blob = new Blob([`\uFEFF${csv}`], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

export function IppDashboard({ rows, cnaeRows = [] }: { rows: Row[]; cnaeRows?: Row[] }) {
  const gceCategories = useMemo(() => categoriesFrom(rows, "setor_ipp"), [rows]);
  const cnaeCategories = useMemo(() => categoriesFrom(cnaeRows, "cnae_ipp"), [cnaeRows]);

  const [perspective, setPerspective] = useState<Perspective>("gce");
  const [isDropdownOpen, setDropdownOpen] = useState(false);
  const [isModalOpen, setModalOpen] = useState(false);
  const [selectedByPerspective, setSelectedByPerspective] = useState<Record<Perspective, string[]>>(() => ({
    gce: categoriesFrom(rows, "setor_ipp"),
    cnae: defaultCnaeSelection(categoriesFrom(cnaeRows, "cnae_ipp")),
  }));

  const activeRows = perspective === "cnae" ? cnaeRows : rows;
  const categoryKey = perspective === "cnae" ? "cnae_ipp" : "setor_ipp";
  const categoryLabel = perspective === "cnae" ? "Setores econômicos (CNAE)" : "Grandes categorias";
  const perspectiveLabel = perspective === "cnae" ? "CNAE 2.0" : "Grandes categorias econômicas";
  const categories = perspective === "cnae" ? cnaeCategories : gceCategories;
  const selectedCategories = selectedByPerspective[perspective].filter((category) => categories.includes(category));

  const sortedRows = useMemo(
    () => activeRows.filter((row) => numeric(row.value) != null).sort((a, b) => rowDate(a).localeCompare(rowDate(b))),
    [activeRows],
  );

  const visibleRows = useMemo(
    () => sortedRows.filter((row) => selectedCategories.includes(String(row[categoryKey] ?? ""))),
    [sortedRows, selectedCategories, categoryKey],
  );

  const metricRows = useMemo(() => categories
    .map((category) => metricsForCategory(sortedRows, categoryKey, category))
    .filter((item): item is MetricRow => item != null), [categories, sortedRows, categoryKey]);

  const selectedMetrics = useMemo(() => selectedCategories
    .map((category) => metricsForCategory(sortedRows, categoryKey, category))
    .filter((item): item is MetricRow => item != null), [selectedCategories, sortedRows, categoryKey]);

  const general = metricRows.find((item) => item.category.toLowerCase().includes("geral")) ?? metricRows[0];
  const strongest = [...selectedMetrics].sort((a, b) => Math.abs(b.value) - Math.abs(a.value))[0];
  const highest = [...selectedMetrics].sort((a, b) => b.value - a.value)[0];
  const fastest = [...selectedMetrics].filter((item) => item.deltaMonth != null).sort((a, b) => (b.deltaMonth ?? 0) - (a.deltaMonth ?? 0))[0];
  const weakest = [...selectedMetrics].filter((item) => item.deltaMonth != null).sort((a, b) => (a.deltaMonth ?? 0) - (b.deltaMonth ?? 0))[0];
  const rising = selectedMetrics.filter((item) => (item.deltaMonth ?? 0) > 0).length;
  const falling = selectedMetrics.filter((item) => (item.deltaMonth ?? 0) < 0).length;
  const selectedAverage = selectedMetrics.length > 0
    ? selectedMetrics.reduce((sum, item) => sum + item.value, 0) / selectedMetrics.length
    : null;

  const visibleDates = Array.from(new Set(visibleRows.map(rowDate))).sort().slice(-72);

  const option: EChartsOption = {
    color: palette,
    tooltip: {
      trigger: "axis",
      valueFormatter: (value) => `${Number(value).toLocaleString("pt-BR", { maximumFractionDigits: 2 })}%`,
    },
    legend: { bottom: 0, type: "scroll", icon: "roundRect", textStyle: { color: "#52606d" } },
    grid: { left: 20, right: 22, top: 24, bottom: 64, containLabel: true },
    xAxis: {
      type: "category",
      data: visibleDates,
      axisLabel: { color: "#718096", hideOverlap: true, formatter: (value: string) => value.slice(0, 7) },
      axisLine: { lineStyle: { color: "#dbe4ea" } },
    },
    yAxis: {
      type: "value",
      name: "% em 12 meses",
      axisLabel: { color: "#718096" },
      splitLine: { lineStyle: { color: "#edf2f5" } },
    },
    dataZoom: [{ type: "inside" }],
    series: selectedCategories.map((category, index) => {
      const categoryRows = sortedRows.filter((row) => String(row[categoryKey] ?? "") === category);
      const byDate = new Map(categoryRows.map((row) => [rowDate(row), row.value]));
      return {
        name: category,
        type: "line",
        smooth: true,
        showSymbol: false,
        data: visibleDates.map((date) => byDate.get(date) ?? null),
        lineStyle: { width: index === 0 ? 3 : 2 },
        emphasis: { focus: "series" },
      };
    }) as SeriesOption[],
  };

  function setSelected(next: string[]) {
    setSelectedByPerspective((current) => ({ ...current, [perspective]: next }));
  }

  function toggleCategory(category: string) {
    setSelected(
      selectedCategories.includes(category)
        ? selectedCategories.filter((item) => item !== category)
        : [...selectedCategories, category],
    );
  }

  function downloadPerspective(target: Perspective, selectedOnly: boolean) {
    const targetRows = target === "cnae" ? cnaeRows : rows;
    const targetKey = target === "cnae" ? "cnae_ipp" : "setor_ipp";
    const targetLabel = target === "cnae" ? "CNAE 2.0" : "Grandes categorias econômicas";
    const selected = selectedByPerspective[target];
    const rowsToDownload = selectedOnly
      ? targetRows.filter((row) => selected.includes(String(row[targetKey] ?? "")))
      : targetRows;

    downloadCsv(`ipp_${target}_${selectedOnly ? "selecionados" : "todos"}.csv`, buildCsv(rowsToDownload, targetKey, targetLabel));
  }

  const insightText = selectedMetrics.length === 0
    ? "Selecione ao menos uma categoria para gerar uma leitura econômica."
    : `${perspective === "cnae" ? "Na abertura por CNAE" : "Na abertura por grandes categorias"}, a média das séries selecionadas está em ${formatPct(selectedAverage)}. Esse resultado mostra a intensidade da pressão de custos industriais acumulada em 12 meses no conjunto observado. ${rising} categoria(s) aceleraram frente ao mês anterior e ${falling} desaceleraram, sinalizando se a pressão de preços está se espalhando ou perdendo tração. O maior destaque da seleção é ${strongest?.category ?? "n/d"}, com IPP de ${formatPct(strongest?.value ?? null)} em ${strongest?.date.slice(0, 7) ?? "n/d"}. Em uma leitura de conjuntura, o IPP ajuda a antecipar pressões de custos que podem chegar aos preços ao consumidor, especialmente quando a alta ocorre em insumos amplamente usados pela indústria.`;

  return (
    <section className="ipp-section">
      <div className="section-heading">
        <span className="eyebrow">PREÇOS AO PRODUTOR</span>
        <h2>IPP e custos industriais</h2>
      </div>

      <div className="ipp-toolbar">
        <div>
          <span>Chave 1 - perspectiva</span>
          <div className="mode-switch" role="group" aria-label="Perspectiva do IPP">
            <button className={perspective === "gce" ? "active" : ""} onClick={() => { setPerspective("gce"); setDropdownOpen(false); }}>Grandes categorias</button>
            <button className={perspective === "cnae" ? "active" : ""} onClick={() => { setPerspective("cnae"); setDropdownOpen(false); }}>Setores CNAE</button>
          </div>
        </div>

        <div className="ipp-multiselect">
          <span>Chave 2 - {categoryLabel}</span>
          <button className="ipp-multiselect-trigger" onClick={() => setDropdownOpen((current) => !current)}>
            {selectedCategories.length} de {categories.length} selecionados
          </button>
          {isDropdownOpen && (
            <div className="ipp-multiselect-menu">
              <div className="ipp-actions compact">
                <button onClick={() => setSelected(categories)}>Selecionar tudo</button>
                <button onClick={() => setSelected([])}>Limpar</button>
              </div>
              <div className="ipp-check-list dropdown">
                {categories.map((category) => (
                  <label key={category} className="ipp-check-item">
                    <input
                      type="checkbox"
                      checked={selectedCategories.includes(category)}
                      onChange={() => toggleCategory(category)}
                    />
                    <span>{category}</span>
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>

        <button className="ipp-data-button" onClick={() => setModalOpen(true)}>Ver dados e baixar</button>
      </div>

      <div className="kpi-grid three">
        <article className="kpi-card">
          <span className="kpi-label">Categorias selecionadas</span>
          <strong>{selectedCategories.length}</strong>
          <div className="kpi-meta"><span>{perspectiveLabel}</span><span>12m</span></div>
          <small className="source-label">Fonte: {source}</small>
        </article>
        <article className="kpi-card">
          <span className="kpi-label">IPP - indústria geral</span>
          <strong>{formatPct(general?.value ?? null)}</strong>
          <div className="kpi-meta"><span>{general?.date.slice(0, 7) ?? "n/d"}</span><span>{formatPp(general?.deltaMonth ?? null)}</span></div>
          <small className="source-label">Fonte: {source}</small>
        </article>
        <article className="kpi-card">
          <span className="kpi-label">Maior variação selecionada</span>
          <strong>{formatPct(strongest?.value ?? null)}</strong>
          <div className="kpi-meta"><span>{strongest?.category ?? "n/d"}</span><span>{strongest?.date.slice(0, 7) ?? "n/d"}</span></div>
          <small className="source-label">Fonte: {source}</small>
        </article>
      </div>

      <section className="ipp-analysis-grid">
        <ChartCard
          title="IPP e custos industriais"
          subtitle={selectedCategories.length > 0 ? `Variação acumulada em 12 meses - ${perspectiveLabel}` : "Selecione ao menos uma categoria para exibir o gráfico"}
          option={option}
          source={source}
          tall
        />
        <article className="insight-card ipp-insight-card">
          <h2>Insights Econômicos</h2>
          <p>{insightText}</p>
          <div className="highlight-boxes">
            <div>
              <span>Maior IPP 12m</span>
              <strong>{highest?.category ?? "n/d"}</strong>
              <small>{formatPct(highest?.value ?? null)}</small>
            </div>
            <div>
              <span>Maior aceleração</span>
              <strong>{fastest?.category ?? "n/d"}</strong>
              <small>{formatPp(fastest?.deltaMonth ?? null)}</small>
            </div>
            <div>
              <span>Maior desaceleração</span>
              <strong>{weakest?.category ?? "n/d"}</strong>
              <small>{formatPp(weakest?.deltaMonth ?? null)}</small>
            </div>
          </div>
          <small className="source-label">Fonte: {source}</small>
        </article>
      </section>

      {isModalOpen && (
        <div className="modal-backdrop" role="presentation" onClick={() => setModalOpen(false)}>
          <div className="data-modal" role="dialog" aria-modal="true" aria-label="Dados do IPP" onClick={(event) => event.stopPropagation()}>
            <div className="table-card-header">
              <div>
                <h2>Dados do IPP</h2>
                <p>Leitura atual e variação em p.p. contra mês anterior, 3 meses e 12 meses da série IPP 12m.</p>
              </div>
              <button onClick={() => setModalOpen(false)}>Fechar</button>
            </div>

            <div className="ipp-download-bar">
              <button onClick={() => downloadPerspective(perspective, true)}>Baixar selecionados</button>
              <button onClick={() => downloadPerspective("gce", false)}>Baixar grandes categorias</button>
              <button onClick={() => downloadPerspective("cnae", false)}>Baixar CNAE</button>
            </div>

            <div className="table-scroll modal-table">
              <table>
                <thead><tr><th>Categoria</th><th>Referência</th><th>IPP 12m</th><th>Mês</th><th>3 meses</th><th>12 meses</th></tr></thead>
                <tbody>
                  {(selectedMetrics.length > 0 ? selectedMetrics : metricRows).map((item) => (
                    <tr key={item.category}>
                      <td>{item.category}</td>
                      <td>{item.date.slice(0, 7)}</td>
                      <td>{formatPct(item.value)}</td>
                      <td>{formatPp(item.deltaMonth)}</td>
                      <td>{formatPp(item.delta3m)}</td>
                      <td>{formatPp(item.delta12m)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <small className="source-label">Fonte: {source}</small>
          </div>
        </div>
      )}
    </section>
  );
}
