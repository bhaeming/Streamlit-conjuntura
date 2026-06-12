"use client";

import { useMemo, useState } from "react";
import type { Row } from "@/lib/data";

type Dataset = {
  name: string;
  rows?: Row[];
  source?: string;
};

type LongRow = {
  dataset: string;
  date: string;
  variable: string;
  value: string | number | null;
  dimensions: string;
  source: string;
};

function rowDate(row: Row) {
  return String(row.date ?? row.Date ?? "");
}

function isNumber(value: Row[string] | undefined) {
  return typeof value === "number" && Number.isFinite(value);
}

function toLongRows(datasets: Dataset[]) {
  return datasets.flatMap((dataset) => (dataset.rows ?? []).flatMap((row) => {
    const keys = Object.keys(row).filter((key) => key !== "date" && key !== "Date");
    const dimensionKeys = keys.filter((key) => !isNumber(row[key]) && key !== "value");
    const dimensions = dimensionKeys.map((key) => `${key}=${String(row[key] ?? "")}`).join(" | ");

    if (isNumber(row.value)) {
      const variable = String(row.indicador ?? row.serie ?? row.variable ?? "value");
      return [{
        dataset: dataset.name,
        date: rowDate(row),
        variable,
        value: row.value,
        dimensions,
        source: dataset.source ?? "",
      }];
    }

    return keys
      .filter((key) => isNumber(row[key]))
      .map((key) => ({
        dataset: dataset.name,
        date: rowDate(row),
        variable: key,
        value: row[key],
        dimensions,
        source: dataset.source ?? "",
      }));
  }));
}

function buildCsv(rows: LongRow[]) {
  const headers = ["dataset", "date", "variable", "value", "dimensions", "source"];
  return [headers, ...rows.map((row) => headers.map((key) => String(row[key as keyof LongRow] ?? "")))]
    .map((line) => line.map((cell) => `"${cell.replace(/"/g, '""')}"`).join(";"))
    .join("\n");
}

function downloadCsv(filename: string, rows: LongRow[]) {
  const blob = new Blob([`\uFEFF${buildCsv(rows)}`], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

export function DataDownloadButton({ title, datasets }: { title: string; datasets: Dataset[] }) {
  const [open, setOpen] = useState(false);
  const [loadedDatasets, setLoadedDatasets] = useState<Dataset[] | null>(null);
  const [loading, setLoading] = useState(false);
  const activeDatasets = loadedDatasets ?? datasets.filter((dataset) => dataset.rows);
  const rows = useMemo(() => toLongRows(activeDatasets), [activeDatasets]);
  const previewRows = rows.slice(0, 700);

  async function openModal() {
    setOpen(true);
    if (loadedDatasets) return;
    setLoading(true);
    const loaded = await Promise.all(datasets.map(async (dataset) => {
      if (dataset.rows) return dataset;
      const response = await fetch(`/data/${dataset.name}.json`);
      const rows = await response.json() as Row[];
      return { ...dataset, rows };
    }));
    setLoadedDatasets(loaded);
    setLoading(false);
  }

  return (
    <>
      <div className="page-data-actions">
        <button onClick={openModal}>Ver dados da página</button>
      </div>
      {open && (
        <div className="modal-backdrop" role="presentation" onClick={() => setOpen(false)}>
          <div className="data-modal" role="dialog" aria-modal="true" aria-label={`Dados - ${title}`} onClick={(event) => event.stopPropagation()}>
            <div className="table-card-header">
              <div>
                <h2>Dados - {title}</h2>
                <p>{loading ? "Carregando dados..." : `Visualização em formato long. Exibindo ${previewRows.length} de ${rows.length} linhas.`}</p>
              </div>
              <button onClick={() => setOpen(false)}>Fechar</button>
            </div>
            <div className="ipp-download-bar">
              <button disabled={loading || rows.length === 0} onClick={() => downloadCsv(`${title.toLowerCase().replace(/\s+/g, "_")}_long.csv`, rows)}>Baixar CSV</button>
            </div>
            <div className="table-scroll modal-table">
              <table>
                <thead><tr><th>Dataset</th><th>Data</th><th>Variável</th><th>Valor</th><th>Dimensões</th><th>Fonte</th></tr></thead>
                <tbody>
                  {previewRows.map((row, index) => (
                    <tr key={`${row.dataset}-${row.date}-${row.variable}-${index}`}>
                      <td>{row.dataset}</td>
                      <td>{row.date.slice(0, 10)}</td>
                      <td>{row.variable}</td>
                      <td>{row.value}</td>
                      <td>{row.dimensions}</td>
                      <td>{row.source}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
