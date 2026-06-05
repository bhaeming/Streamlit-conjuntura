import { AppShell } from "@/components/AppShell";
import { DashboardHeader } from "@/components/DashboardHeader";
import { KpiCard } from "@/components/KpiCard";
import { SeriesChart } from "@/components/SeriesChart";
import { loadData } from "@/lib/data";
import { lastValue, month, number } from "@/lib/format";

export default function Trabalho() {
  const rows = loadData("socioeconomico_quarterly");
  const latest = lastValue(rows, "taxa_desemprego");
  const metrics = [{ key: "taxa_desemprego", label: "Desemprego", suffix: "%" }, { key: "taxa_ocupacao", label: "Ocupação", suffix: "%" }, { key: "renda_media", label: "Renda média", suffix: "" }, { key: "informalidade", label: "Informalidade", suffix: "%" }];
  return <AppShell active="/trabalho"><DashboardHeader eyebrow="Emprego e renda" title="Mercado de trabalho" description="Indicadores trimestrais de emprego, participação e renda." reference={latest ? month(latest.date) : "n/d"} />
    <div className="content"><div className="kpi-grid">{metrics.map(({ key, label, suffix }) => { const item = lastValue(rows, key); return <KpiCard key={key} label={label} value={key === "renda_media" ? `R$ ${number(item?.value ?? 0, 0)}` : `${number(item?.value ?? 0, 1)}${suffix}`} reference={item ? month(item.date) : ""} />; })}</div>
    <SeriesChart rows={rows} series={{ taxa_desemprego: "Desemprego", taxa_ocupacao: "Ocupação", informalidade: "Informalidade", desalentadas: "Desalentadas" }} title="Indicadores do mercado de trabalho" subtitle="Taxas trimestrais, em percentual" suffix="%" />
    <SeriesChart rows={rows} series={{ renda_media: "Renda média" }} title="Rendimento médio real" subtitle="Valor em reais" />
    </div></AppShell>;
}
