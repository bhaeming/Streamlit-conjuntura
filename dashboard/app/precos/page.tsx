import { AppShell } from "@/components/AppShell";
import { DashboardHeader } from "@/components/DashboardHeader";
import { KpiCard } from "@/components/KpiCard";
import { SeriesChart } from "@/components/SeriesChart";
import { loadData } from "@/lib/data";
import { lastValue, month, number } from "@/lib/format";

export default function Precos() {
  const rows = loadData("ipca_all").map((row) => ({ ...row, date: row.Date }));
  const latest = lastValue(rows, "ipca_12m");
  const metrics = [{ key: "ipca", label: "IPCA mensal" }, { key: "ipca_12m", label: "IPCA 12 meses" }, { key: "ipca_livres_12m_calc", label: "Preços livres 12m" }, { key: "ipca_administrados_12m_calc", label: "Administrados 12m" }];
  return <AppShell active="/precos"><DashboardHeader eyebrow="Inflação" title="Preços" description="Evolução da inflação ao consumidor e seus componentes." reference={latest ? month(latest.date) : "n/d"} />
    <div className="content"><div className="kpi-grid">{metrics.map(({ key, label }) => { const item = lastValue(rows, key); return <KpiCard key={key} label={label} value={`${number(item?.value ?? 0, 2)}%`} reference={item ? month(item.date) : ""} />; })}</div>
    <SeriesChart rows={rows} series={{ ipca_12m: "IPCA 12m", ipca_livres_12m_calc: "Livres 12m", ipca_administrados_12m_calc: "Administrados 12m" }} title="Inflação ao consumidor" subtitle="Variação acumulada em 12 meses" suffix="%" />
    <SeriesChart rows={rows} series={{ ipca: "IPCA mensal", ipca_livres: "Livres mensal", ipca_administrados: "Administrados mensal" }} title="Variação mensal" subtitle="Leitura mês a mês dos componentes" type="bar" suffix="%" />
    </div></AppShell>;
}
