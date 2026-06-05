import { AppShell } from "@/components/AppShell";
import { DashboardHeader } from "@/components/DashboardHeader";
import { KpiCard } from "@/components/KpiCard";
import { SeriesChart } from "@/components/SeriesChart";
import { loadData } from "@/lib/data";
import { lastValue, month, number } from "@/lib/format";

export default function Atividade() {
  const sgs = loadData("sgs_dados");
  const sectors = loadData("indust_comer_serv");
  const last = lastValue(sgs, "ibc_br_dessaz");
  return <AppShell active="/atividade"><DashboardHeader eyebrow="Produção e crescimento" title="Atividade econômica" description="Acompanhe o ritmo da economia brasileira e seus principais setores." reference={last ? month(last.date) : "n/d"} />
    <div className="content"><div className="kpi-grid three">
      {["pim_12m", "pmc_12m", "pms_12m"].map((key, index) => { const item = lastValue(sectors, key); return <KpiCard key={key} label={["Indústria 12m", "Comércio 12m", "Serviços 12m"][index]} value={`${number(item?.value ?? 0, 1)}%`} reference={item ? month(item.date) : ""} />; })}
    </div>
    <SeriesChart rows={sgs} series={{ ibc_br: "IBC-Br", ibc_br_dessaz: "IBC-Br dessazonalizado" }} title="Índice de atividade econômica" subtitle="Índice mensal do Banco Central" />
    <SeriesChart rows={sectors} series={{ pim_12m: "Indústria", pmc_12m: "Comércio", pms_12m: "Serviços" }} title="Atividade por setor" subtitle="Variação acumulada em 12 meses" suffix="%" />
    </div></AppShell>;
}
