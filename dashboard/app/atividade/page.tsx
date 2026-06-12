import { AppShell } from "@/components/AppShell";
import { DataDownloadButton } from "@/components/DataDownloadButton";
import { DashboardHeader } from "@/components/DashboardHeader";
import { KpiCard } from "@/components/KpiCard";
import { SeriesChart } from "@/components/SeriesChart";
import { UfRadarChart } from "@/components/UfRadarChart";
import { loadData } from "@/lib/data";
import { lastValue, month, number } from "@/lib/format";

export default function Atividade() {
  const sgs = loadData("sgs_dados");
  const sectors = loadData("indust_comer_serv");
  const ibcUf = loadData("ibc_uf");
  const last = lastValue(sgs, "ibc_br_dessaz");
  const sectorLabels = ["Industria 12m", "Comercio 12m", "Servicos 12m"];

  return (
    <AppShell active="/atividade">
      <DashboardHeader eyebrow="Producao e crescimento" title="Atividade economica" description="Acompanhe o ritmo da economia brasileira e seus principais setores." reference={last ? month(last.date) : "n/d"} />
      <div className="content">
        <DataDownloadButton
          title="Atividade economica"
          datasets={[
            { name: "sgs_dados", source: "BCB (2026)" },
            { name: "indust_comer_serv", source: "IBGE (2026)" },
            { name: "ibc_uf", source: "BCB (2026)" },
          ]}
        />
        <div className="kpi-grid three">
          {["pim_12m", "pmc_12m", "pms_12m"].map((key, index) => {
            const item = lastValue(sectors, key);
            return <KpiCard key={key} label={sectorLabels[index]} value={`${number(item?.value ?? 0, 1)}%`} reference={item ? month(item.date) : ""} source="IBGE (2026)" />;
          })}
        </div>
        <SeriesChart rows={sgs} series={{ ibc_br: "IBC-Br", ibc_br_dessaz: "IBC-Br dessazonalizado" }} title="Indice de atividade economica" subtitle="Indice mensal do Banco Central" source="BCB (2026)" />
        <UfRadarChart rows={ibcUf} />
        <SeriesChart rows={sectors} series={{ pim_12m: "Industria", pmc_12m: "Comercio", pms_12m: "Servicos" }} title="Atividade por setor" subtitle="Variacao acumulada em 12 meses" suffix="%" source="IBGE (2026)" />
      </div>
    </AppShell>
  );
}
