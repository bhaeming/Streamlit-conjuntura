import { AppShell } from "@/components/AppShell";
import { DataDownloadButton } from "@/components/DataDownloadButton";
import { DashboardHeader } from "@/components/DashboardHeader";
import { InflationDashboard } from "@/components/InflationDashboard";
import { IppDashboard } from "@/components/IppDashboard";
import { KpiCard } from "@/components/KpiCard";
import { loadData } from "@/lib/data";
import { lastValue, month, number } from "@/lib/format";

const source = "IBGE/BCB (2026)";

export default function Precos() {
  const rows = loadData("ipca_all").map((row) => ({ ...row, date: row.Date }));
  const groupRows = loadData("ipca_grupos");
  const ippRows = loadData("ipp_m");
  const ippCnaeRows = loadData("ipp_cnae");
  const latest = lastValue(rows, "ipca_12m");
  const metrics = [
    { key: "ipca", label: "IPCA mensal" },
    { key: "ipca_12m", label: "IPCA 12 meses" },
    { key: "ipca_livres_12m_calc", label: "Precos livres 12m" },
    { key: "ipca_administrados_12m_calc", label: "Administrados 12m" },
  ];

  return (
    <AppShell active="/precos">
      <DashboardHeader
        eyebrow="Inflacao"
        title="Precos"
        description="Evolucao da inflacao ao consumidor e seus componentes."
        reference={latest ? month(latest.date) : "n/d"}
      />
      <div className="content">
        <DataDownloadButton
          title="Precos"
          datasets={[
            { name: "ipca_all", source },
            { name: "ipca_grupos", source: "IBGE (2026)" },
            { name: "ipp_m", source: "IBGE (2026)" },
            { name: "ipp_cnae", source: "IBGE (2026)" },
          ]}
        />
        <div className="kpi-grid">
          {metrics.map(({ key, label }) => {
            const item = lastValue(rows, key);
            return (
              <KpiCard
                key={key}
                label={label}
                value={`${number(item?.value ?? 0, 2)}%`}
                reference={item ? month(item.date) : ""}
                source={source}
              />
            );
          })}
        </div>
        <InflationDashboard ipcaRows={rows} groupRows={groupRows} />
        <IppDashboard rows={ippRows} cnaeRows={ippCnaeRows} />
      </div>
    </AppShell>
  );
}
