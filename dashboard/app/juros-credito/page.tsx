import { AppShell } from "@/components/AppShell";
import { DashboardHeader } from "@/components/DashboardHeader";
import { KpiCard } from "@/components/KpiCard";
import { SeriesChart } from "@/components/SeriesChart";
import { loadData } from "@/lib/data";
import { compact, lastValue, month, number } from "@/lib/format";

export default function JurosCredito() {
  const sgs = loadData("sgs_dados");
  const selicRows = loadData("selic_mensal");
  const selic = lastValue(selicRows, "selic");
  const credito = lastValue(sgs, "credito_total");
  const juros = lastValue(sgs, "taxa_juros_total");
  const inad = lastValue(sgs, "inadimplencia_total");
  return <AppShell active="/juros-credito"><DashboardHeader eyebrow="Condições financeiras" title="Juros e crédito" description="Política monetária e condições do mercado de crédito." reference={selic ? month(selic.date) : "n/d"} />
    <div className="content"><div className="kpi-grid"><KpiCard label="Taxa Selic" value={`${number(selic?.value ?? 0, 2)}%`} reference={selic ? month(selic.date) : ""} /><KpiCard label="Crédito total" value={`R$ ${compact((credito?.value ?? 0) * 1_000_000)}`} reference={credito ? month(credito.date) : ""} /><KpiCard label="Juros total" value={`${number(juros?.value ?? 0, 2)}%`} reference={juros ? month(juros.date) : ""} /><KpiCard label="Inadimplência total" value={`${number(inad?.value ?? 0, 2)}%`} reference={inad ? month(inad.date) : ""} /></div>
    <SeriesChart rows={selicRows} series={{ selic: "Selic" }} title="Taxa básica de juros" subtitle="Meta Selic, em % ao ano" suffix="%" />
    <SeriesChart rows={sgs} series={{ credito_pf: "Pessoa física", credito_pj: "Pessoa jurídica", credito_total: "Total" }} title="Estoque de crédito" subtitle="Saldo em milhões de reais" />
    <div className="two-columns"><SeriesChart rows={sgs} series={{ taxa_juros_pf: "PF", taxa_juros_pj: "PJ", taxa_juros_total: "Total" }} title="Taxas de juros" subtitle="Percentual ao ano" suffix="%" /><SeriesChart rows={sgs} series={{ inadimplencia_pf: "PF", inadimplencia_pj: "PJ", inadimplencia_total: "Total" }} title="Inadimplência" subtitle="Percentual da carteira" suffix="%" /></div>
    </div></AppShell>;
}
