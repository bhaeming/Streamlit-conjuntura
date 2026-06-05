import { AppShell } from "@/components/AppShell";
import { ThemeCard } from "@/components/ThemeCard";
import { KpiCard } from "@/components/KpiCard";
import { themes } from "@/lib/themes";
import { loadData } from "@/lib/data";
import { lastValue, month, number } from "@/lib/format";

export default function Home() {
  const ipca = lastValue(loadData("ipca_all"), "ipca_12m");
  const selic = lastValue(loadData("selic_mensal"), "selic");
  const socio = loadData("socioeconomico_quarterly");
  const desemprego = lastValue(socio, "taxa_desemprego");
  const sgs = loadData("sgs_dados");
  const ibc = lastValue(sgs, "ibc_br_dessaz");

  return (
    <AppShell>
      <div className="home-hero">
        <div><span className="eyebrow">PAINEL DE CONJUNTURA 2.0</span><h1>Economia brasileira,<br /><em>em perspectiva.</em></h1><p>Indicadores essenciais organizados para uma leitura clara, rápida e comparável do cenário econômico.</p></div>
        <div className="hero-orbit"><span>Dados oficiais</span><strong>4</strong><small>dimensões<br />integradas</small></div>
      </div>
      <section className="snapshot"><div><span className="eyebrow">RESUMO EXECUTIVO</span><h2>Últimos indicadores</h2></div><div className="kpi-grid">
        <KpiCard label="IPCA 12 meses" value={`${number(ipca?.value ?? 0, 2)}%`} reference={ipca ? month(ipca.date) : ""} />
        <KpiCard label="Selic" value={`${number(selic?.value ?? 0, 2)}%`} reference={selic ? month(selic.date) : ""} />
        <KpiCard label="Desemprego" value={`${number(desemprego?.value ?? 0, 1)}%`} reference={desemprego ? month(desemprego.date) : ""} />
        <KpiCard label="IBC-Br dessaz." value={number(ibc?.value ?? 0, 2)} reference={ibc ? month(ibc.date) : ""} />
      </div></section>
      <section className="themes-section"><div className="section-heading"><span className="eyebrow">NAVEGUE POR TEMA</span><h2>Escolha uma dimensão para analisar</h2></div><div className="theme-grid">{themes.map((theme) => <ThemeCard key={theme.href} theme={theme} />)}</div></section>
    </AppShell>
  );
}
