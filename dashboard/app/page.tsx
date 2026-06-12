import { AppShell } from "@/components/AppShell";
import { DataDownloadButton } from "@/components/DataDownloadButton";
import { KpiCard } from "@/components/KpiCard";
import { ThemeCard } from "@/components/ThemeCard";
import { loadData } from "@/lib/data";
import { lastValue, month, number } from "@/lib/format";
import { themes } from "@/lib/themes";

export default function Home() {
  const ipcaRows = loadData("ipca_all");
  const selicRows = loadData("selic_mensal");
  const ipca = lastValue(ipcaRows, "ipca_12m");
  const selic = lastValue(selicRows, "selic");
  const socio = loadData("socioeconomico_quarterly");
  const desemprego = lastValue(socio, "taxa_desemprego");
  const sgs = loadData("sgs_dados");
  const ibc = lastValue(sgs, "ibc_br_dessaz");

  return (
    <AppShell>
      <div className="home-hero">
        <div>
          <span className="eyebrow">PAINEL DE CONJUNTURA 2.0</span>
          <h1>Economia brasileira,<br /><em>em perspectiva.</em></h1>
          <p>Indicadores essenciais organizados para uma leitura clara, rapida e comparavel do cenario economico.</p>
        </div>
        <div className="hero-orbit"><span>Dados oficiais</span><strong>4</strong><small>dimensoes<br />integradas</small></div>
      </div>
      <section className="snapshot">
        <div><span className="eyebrow">RESUMO EXECUTIVO</span><h2>Ultimos indicadores</h2></div>
        <DataDownloadButton
          title="Resumo executivo"
          datasets={[
            { name: "ipca_all", source: "IBGE/BCB (2026)" },
            { name: "selic_mensal", source: "BCB (2026)" },
            { name: "socioeconomico_quarterly", source: "IBGE (2026)" },
            { name: "sgs_dados", source: "BCB (2026)" },
          ]}
        />
        <div className="kpi-grid">
          <KpiCard label="IPCA 12 meses" value={`${number(ipca?.value ?? 0, 2)}%`} reference={ipca ? month(ipca.date) : ""} source="IBGE/BCB (2026)" />
          <KpiCard label="Selic" value={`${number(selic?.value ?? 0, 2)}%`} reference={selic ? month(selic.date) : ""} source="BCB (2026)" />
          <KpiCard label="Desemprego" value={`${number(desemprego?.value ?? 0, 1)}%`} reference={desemprego ? month(desemprego.date) : ""} source="IBGE (2026)" />
          <KpiCard label="IBC-Br dessaz." value={number(ibc?.value ?? 0, 2)} reference={ibc ? month(ibc.date) : ""} source="BCB (2026)" />
        </div>
      </section>
      <section className="themes-section">
        <div className="section-heading"><span className="eyebrow">NAVEGUE POR TEMA</span><h2>Escolha uma dimensao para analisar</h2></div>
        <div className="theme-grid">{themes.map((theme) => <ThemeCard key={theme.href} theme={theme} />)}</div>
      </section>
    </AppShell>
  );
}
