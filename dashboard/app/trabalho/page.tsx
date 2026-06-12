import { AppShell } from "@/components/AppShell";
import { BrazilUnemploymentMap } from "@/components/BrazilUnemploymentMap";
import { DataDownloadButton } from "@/components/DataDownloadButton";
import { DashboardHeader } from "@/components/DashboardHeader";
import { KpiCard } from "@/components/KpiCard";
import { SeriesChart } from "@/components/SeriesChart";
import { loadData } from "@/lib/data";
import { lastValue, month, number } from "@/lib/format";
import fs from "node:fs";
import path from "node:path";

const source = "IBGE (2026)";

export default function Trabalho() {
  const rows = loadData("socioeconomico_quarterly");
  const desempUf = loadData("desemp_uf");
  const socioUf = loadData("socioeconomico_uf");
  const geoJson = JSON.parse(fs.readFileSync(path.join(process.cwd(), "..", "assets", "geo", "BR_UF_2023.geojson"), "utf8"));
  const latest = lastValue(rows, "taxa_desemprego");
  const metrics = [
    { key: "taxa_desemprego", label: "Desemprego", suffix: "%" },
    { key: "taxa_ocupacao", label: "Ocupacao", suffix: "%" },
    { key: "renda_media", label: "Renda media", suffix: "" },
    { key: "informalidade", label: "Informalidade", suffix: "%" },
  ];

  return (
    <AppShell active="/trabalho">
      <DashboardHeader eyebrow="Emprego e renda" title="Mercado de trabalho" description="Indicadores trimestrais de emprego, participacao e renda." reference={latest ? month(latest.date) : "n/d"} />
      <div className="content">
        <DataDownloadButton
          title="Mercado de trabalho"
          datasets={[
            { name: "socioeconomico_quarterly", source },
            { name: "desemp_uf", source },
            { name: "socioeconomico_uf", source },
          ]}
        />
        <div className="kpi-grid">
          {metrics.map(({ key, label, suffix }) => {
            const item = lastValue(rows, key);
            return <KpiCard key={key} label={label} value={key === "renda_media" ? `R$ ${number(item?.value ?? 0, 0)}` : `${number(item?.value ?? 0, 1)}${suffix}`} reference={item ? month(item.date) : ""} source={source} />;
          })}
        </div>
        <BrazilUnemploymentMap rows={socioUf.length ? socioUf : desempUf} geoJson={geoJson} />
        <SeriesChart rows={rows} series={{ taxa_desemprego: "Desemprego", taxa_ocupacao: "Ocupacao", informalidade: "Informalidade", desalentadas: "Desalentadas" }} title="Indicadores do mercado de trabalho" subtitle="Taxas trimestrais, em percentual" suffix="%" source={source} />
        <SeriesChart rows={rows} series={{ renda_media: "Renda media" }} title="Rendimento medio real" subtitle="Valor em reais" source={source} />
      </div>
    </AppShell>
  );
}
