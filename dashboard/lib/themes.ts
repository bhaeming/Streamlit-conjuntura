import { Activity, BadgeDollarSign, Landmark, Users } from "lucide-react";

export const themes = [
  {
    href: "/atividade",
    title: "Atividade econômica",
    eyebrow: "Produção e crescimento",
    description: "PIB, IBC-Br, indústria, comércio e serviços.",
    icon: Activity,
    color: "#087f5b",
    tint: "#e9f7f1",
    source: "BCB/IBGE (2026)",
  },
  {
    href: "/precos",
    title: "Preços",
    eyebrow: "Inflação",
    description: "IPCA agregado, grupos e preços ao produtor.",
    icon: BadgeDollarSign,
    color: "#1677a8",
    tint: "#e9f5fa",
    source: "IBGE/BCB (2026)",
  },
  {
    href: "/juros-credito",
    title: "Juros e crédito",
    eyebrow: "Condições financeiras",
    description: "Selic, crédito, taxas de juros e inadimplência.",
    icon: Landmark,
    color: "#176b87",
    tint: "#e9f3f6",
    source: "BCB (2026)",
  },
  {
    href: "/trabalho",
    title: "Mercado de trabalho",
    eyebrow: "Emprego e renda",
    description: "Desemprego, ocupação, renda e informalidade.",
    icon: Users,
    color: "#2b8a3e",
    tint: "#edf8ee",
    source: "IBGE (2026)",
  },
];
