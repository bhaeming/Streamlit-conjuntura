import Link from "next/link";
import { ArrowUpRight } from "lucide-react";
import type { themes } from "@/lib/themes";

type Theme = (typeof themes)[number];

export function ThemeCard({ theme }: { theme: Theme }) {
  const Icon = theme.icon;
  return (
    <Link href={theme.href} className="theme-card" style={{ "--accent": theme.color, "--tint": theme.tint } as React.CSSProperties}>
      <div className="theme-card-top">
        <span className="theme-icon"><Icon size={23} /></span>
        <ArrowUpRight className="theme-arrow" size={20} />
      </div>
      <span className="eyebrow">{theme.eyebrow}</span>
      <h2>{theme.title}</h2>
      <p>{theme.description}</p>
      <small className="source-label">Fonte: {theme.source}</small>
      <span className="open-label">Explorar indicadores</span>
    </Link>
  );
}
