import Link from "next/link";
import { ChevronRight, House } from "lucide-react";

export function DashboardHeader({
  eyebrow,
  title,
  description,
  reference,
}: {
  eyebrow: string;
  title: string;
  description: string;
  reference: string;
}) {
  return (
    <header className="dashboard-header">
      <div className="breadcrumb"><Link href="/"><House size={14} /> Início</Link><ChevronRight size={14} /><span>{title}</span></div>
      <div className="header-row">
        <div><span className="eyebrow">{eyebrow}</span><h1>{title}</h1><p>{description}</p></div>
        <div className="reference"><span className="status-dot" /><span><small>Última referência</small><strong>{reference}</strong></span></div>
      </div>
    </header>
  );
}
