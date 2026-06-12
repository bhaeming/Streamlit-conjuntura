import { ArrowDownRight, ArrowUpRight, Minus } from "lucide-react";

export function KpiCard({
  label,
  value,
  reference,
  change,
  source,
}: {
  label: string;
  value: string;
  reference?: string;
  change?: number | null;
  source?: string;
}) {
  const positive = change != null && change > 0;
  const negative = change != null && change < 0;
  const DeltaIcon = positive ? ArrowUpRight : negative ? ArrowDownRight : Minus;
  return (
    <article className="kpi-card">
      <span className="kpi-label">{label}</span>
      <strong>{value}</strong>
      <div className="kpi-meta">
        {change != null && <span className={positive ? "up" : negative ? "down" : ""}><DeltaIcon size={14} /> {Math.abs(change).toFixed(2)}</span>}
        <span>{reference}</span>
      </div>
      {source && <small className="source-label">Fonte: {source}</small>}
    </article>
  );
}
