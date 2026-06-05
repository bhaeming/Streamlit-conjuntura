import Link from "next/link";
import { BarChart3 } from "lucide-react";
import { themes } from "@/lib/themes";

export function AppShell({ children, active }: { children: React.ReactNode; active?: string }) {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <Link href="/" className="brand">
          <span className="brand-mark"><BarChart3 size={21} /></span>
          <span><strong>Conjuntura</strong><small>Brasil em dados</small></span>
        </Link>
        <nav aria-label="Temas">
          <span className="nav-label">PAINÉIS</span>
          {themes.map(({ href, title, icon: Icon }) => (
            <Link key={href} href={href} className={`nav-item ${active === href ? "active" : ""}`}>
              <Icon size={18} /> {title}
            </Link>
          ))}
        </nav>
        <div className="sidebar-note">
          <span className="status-dot" /> Dados atualizados
          <small>Fontes oficiais consolidadas</small>
        </div>
      </aside>
      <main className="main">{children}</main>
    </div>
  );
}
