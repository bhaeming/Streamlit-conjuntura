from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="Dinâmica econômica", layout="wide")
st.title("Dinâmica econômica")


# ============================================================
# PATHS
# ============================================================
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "processed"

PIB_PATH = DATA_DIR / "pibs_quarterly.parquet"
IBC_PATH = DATA_DIR / "sgs_dados.parquet"          # IBC-Br + crédito + juros etc
PPP_PATH = DATA_DIR / "indust_comer_serv.parquet"  # PIM/PMC/PMS
IBCUF_PATH = DATA_DIR / "ibc_uf.parquet"           # IBC por UF (SGS)


# ============================================================
# LOADERS
# ============================================================
def _ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante coluna 'date' em datetime:
    - se existir 'date'
    - senão 'Date'
    - senão usa índice / primeira coluna após reset_index
    """
    out = df.copy()

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    elif "Date" in out.columns:
        out["date"] = pd.to_datetime(out["Date"], errors="coerce")
    else:
        out = out.reset_index()
        # tenta candidatos comuns
        candidates = [c for c in ["date", "Date", "index"] if c in out.columns]
        if candidates:
            out["date"] = pd.to_datetime(out[candidates[0]], errors="coerce")
        else:
            out["date"] = pd.to_datetime(out.iloc[:, 0], errors="coerce")

    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return out


@st.cache_data(show_spinner=False)
def load_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
    return _ensure_date_column(df)


@st.cache_data(show_spinner=False)
def load_sgs_monthly(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
    df = _ensure_date_column(df)
    return df


@st.cache_data(show_spinner=False)
def load_indus_comer_serv(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
    df = _ensure_date_column(df)
    return df


@st.cache_data(show_spinner=False)
def load_ibc_uf(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
    df = _ensure_date_column(df)
    return df


# ============================================================
# HELPERS
# ============================================================
def add_quarter_label(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["trimestre"] = out["date"].dt.to_period("Q").astype(str)
    return out


def last_value(df: pd.DataFrame, col: str):
    if col not in df.columns:
        return None, None
    s = df[["date", col]].dropna().sort_values("date")
    if s.empty:
        return None, None
    return s.iloc[-1]["date"], float(s.iloc[-1][col])


def wide_to_long(df: pd.DataFrame, date_col: str, value_cols: list[str], name_map: dict[str, str]) -> pd.DataFrame:
    keep = [date_col] + [c for c in value_cols if c in df.columns]
    out = df[keep].copy()
    out = out.melt(id_vars=[date_col], var_name="serie", value_name="value")
    out["serie"] = out["serie"].map(name_map).fillna(out["serie"])
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    return out.dropna(subset=[date_col, "value"]).sort_values(date_col)


def build_pib_bar_figure(df: pd.DataFrame, dim_col: str | None):
    if dim_col is None:
        fig = px.bar(
            df.sort_values("date"),
            x="trimestre",
            y="value",
            title="PIB — em relação ao mesmo período do ano anterior (%)",
        )
    else:
        fig = px.bar(
            df.sort_values("date"),
            x="trimestre",
            y="value",
            color=dim_col,
            barmode="group",
            title="PIB — em relação ao mesmo período do ano anterior (%)",
        )

    fig.update_layout(
        xaxis_title="Trimestre",
        yaxis_title="Valor",
        bargap=0.15,
        legend_title_text=(dim_col if dim_col else ""),
    )
    return fig


def build_line_figure(df: pd.DataFrame, col: str, title: str, y_label: str):
    if col not in df.columns:
        return None
    plot_df = df[["date", col]].dropna().sort_values("date").rename(columns={col: "value"})
    if plot_df.empty:
        return None
    fig = px.line(plot_df, x="date", y="value", title=title)
    fig.update_layout(xaxis_title="Data", yaxis_title=y_label)
    return fig


# ------------------------------
# 12 meses (soma/soma) — igual BCB
# ------------------------------
def sum_ratio_12m(series: pd.Series) -> pd.Series:
    """
    (soma últimos 12 / soma 12 anteriores) - 1, em %
    Rolling robusto (evita erro por iloc e por buracos).
    """
    s = series.dropna().sort_index().astype(float)

    sum_last_12 = s.rolling(window=12, min_periods=12).sum()
    sum_prev_12 = sum_last_12.shift(12)

    out = (sum_last_12 / sum_prev_12 - 1.0) * 100.0
    return out


def compute_ibc_metrics(df: pd.DataFrame, col: str) -> dict:
    """
    Métricas padrão:
    - m/m (%)
    - 12m soma/soma (%)
    - YTD soma jan..m / soma jan..m ano anterior (%)
    - last_date, last_value
    """
    if col not in df.columns:
        return {"mom": None, "acc12": None, "ytd": None, "last_date": None, "last_value": None}

    s = (
        df[["date", col]]
        .dropna()
        .sort_values("date")
        .set_index("date")[col]
        .astype(float)
    )
    if s.empty:
        return {"mom": None, "acc12": None, "ytd": None, "last_date": None, "last_value": None}

    last_date = s.index.max()
    last_value = float(s.loc[last_date])

    # m/m
    mom = None
    if len(s) >= 2:
        prev = float(s.iloc[-2])
        if prev != 0:
            mom = ((last_value / prev) - 1) * 100

    # 12m soma/soma (BCB)
    acc12 = None
    acc12_s = sum_ratio_12m(s)
    if not acc12_s.dropna().empty:
        acc12 = float(acc12_s.dropna().iloc[-1])

    # YTD soma/soma
    ytd = None
    year = last_date.year
    month_ref = last_date.month
    cur = s[(s.index.year == year) & (s.index.month <= month_ref)]
    prev = s[(s.index.year == (year - 1)) & (s.index.month <= month_ref)]
    if (not cur.empty) and (not prev.empty):
        cur_sum = float(cur.sum())
        prev_sum = float(prev.sum())
        if prev_sum != 0:
            ytd = ((cur_sum / prev_sum) - 1) * 100

    return {"mom": mom, "acc12": acc12, "ytd": ytd, "last_date": last_date, "last_value": last_value}


# ------------------------------
# Radar: crescimento por UF no ano selecionado
# ------------------------------
UF_LABELS = {
    # regiões
    "ibc_norte": "Norte",
    "ibc_ne": "Nordeste",
    "ibc_sul": "Sul",
    "ibc_co": "Centro-Oeste",
    "ibc_se": "Sudeste",
    # UFs (as que você tem)
    "ibc_am": "AM",
    "ibc_pa": "PA",
    "ibc_ce": "CE",
    "ibc_pe": "PE",
    "ibc_ba": "BA",
    "ibc_es": "ES",
    "ibc_mg": "MG",
    "ibc_rj": "RJ",
    "ibc_sp": "SP",
    "ibc_pr": "PR",
    "ibc_sc": "SC",
    "ibc_rs": "RS",
    "ibc_go": "GO",
}


def year_sum_growth(df: pd.DataFrame, col: str, year: int) -> float | None:
    """
    Crescimento no ano (como o '12 months' em dezembro):
    sum(Jan..Dez year) / sum(Jan..Dez year-1) - 1
    """
    if col not in df.columns:
        return None

    s = df[["date", col]].dropna().copy()
    if s.empty:
        return None

    s["date"] = pd.to_datetime(s["date"], errors="coerce")
    s = s.dropna(subset=["date"]).sort_values("date")
    s[col] = pd.to_numeric(s[col], errors="coerce")

    cur = s[s["date"].dt.year == year][col].dropna()
    prev = s[s["date"].dt.year == (year - 1)][col].dropna()

    # ideal: 12 observações em cada ano
    if len(cur) < 12 or len(prev) < 12:
        return None

    cur_sum = float(cur.sum())
    prev_sum = float(prev.sum())
    if prev_sum == 0:
        return None

    return (cur_sum / prev_sum - 1.0) * 100.0


def build_uf_growth_radar(
    df_uf: pd.DataFrame,
    year: int,
    use_dessaz: bool,
    include_regions: bool,
) -> tuple[object | None, pd.DataFrame]:
    suffix = "_dessaz" if use_dessaz else ""
    rows = []

    keys = []
    for k in UF_LABELS.keys():
        if (not include_regions) and k in {"ibc_norte", "ibc_ne", "ibc_sul", "ibc_co", "ibc_se"}:
            continue
        keys.append(k)

    for base_col in keys:
        col = f"{base_col}{suffix}"
        label = UF_LABELS.get(base_col, base_col.replace("ibc_", "").upper())

        g = year_sum_growth(df_uf, col=col, year=year)
        if g is not None and np.isfinite(g):
            rows.append({"UF": label, "crescimento_%": float(g)})

    if not rows:
        return None, pd.DataFrame(columns=["UF", "crescimento_%"])

    table = pd.DataFrame(rows).sort_values("crescimento_%", ascending=False).reset_index(drop=True)

    # fecha o polígono
    radar_df = pd.concat([table, table.iloc[[0]]], ignore_index=True)

    fig = px.line_polar(
        radar_df,
        r="crescimento_%",
        theta="UF",
        line_close=True,
        title=f"Crescimento por UF — {year} (soma/soma, %)",
    )

    fig.update_traces(
        line=dict(width=2),
        # fill="toself",  # se quiser preencher: descomente
        # opacity=0.85,
    )

    fig.update_layout(
        # fundo transparente (remove branco)
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="rgba(255,255,255,0.92)"),

        # polar (círculo) também transparente/escuro
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(
                showline=True,
                linewidth=1,
                linecolor="rgba(255,255,255,0.25)",
                gridcolor="rgba(255,255,255,0.18)",
                tickfont=dict(color="rgba(255,255,255,0.75)"),
                ticksuffix="%",
            ),
            angularaxis=dict(
                showline=True,
                linewidth=1,
                linecolor="rgba(255,255,255,0.25)",
                gridcolor="rgba(255,255,255,0.12)",
                tickfont=dict(color="rgba(255,255,255,0.85)"),
            ),
        ),

        showlegend=False,
        margin=dict(l=10, r=10, t=60, b=10),
        title=dict(x=0.02, xanchor="left"),
    )

    return fig, table


# ============================================================
# UI TABS
# ============================================================
def tab_pib():
    st.subheader("PIB (IBGE) — trimestral")

    if not PIB_PATH.exists():
        st.error(f"Arquivo não encontrado: {PIB_PATH}")
        return

    pib = load_parquet(PIB_PATH)
    pib = add_quarter_label(pib)

    possible_dim_cols = [c for c in ["setor", "grupo"] if c in pib.columns]
    dim_col = possible_dim_cols[0] if possible_dim_cols else None

    if dim_col is None:
        st.info("Não encontrei dimensão ('setor' ou 'grupo'). Mostrando série agregada.")
        fig = build_pib_bar_figure(pib, dim_col=None)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Dados recentes", expanded=False):
            st.dataframe(pib.sort_values("date").tail(16), use_container_width=True)
        return

    options = sorted(pib[dim_col].dropna().unique().tolist())
    default_candidates = ["PIB a preços de mercado", "Formação bruta de capital fixo"]
    default_sel = [x for x in default_candidates if x in options] or options[:4]

    c1, c2 = st.columns([1, 1])
    with c1:
        select_all = st.button("Selecionar tudo", key="pib_select_all")
    with c2:
        clear_all = st.button("Limpar seleção", key="pib_clear_all")

    default_sel = [x for x in default_sel if x in options]
    if not default_sel and options:
        default_sel = [options[0]]

    if clear_all:
        st.session_state["pib_series"] = []
        selected = []
    elif select_all:
        st.session_state["pib_series"] = options
        selected = options
    else:
        selected = st.multiselect("Selecionar séries", options, default=default_sel, key="pib_series")

    plot_df = pib[pib[dim_col].isin(selected)].copy() if selected else pd.DataFrame()

    if plot_df.empty:
        st.warning("Nenhuma série selecionada.")
        return

    fig = build_pib_bar_figure(plot_df, dim_col=dim_col)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Dados recentes (PIB e componentes)", expanded=False):
        df_view = plot_df.sort_values("date").groupby(dim_col, as_index=False).tail(6)
        st.dataframe(df_view, use_container_width=True)


def tab_dinamicas():
    st.subheader("Dinâmicas mensais — PIM / PMC / PMS (12 meses)")

    if not PPP_PATH.exists():
        st.info(f"Arquivo não encontrado: {PPP_PATH}")
        return

    ppp = load_indus_comer_serv(PPP_PATH)

    # métricas headline
    m1, m2, m3 = st.columns(3)
    for i, (col, label) in enumerate(
        [
            ("pim_12m", "PIM — 12 meses (%)"),
            ("pmc_12m", "PMC — 12 meses (%)"),
            ("pms_12m", "PMS — 12 meses (%)"),
        ]
    ):
        d, v = last_value(ppp, col)
        (m1, m2, m3)[i].metric(label, f"{v:.1f}%" if v is not None else "n/d")

    series_map = {"pim_12m": "PIM — 12 meses (%)", "pmc_12m": "PMC — 12 meses (%)", "pms_12m": "PMS — 12 meses (%)"}
    series_cols = [c for c in series_map.keys() if c in ppp.columns]
    if not series_cols:
        st.warning("Não encontrei colunas pim_12m/pmc_12m/pms_12m.")
        st.write("Colunas disponíveis:", list(ppp.columns))
        return

    options = [series_map[c] for c in series_cols]
    default_sel = options[:]  # todas

    c1, c2 = st.columns([1, 1])
    with c1:
        select_all = st.button("Selecionar tudo", key="ppp_select_all")
    with c2:
        clear_all = st.button("Limpar seleção", key="ppp_clear_all")

    if clear_all:
        st.session_state["ppp_series"] = []
        selected_labels = []
    elif select_all:
        st.session_state["ppp_series"] = options
        selected_labels = options
    else:
        selected_labels = st.multiselect("Selecionar séries", options, default=default_sel, key="ppp_series")

    inv_map = {v: k for k, v in series_map.items()}
    selected_cols = [inv_map[l] for l in selected_labels if l in inv_map]

    if not selected_cols:
        st.warning("Nenhuma série selecionada.")
        return

    plot_long = wide_to_long(ppp, date_col="date", value_cols=selected_cols, name_map=series_map)
    fig = px.line(plot_long, x="date", y="value", color="serie", title="PIM / PMC / PMS — variação em 12 meses (%)")
    fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)", legend_title_text="Série")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Dados recentes (PIM/PMC/PMS — 12m)", expanded=False):
        st.dataframe(ppp[["date"] + selected_cols].dropna().tail(18), use_container_width=True)


def tab_atividade_regional():
    st.subheader("IBC-Br e IBC por UF")

    if not IBC_PATH.exists():
        st.info(f"Arquivo não encontrado: {IBC_PATH}")
        return

    sgs_m = load_sgs_monthly(IBC_PATH)

    # ===== Layout 2 colunas (esq: IBC-Br; dir: radar) =====
    col_left, col_right = st.columns([2.2, 1.2], gap="large")

    with col_left:
        # Série do IBC-Br (observada vs dessaz)
        use_dessaz_br = st.checkbox("Usar dessazonalizado (IBC-Br)", value=False, key="ibc_br_dessaz")

        col_series = "ibc_br_dessaz" if use_dessaz_br else "ibc_br"
        if col_series not in sgs_m.columns:
            st.warning(f"Não encontrei a coluna '{col_series}' em sgs_dados.parquet.")
            st.write("Colunas disponíveis:", list(sgs_m.columns))
            return

        m = compute_ibc_metrics(sgs_m, col=col_series)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("m/m", f"{m['mom']:.2f}%" if m["mom"] is not None else "n/d")
        c2.metric("12m (soma/soma)", f"{m['acc12']:.2f}%" if m["acc12"] is not None else "n/d")
        c3.metric("YTD", f"{m['ytd']:.2f}%" if m["ytd"] is not None else "n/d")
        c4.metric("Última ref.", m["last_date"].strftime("%Y-%m") if m.get("last_date") is not None else "n/d")

        # Linha do IBC-Br
        title = "IBC-Br — índice (dessazonalizado)" if use_dessaz_br else "IBC-Br — índice (observado)"
        fig = build_line_figure(sgs_m, col=col_series, title=title, y_label="Índice")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados suficientes para plotar o IBC-Br.")

        with st.expander("Dados recentes (IBC-Br)", expanded=False):
            st.dataframe(sgs_m[["date", col_series]].dropna().tail(24), use_container_width=True)

    with col_right:
        st.markdown("### Crescimento por UF (radar)")

        if not IBCUF_PATH.exists():
            st.info(f"Arquivo não encontrado: {IBCUF_PATH}")
            return

        df_uf = load_ibc_uf(IBCUF_PATH)

        # ano disponível
        years = sorted(df_uf["date"].dt.year.dropna().unique().tolist())
        years = [int(y) for y in years if np.isfinite(y)]
        if not years:
            st.warning("Não encontrei anos válidos em ibc_uf.parquet.")
            return

        year_sel = st.selectbox("Ano", years[::-1], index=0, key="uf_year_sel")  # maior primeiro
        use_dessaz = st.checkbox("Usar dessazonalizado", value=False, key="uf_dessaz")
        include_regions = st.checkbox("Incluir regiões", value=False, key="uf_regions")

        fig_radar, table = build_uf_growth_radar(
            df_uf=df_uf,
            year=int(year_sel),
            use_dessaz=use_dessaz,
            include_regions=include_regions,
        )

        if fig_radar is None or table.empty:
            st.info(
                "Sem dados suficientes para calcular o crescimento no ano selecionado. "
                "Preciso de 12 observações no ano e 12 no ano anterior, para cada UF."
            )
        else:
            st.plotly_chart(fig_radar, use_container_width=True)

            # Tabela ao lado do radar (já está na coluna direita)
            st.markdown(f"**Tabela — crescimento (soma/soma) no ano {year_sel}**")
            table_view = table.rename(columns={"UF": "UF", "crescimento_%": "Crescimento (%)"}).copy()
            table_view["Crescimento (%)"] = table_view["Crescimento (%)"].map(lambda x: f"{x:.2f}%")
            st.dataframe(table_view, use_container_width=True)


# ============================================================
# APP
# ============================================================
tabs = st.tabs(["PIB", "Dinâmicas", "Atividade regional"])

with tabs[0]:
    tab_pib()

with tabs[1]:
    tab_dinamicas()

with tabs[2]:
    tab_atividade_regional()