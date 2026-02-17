from __future__ import annotations

from pathlib import Path

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
IBC_PATH = DATA_DIR / "sgs_dados.parquet"
PPP_PATH = DATA_DIR / "indust_comer_serv.parquet"
IBCUF_PATH = DATA_DIR / "ibc_uf.parquet"  # (placeholder p/ você usar depois)


# ============================================================
# LOADERS
# ============================================================
@st.cache_data(show_spinner=False)
def load_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


@st.cache_data(show_spinner=False)
def load_sgs_monthly(path: Path) -> pd.DataFrame:
    """
    Loader mensal robusto (date em 'date', 'Date' ou índice).
    """
    df = pd.read_parquet(path).copy()

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    elif "Date" in df.columns:
        df["date"] = pd.to_datetime(df["Date"], errors="coerce")
    else:
        df = df.reset_index()
        candidates = [c for c in ["date", "Date", "index"] if c in df.columns]
        if candidates:
            df["date"] = pd.to_datetime(df[candidates[0]], errors="coerce")
        else:
            df["date"] = pd.to_datetime(df[df.columns[0]], errors="coerce")

    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


@st.cache_data(show_spinner=False)
def load_indus_comer_serv(path: Path) -> pd.DataFrame:
    """
    Loader mensal (PIM/PMC/PMS). Garante coluna 'date' e ordena.
    """
    df = pd.read_parquet(path).copy()
    if "date" not in df.columns:
        df = df.reset_index()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


# ============================================================
# TRANSFORMS / HELPERS
# ============================================================
def add_quarter_label(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["trimestre"] = out["date"].dt.to_period("Q").astype(str)
    return out


def last_value(df: pd.DataFrame, col: str):
    s = df[["date", col]].dropna().sort_values("date")
    if s.empty:
        return None, None
    return s.iloc[-1]["date"], float(s.iloc[-1][col])


def render_last_value_metrics(df: pd.DataFrame, cols: list[tuple[str, str]]):
    """
    cols = [(col_name, label), ...]
    """
    n = len(cols)
    cols_ui = st.columns(n)

    for i, (col, label) in enumerate(cols):
        if col not in df.columns:
            cols_ui[i].metric(label, "n/d")
            continue

        d, v = last_value(df, col)
        if v is None:
            cols_ui[i].metric(label, "n/d")
        else:
            cols_ui[i].metric(label, f"{v:.1f}%")


def wide_to_long(df: pd.DataFrame, date_col: str, value_cols: list[str], name_map: dict[str, str]) -> pd.DataFrame:
    out = df[[date_col] + value_cols].copy()
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
    plot_df = df[["date", col]].dropna().sort_values("date").rename(columns={col: "value"})
    fig = px.line(plot_df, x="date", y="value", title=title)
    fig.update_layout(xaxis_title="Data", yaxis_title=y_label)
    return fig


def compute_ibc_metrics(df: pd.DataFrame, col: str = "ibc_br") -> dict:
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

    # m/m (%)
    mom = None
    if len(s) >= 2:
        prev_value = float(s.iloc[-2])
        if prev_value != 0:
            mom = ((last_value / prev_value) - 1) * 100

    # 12m acumulado (%): soma últimos 12 / soma 12 anteriores
    acc12 = None
    if len(s) >= 24:
        last_12 = float(s.iloc[-12:].sum())
        prev_12 = float(s.iloc[-24:-12].sum())
        if prev_12 != 0:
            acc12 = ((last_12 / prev_12) - 1) * 100

    # YTD (%): soma jan..m_ref do ano atual / soma jan..m_ref do ano anterior
    ytd = None
    year = last_date.year
    month_ref = last_date.month

    cur_period = s[(s.index.year == year) & (s.index.month <= month_ref)]
    prev_period = s[(s.index.year == (year - 1)) & (s.index.month <= month_ref)]

    if (not cur_period.empty) and (not prev_period.empty):
        cur_sum = float(cur_period.sum())
        prev_sum = float(prev_period.sum())
        if prev_sum != 0:
            ytd = ((cur_sum / prev_sum) - 1) * 100

    return {"mom": mom, "acc12": acc12, "ytd": ytd, "last_date": last_date, "last_value": last_value}


# ============================================================
# UI SECTIONS (ABAS)
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
        plot_df = pib.copy()
        if plot_df.empty:
            st.warning("Sem dados de PIB.")
            return
        fig = build_pib_bar_figure(plot_df, dim_col=None)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Dados recentes", expanded=False):
            st.dataframe(plot_df.sort_values("date").tail(16), use_container_width=True)
        return

    # seletores
    options = sorted(pib[dim_col].dropna().unique().tolist())
    default_candidates = [
        "PIB a preços de mercado",
        "Formação bruta de capital fixo",
    ]
    default_sel = [x for x in default_candidates if x in options]
    if not default_sel:
        default_sel = options[:4]

    c1, c2 = st.columns([1, 1])
    with c1:
        select_all = st.button("Selecionar tudo", key="pib_select_all")
    with c2:
        clear_all = st.button("Limpar seleção", key="pib_clear_all")

    # default seguro
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
        st.warning("Nenhuma série selecionada. Selecione ao menos uma série para exibir o gráfico.")
        return

    fig = build_pib_bar_figure(plot_df, dim_col=dim_col)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Dados recentes (PIB e componentes)", expanded=False):
        df_view = (
            plot_df.sort_values("date")
            .groupby(dim_col, as_index=False)
            .tail(6)
        )
        st.dataframe(df_view, use_container_width=True)


def tab_dinamicas():
    st.subheader("Dinâmicas mensais — PIM / PMC / PMS (12 meses)")

    if not PPP_PATH.exists():
        st.info(f"Arquivo não encontrado: {PPP_PATH}")
        return

    ppp = load_indus_comer_serv(PPP_PATH)

    # métricas headline
    render_last_value_metrics(
        ppp,
        cols=[
            ("pim_12m", "PIM — 12 meses (%)"),
            ("pmc_12m", "PMC — 12 meses (%)"),
            ("pms_12m", "PMS — 12 meses (%)"),
        ],
    )

    series_map = {
        "pim_12m": "PIM — 12 meses (%)",
        "pmc_12m": "PMC — 12 meses (%)",
        "pms_12m": "PMS — 12 meses (%)",
    }

    series_cols = [c for c in series_map.keys() if c in ppp.columns]
    if not series_cols:
        st.warning("Não encontrei as colunas esperadas (pim_12m, pmc_12m, pms_12m).")
        st.write("Colunas disponíveis:", list(ppp.columns))
        return

    options = [series_map[c] for c in series_cols]
    default_sel = options[:]  # começa com todas

    c1, c2 = st.columns([1, 1])
    with c1:
        select_all = st.button("Selecionar tudo", key="ppp_select_all")
    with c2:
        clear_all = st.button("Limpar seleção", key="ppp_clear_all")

    # default seguro
    default_sel = [x for x in default_sel if x in options]
    if not default_sel and options:
        default_sel = [options[0]]

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
    fig = px.line(
        plot_long,
        x="date",
        y="value",
        color="serie",
        title="PIM / PMC / PMS — variação em 12 meses (%)",
    )
    fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)", legend_title_text="Série")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Dados recentes (PIM/PMC/PMS — 12m)", expanded=False):
        view_cols = ["date"] + selected_cols
        st.dataframe(ppp[view_cols].dropna().tail(18), use_container_width=True)


def tab_atividade_regional():
    st.subheader("Atividade regional — IBC")

    if not IBC_PATH.exists():
        st.info(f"Arquivo mensal não encontrado: {IBC_PATH}")
        return

    sgs_m = load_sgs_monthly(IBC_PATH)

    if "ibc_br" not in sgs_m.columns:
        st.warning("A coluna 'ibc_br' não foi encontrada em sgs_dados.parquet.")
        st.write("Colunas disponíveis:", list(sgs_m.columns))
        return

    m = compute_ibc_metrics(sgs_m, col="ibc_br")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Variação mensal (m/m)", f"{m['mom']:.2f}%" if m["mom"] is not None else "n/d")
    with c2:
        st.metric("12 meses (soma/soma)", f"{m['acc12']:.2f}%" if m["acc12"] is not None else "n/d")
    with c3:
        st.metric("Acumulado no ano (YTD)", f"{m['ytd']:.2f}%" if m["ytd"] is not None else "n/d")
    with c4:
        st.metric("Última referência", m["last_date"].strftime("%Y-%m") if m.get("last_date") is not None else "n/d")

    fig = build_line_figure(
        sgs_m,
        col="ibc_br",
        title="IBC-Br — índice (sem ajuste sazonal)",
        y_label="Índice",
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Dados mais recentes do IBC-Br", expanded=False):
        st.dataframe(
            sgs_m[["date", "ibc_br"]].dropna().sort_values("date").tail(24),
            use_container_width=True,
        )

    

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




