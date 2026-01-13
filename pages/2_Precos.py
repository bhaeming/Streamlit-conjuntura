from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# CONFIG / PAGE
# ============================================================
st.set_page_config(page_title="Preços ao consumidor e ao produtor", layout="wide")
st.title("Preços ao consumidor e ao produtor")


# ============================================================
# PATHS (dados processados)
# ============================================================
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "processed"

IPP_PATH = DATA_DIR / "ipp_m.parquet"
IPCA_GRUPOS_PATH = DATA_DIR / "ipca_grupos.parquet"
IPCA_ALL_PATH = DATA_DIR / "ipca_all.parquet"


# ============================================================
# HELPERS: LOADERS / METRICS / TRANSFORMS
# ============================================================

def _ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que exista a coluna 'date' (datetime), vindo de:
    - 'date'
    - 'Date'
    - índice
    """
    out = df.copy()

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    elif "Date" in out.columns:
        out["date"] = pd.to_datetime(out["Date"], errors="coerce")
    else:
        out = out.reset_index()
        candidates = [c for c in ["date", "Date", "index"] if c in out.columns]
        if candidates:
            out["date"] = pd.to_datetime(out[candidates[0]], errors="coerce")
        else:
            out["date"] = pd.to_datetime(out.iloc[:, 0], errors="coerce")

    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return out


@st.cache_data(show_spinner=False)
def load_parquet_with_date(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
    return _ensure_date_column(df)


@st.cache_data(show_spinner=False)
def load_ipp_long(path: Path) -> pd.DataFrame:
    """
    Esperado: colunas ['date','setor_ipp','value'] em formato long.
    """
    df = pd.read_parquet(path).copy()
    df = _ensure_date_column(df)

    # garante numérico
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["date", "value"]).sort_values("date").reset_index(drop=True)
    return df


@st.cache_data(show_spinner=False)
def load_ipca_grupos_long(path: Path) -> pd.DataFrame:
    """
    Esperado: colunas ['date','grupo','indicador','value'].
    """
    df = pd.read_parquet(path).copy()
    df = _ensure_date_column(df)

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "grupo", "indicador", "value"]).sort_values(["date", "grupo"])
    return df


def last_value(df: pd.DataFrame, col: str) -> tuple[pd.Timestamp | None, float | None]:
    if col not in df.columns:
        return None, None
    s = df[["date", col]].dropna().sort_values("date")
    if s.empty:
        return None, None
    return pd.Timestamp(s.iloc[-1]["date"]), float(s.iloc[-1][col])


def metric_last(df: pd.DataFrame, label: str, col: str, fmt: str = "{:.2f}%") -> None:
    d, v = last_value(df, col)
    if v is None:
        st.metric(label, "n/d")
    else:
        st.metric(label, fmt.format(v))


def wide_to_long(df: pd.DataFrame, cols: list[str], name_map: dict[str, str]) -> pd.DataFrame:
    keep = ["date"] + [c for c in cols if c in df.columns]
    out = df[keep].copy()
    out = out.melt(id_vars=["date"], var_name="serie", value_name="value")
    out["serie"] = out["serie"].map(name_map).fillna(out["serie"])
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["date", "value"]).sort_values("date")
    return out


def last_value_for_sector(ipp: pd.DataFrame, setor: str) -> tuple[pd.Timestamp | None, float | None]:
    if "setor_ipp" not in ipp.columns:
        return None, None
    s = ipp.loc[ipp["setor_ipp"] == setor, ["date", "value"]].dropna().sort_values("date")
    if s.empty:
        return None, None
    return pd.Timestamp(s.iloc[-1]["date"]), float(s.iloc[-1]["value"])


def ipca_contribuicoes(df_ipca_grupos: pd.DataFrame) -> pd.DataFrame:
    """
    Converte (variacao_mensal, peso_mensal) em contribuição em p.p. por grupo.
    """
    var_m = (
        df_ipca_grupos[df_ipca_grupos["indicador"] == "variacao_mensal"]
        .rename(columns={"value": "variacao_mensal"})
        [["date", "grupo", "variacao_mensal"]]
    )
    peso_m = (
        df_ipca_grupos[df_ipca_grupos["indicador"] == "peso_mensal"]
        .rename(columns={"value": "peso_mensal"})
        [["date", "grupo", "peso_mensal"]]
    )
    out = var_m.merge(peso_m, on=["date", "grupo"], how="inner")
    out["contrib_pp"] = out["variacao_mensal"] * out["peso_mensal"] / 100.0
    return out


# ============================================================
# SECTION 1 — IPCA (headline + gráfico)
# ============================================================
st.header("Inflação ao consumidor (IPCA)")

if not IPCA_ALL_PATH.exists():
    st.error(f"Arquivo não encontrado: {IPCA_ALL_PATH}")
    st.stop()

ipca_agg = load_parquet_with_date(IPCA_ALL_PATH)

needed = ["ipca", "ipca_12m", "ipca_livres_12m_calc", "ipca_administrados_12m_calc"]
missing = [c for c in needed if c not in ipca_agg.columns]
if missing:
    st.warning(f"Colunas ausentes em ipca_all.parquet: {missing}")
else:
    # métricas
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        metric_last(ipca_agg, "IPCA (mês)", "ipca", fmt="{:.2f}%")
    with c2:
        metric_last(ipca_agg, "IPCA (12m)", "ipca_12m", fmt="{:.2f}%")
    with c3:
        d_last, _ = last_value(ipca_agg, "ipca_12m")
        st.metric("Última referência", d_last.strftime("%Y-%m") if d_last is not None else "n/d")
    with c4:
        metric_last(ipca_agg, "IPCA livres (12m)", "ipca_livres_12m_calc", fmt="{:.2f}%")
    with c5:
        metric_last(ipca_agg, "IPCA administrados (12m)", "ipca_administrados_12m_calc", fmt="{:.2f}%")

    # gráfico (curvas em 12m + opcional mensal)
    series_map = {
        "ipca_12m": "IPCA (12m)",
        "ipca_livres_12m_calc": "IPCA livres (12m)",
        "ipca_administrados_12m_calc": "IPCA administrados (12m)",
        "ipca": "IPCA (mensal)",
    }
    options = list(series_map.values())

    colA, colB = st.columns([1, 1])
    with colA:
        select_all = st.button("Selecionar tudo", key="ipca_select_all")
    with colB:
        clear_all = st.button("Limpar seleção", key="ipca_clear_all")


    default_sel = ["IPCA (12m)"]
    default_sel = [x for x in default_sel if x in options]
    if not default_sel and options:
        default_sel = [options[0]]

    if clear_all:
        st.session_state["ipca_series"] = []
        selected_labels = []
    elif select_all:
        st.session_state["ipca_series"] = options
        selected_labels = options
    else:
        selected_labels = st.multiselect(
            "Selecionar séries (curvas)",
            options,
            default=default_sel,
            key="ipca_series",
        )

    inv = {v: k for k, v in series_map.items()}
    selected_cols = [inv[l] for l in selected_labels if l in inv]

    if not selected_cols:
        st.warning("Nenhuma série selecionada.")
    else:
        plot_long = wide_to_long(ipca_agg, selected_cols, series_map)
        fig = px.line(plot_long, x="date", y="value", color="serie", title="IPCA — curvas em 12 meses")
        fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)", legend_title_text="Série")
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Dados recentes (IPCA)", expanded=False):
        st.dataframe(
            ipca_agg[["date", "ipca", "ipca_12m"]].dropna().tail(24),
            use_container_width=True,
        )

st.divider()


# ============================================================
# SECTION 2 — Composição do IPCA (contribuições por grupo)
# ============================================================
st.subheader("Composição do IPCA mensal (contribuições por grupo)")

if not IPCA_GRUPOS_PATH.exists():
    st.info(f"Arquivo não encontrado: {IPCA_GRUPOS_PATH}")
    st.info("Gere o ipca_grupos.parquet no pipeline para habilitar esta visualização.")
else:
    ipca_g = load_ipca_grupos_long(IPCA_GRUPOS_PATH)
    contrib = ipca_contribuicoes(ipca_g)

    # referência mensal
    contrib["ref"] = contrib["date"].dt.to_period("M").astype(str)

    min_d = contrib["date"].min()
    max_d = contrib["date"].max()

    c_left, c_right = st.columns([2, 1])
    with c_left:
        date_range = st.slider(
            "Período",
            min_value=min_d.to_pydatetime(),
            max_value=max_d.to_pydatetime(),
            value=(min_d.to_pydatetime(), max_d.to_pydatetime()),
            key="ipca_comp_period",
        )

    start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    contrib_f = contrib[(contrib["date"] >= start) & (contrib["date"] <= end)].copy()

    if contrib_f.empty:
        st.warning("Sem dados no período selecionado.")
        st.stop()

    # remove agregados
    mask_geral = contrib_f["grupo"].str.contains(r"índice geral|geral|índice\s+cheio", case=False, na=False)
    contrib_f = contrib_f[~mask_geral].copy()

    with c_right:
        st.caption("Seleção de grupos")
        grupos_all = sorted(contrib_f["grupo"].dropna().unique().tolist())

        b1, b2 = st.columns(2)
        with b1:
            sel_all = st.button("Selecionar tudo", key="ipca_grupos_all")
        with b2:
            clr_all = st.button("Limpar", key="ipca_grupos_none")

        if sel_all:
            grupos_sel = grupos_all
        elif clr_all:
            grupos_sel = []
        else:
            grupos_sel = st.multiselect(
                "Escolher grupos (empilhado)",
                grupos_all,
                default=grupos_all,
                key="ipca_grupos_multiselect",
            )

        agrupar_outros = st.checkbox(
            "Agrupar não selecionados como 'Outros'",
            value=True,
            key="ipca_agrupar_outros",
        )

    if not grupos_sel:
        st.warning("Nenhum grupo selecionado.")
        st.stop()

    contrib_f["grupo_plot"] = contrib_f["grupo"]
    if agrupar_outros:
        contrib_f["grupo_plot"] = contrib_f["grupo"].where(contrib_f["grupo"].isin(grupos_sel), "Outros")
    else:
        contrib_f = contrib_f[contrib_f["grupo"].isin(grupos_sel)].copy()
        contrib_f["grupo_plot"] = contrib_f["grupo"]

    # stack por mês
    plot_stack = (
        contrib_f.groupby(["ref", "grupo_plot"], as_index=False)["contrib_pp"]
        .sum()
        .sort_values("ref")
    )
    plot_stack["date"] = pd.to_datetime(plot_stack["ref"] + "-01") + pd.offsets.MonthEnd(0)

    # total
    total_pp = (
        plot_stack.groupby("ref", as_index=False)["contrib_pp"]
        .sum()
        .rename(columns={"contrib_pp": "ipca_calc"})
        .sort_values("ref")
    )
    total_pp["date"] = pd.to_datetime(total_pp["ref"] + "-01") + pd.offsets.MonthEnd(0)

    # linha do índice geral (usa ipca mensal se tiver; senão usa somatório)
    line_df = total_pp[["date", "ref", "ipca_calc"]].copy()
    line_df["indice_geral"] = line_df["ipca_calc"]

    if "ipca" in ipca_agg.columns:
        tmp = ipca_agg[["date", "ipca"]].dropna().copy()
        tmp["ref"] = tmp["date"].dt.to_period("M").astype(str)
        ipca_headline = tmp.groupby("ref", as_index=False)["ipca"].last()
        line_df = line_df.merge(ipca_headline, on="ref", how="left")
        line_df["indice_geral"] = line_df["ipca"].combine_first(line_df["ipca_calc"])

    # métricas
    last_ref = total_pp["ref"].iloc[-1]
    last_calc = float(total_pp.loc[total_pp["ref"] == last_ref, "ipca_calc"].iloc[0])

    m1, m2 = st.columns([1, 1])
    with m1:
        st.metric("Última referência", last_ref)
    with m2:
        st.metric("Somatório das contribuições (p.p.)", f"{last_calc:.2f}%")

    # highlights
    last_month = contrib_f[contrib_f["ref"] == last_ref].copy()
    if not last_month.empty:
        rank = (
            last_month.groupby("grupo_plot", as_index=False)["contrib_pp"]
            .sum()
            .sort_values("contrib_pp")
        )
        worst = rank.head(1)
        best = rank.tail(1)

        h1, h2 = st.columns([1, 1])
        with h1:
            st.caption("Maior pressão altista (no mês)")
            st.write(f"**{best['grupo_plot'].iloc[0]}**: {float(best['contrib_pp'].iloc[0]):+.2f} p.p.")
        with h2:
            st.caption("Maior alívio (no mês)")
            st.write(f"**{worst['grupo_plot'].iloc[0]}**: {float(worst['contrib_pp'].iloc[0]):+.2f} p.p.")

    # gráfico combinado
    fig_combo = px.bar(
        plot_stack,
        x="date",
        y="contrib_pp",
        color="grupo_plot",
        title="IPCA mensal — contribuições por grupo (p.p.) + índice geral",
    )

    fig_combo.add_scatter(
        x=line_df["date"],
        y=line_df["indice_geral"],
        mode="lines+markers",
        name="Índice geral",
        yaxis="y2",
    )

    fig_combo.update_layout(
        barmode="stack",
        xaxis_title="Data",
        yaxis_title="Contribuição (p.p.)",
        yaxis2=dict(
            title="Índice geral (%)",
            overlaying="y",
            side="right",
            showgrid=False,
            zeroline=False,
        ),
        legend_title_text="Grupo",
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.25,
            xanchor="left",
            x=0,
        ),
        margin=dict(b=120),
    )

    st.plotly_chart(fig_combo, use_container_width=True, key="ipca_combo")


    with st.expander("Dados (contribuições)", expanded=False):
        st.dataframe(plot_stack.sort_values(["date", "grupo_plot"]).tail(36), use_container_width=True)

st.divider()


# ============================================================
# SECTION 3 — IPP
# ============================================================
st.header("Preços ao produtor em 12 meses (IPP)")

if not IPP_PATH.exists():
    st.info(f"Arquivo não encontrado: {IPP_PATH}")
    st.stop()

ipp = load_ipp_long(IPP_PATH)

required_cols = {"date", "setor_ipp", "value"}
if not required_cols.issubset(set(ipp.columns)):
    st.warning(f"IPP precisa ter as colunas {required_cols}. Colunas atuais: {list(ipp.columns)}")
else:
    setores = sorted(ipp["setor_ipp"].dropna().unique().tolist())

    col1, col2 = st.columns([2, 1])
    with col1:
        setor_sel = st.selectbox("Selecionar setor do IPP", setores, index=0)
    with col2:
        d_last, v_last = last_value_for_sector(ipp, setor_sel)
        st.metric("Última observação", f"{v_last:.2f}%" if v_last is not None else "n/d")

    modo = st.radio("Visualização", ["Setor selecionado", "Comparar setores"], horizontal=True)

    if modo == "Setor selecionado":
        plot_df = ipp[ipp["setor_ipp"] == setor_sel].sort_values("date")
        fig = px.line(plot_df, x="date", y="value", title=f"IPP — {setor_sel}")
        fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)")
    else:
        default_comp = setores[:3]
        comp_sel = st.multiselect("Selecionar setores para comparar", setores, default=default_comp, key="ipp_comp")
        plot_df = ipp[ipp["setor_ipp"].isin(comp_sel)].sort_values("date")
        fig = px.line(plot_df, x="date", y="value", color="setor_ipp", title="IPP — comparação entre setores")
        fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)", legend_title_text="Setor")

    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Dados recentes (IPP)", expanded=False):
        st.dataframe(ipp.sort_values("date").tail(36), use_container_width=True)
