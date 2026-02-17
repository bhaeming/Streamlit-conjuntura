from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="Preços ao consumidor e ao produtor", layout="wide")
st.title("Preços ao consumidor e ao produtor")


# ============================================================
# PATHS
# ============================================================
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "processed"

IPP_PATH = DATA_DIR / "ipp_m.parquet"
IPCA_GRUPOS_PATH = DATA_DIR / "ipca_grupos.parquet"
IPCA_ALL_PATH = DATA_DIR / "ipca_all.parquet"


# ============================================================
# HELPERS / LOADERS
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

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["date", "value"]).sort_values("date").reset_index(drop=True)
    return df


@st.cache_data(show_spinner=False)
def load_ipca_grupos_long(path: Path) -> pd.DataFrame:
    """
    Esperado: colunas ['date','grupo','indicador','value'].
    Indicadores observados: ['peso_mensal', 'variacao_12m', 'variacao_mensal'] (segundo você).
    """
    df = pd.read_parquet(path).copy()
    df = _ensure_date_column(df)

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["grupo"] = df["grupo"].astype(str).str.strip()
    df["indicador"] = df["indicador"].astype(str).str.strip()

    df = df.dropna(subset=["date", "grupo", "indicador", "value"]).sort_values(["date", "grupo"])
    return df


def wide_to_long(df: pd.DataFrame, cols: list[str], name_map: dict[str, str]) -> pd.DataFrame:
    keep = ["date"] + [c for c in cols if c in df.columns]
    out = df[keep].copy()
    out = out.melt(id_vars=["date"], var_name="serie", value_name="value")
    out["serie"] = out["serie"].map(name_map).fillna(out["serie"])
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["date", "value"]).sort_values("date")
    return out


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


def safe_multiselect(label: str, options: list[str], default: list[str], key: str) -> list[str]:
    """
    Garante que defaults existam em options (evita StreamlitAPIException).
    """
    default = [d for d in default if d in options]
    if not default and options:
        default = [options[0]]
    return st.multiselect(label, options, default=default, key=key)


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
        .rename(columns={"value": "variacao_mensal"})[["date", "grupo", "variacao_mensal"]]
    )
    peso_m = (
        df_ipca_grupos[df_ipca_grupos["indicador"] == "peso_mensal"]
        .rename(columns={"value": "peso_mensal"})[["date", "grupo", "peso_mensal"]]
    )

    out = var_m.merge(peso_m, on=["date", "grupo"], how="inner")
    out["contrib_pp"] = out["variacao_mensal"] * out["peso_mensal"] / 100.0
    return out


def groups_selector_ui(grupos_all: list[str], key_prefix: str, default_n: int = 8) -> list[str]:
    """
    UI padrão: selecionar tudo / limpar / multiselect
    """
    col1, col2 = st.columns([1, 1])
    with col1:
        sel_all = st.button("Selecionar tudo", key=f"{key_prefix}_all")
    with col2:
        clr_all = st.button("Limpar", key=f"{key_prefix}_clear")

    default = grupos_all[:default_n] if len(grupos_all) > default_n else grupos_all

    if clr_all:
        st.session_state[f"{key_prefix}_ms"] = []
        return []
    if sel_all:
        st.session_state[f"{key_prefix}_ms"] = grupos_all
        return grupos_all

    return st.multiselect(
        "Selecionar grupos",
        grupos_all,
        default=default,
        key=f"{key_prefix}_ms",
    )


def wide_ipca_grupos(df_long: pd.DataFrame, indicador: str) -> pd.DataFrame:
    """
    Converte df long (date, grupo, indicador, value) para wide:
    index=date, columns=grupo, values=value para um indicador específico.
    """
    tmp = df_long[df_long["indicador"] == indicador].copy()
    if tmp.empty:
        return pd.DataFrame()

    wide = (
        tmp.pivot_table(index="date", columns="grupo", values="value", aggfunc="last")
        .sort_index()
        .reset_index()
    )
    return wide


def last_ref_label(d: pd.Timestamp | None) -> str:
    if d is None or pd.isna(d):
        return "n/d"
    return pd.Timestamp(d).strftime("%Y-%m")


# ============================================================
# LOAD IPCA ALL once (usado em duas abas)
# ============================================================
ipca_agg: pd.DataFrame | None = None
if IPCA_ALL_PATH.exists():
    ipca_agg = load_parquet_with_date(IPCA_ALL_PATH)


# ============================================================
# TABS
# ============================================================
tabs = st.tabs(["IPCA agregado", "IPCA desagregado", "Preços ao produtor"])


# ============================================================
# TAB 1 — IPCA agregado
# ============================================================
with tabs[0]:
    st.subheader("IPCA agregado")

    if not IPCA_ALL_PATH.exists():
        st.error(f"Arquivo não encontrado: {IPCA_ALL_PATH}")
        st.stop()

    assert ipca_agg is not None

    needed = ["ipca", "ipca_12m", "ipca_livres_12m_calc", "ipca_administrados_12m_calc"]
    missing = [c for c in needed if c not in ipca_agg.columns]
    if missing:
        st.warning(f"Colunas ausentes em ipca_all.parquet: {missing}")
    else:
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            metric_last(ipca_agg, "IPCA (mês)", "ipca", fmt="{:.2f}%")
        with c2:
            metric_last(ipca_agg, "IPCA (12m)", "ipca_12m", fmt="{:.2f}%")
        with c3:
            d_last, _ = last_value(ipca_agg, "ipca_12m")
            st.metric("Última referência", last_ref_label(d_last))
        with c4:
            metric_last(ipca_agg, "IPCA livres (12m)", "ipca_livres_12m_calc", fmt="{:.2f}%")
        with c5:
            metric_last(ipca_agg, "IPCA administrados (12m)", "ipca_administrados_12m_calc", fmt="{:.2f}%")

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

        if clear_all:
            st.session_state["ipca_series"] = []
            selected_labels = []
        elif select_all:
            st.session_state["ipca_series"] = options
            selected_labels = options
        else:
            selected_labels = safe_multiselect(
                "Selecionar séries (curvas)",
                options,
                default_sel,
                key="ipca_series",
            )

        inv = {v: k for k, v in series_map.items()}
        selected_cols = [inv[l] for l in selected_labels if l in inv]

        if not selected_cols:
            st.warning("Nenhuma série selecionada.")
        else:
            plot_long = wide_to_long(ipca_agg, selected_cols, series_map)
            fig = px.line(plot_long, x="date", y="value", color="serie", title="IPCA — curvas selecionadas")
            fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)", legend_title_text="Série")
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("Dados recentes (IPCA)", expanded=False):
            st.dataframe(
                ipca_agg[["date", "ipca", "ipca_12m"]].dropna().tail(24),
                use_container_width=True,
            )


# ============================================================
# TAB 2 — IPCA desagregado
# ============================================================
with tabs[1]:
    st.subheader("IPCA desagregado")

    if not IPCA_GRUPOS_PATH.exists():
        st.info(f"Arquivo não encontrado: {IPCA_GRUPOS_PATH}")
        st.info("Gere o ipca_grupos.parquet no pipeline para habilitar esta visualização.")
        st.stop()

    ipca_g = load_ipca_grupos_long(IPCA_GRUPOS_PATH)

    # ===========================
    # (CORRIGIDO) Linha 12m por grupo (fica SOMENTE nesta aba)
    # + layout 2 colunas (gráfico | tabela)
    # ===========================
    indicadores_disponiveis = set(ipca_g["indicador"].dropna().unique().tolist())
    if "variacao_12m" not in indicadores_disponiveis:
        st.warning("Não encontrei 'variacao_12m' no ipca_grupos.parquet.")
    else:
        # referência mais recente (pelo arquivo long)
        last_date = ipca_g["date"].max()
        last_ref = last_ref_label(last_date)

        st.markdown(f"### IPCA por grupo — variação em 12 meses (%) | referência {last_ref}")

        # wide 12m p/ gráfico
        ipca12_wide = wide_ipca_grupos(ipca_g, "variacao_12m")

        if ipca12_wide.empty:
            st.warning("Sem dados de variacao_12m por grupo.")
        else:
            grupos_all = sorted([c for c in ipca12_wide.columns if c != "date"])
            grupos_sel = groups_selector_ui(grupos_all, key_prefix="ipca12_grupos", default_n=10)

            col_left, col_right = st.columns([2, 1])

            # ---- COLUNA ESQUERDA: gráfico 12m
            with col_left:
                if not grupos_sel:
                    st.warning("Selecione ao menos um grupo para exibir a curva em 12 meses.")
                else:
                    plot_long_12m = ipca12_wide[["date"] + grupos_sel].melt(
                        id_vars=["date"],
                        var_name="grupo",
                        value_name="value",
                    )
                    plot_long_12m["value"] = pd.to_numeric(plot_long_12m["value"], errors="coerce")
                    plot_long_12m = plot_long_12m.dropna(subset=["date", "value"]).sort_values("date")

                    fig_12m = px.line(
                        plot_long_12m,
                        x="date",
                        y="value",
                        color="grupo",
                        title=f"Variação em 12 meses — por grupo (ref. {last_ref})",
                    )
                    fig_12m.update_layout(
                        xaxis_title="Data",
                        yaxis_title="Variação (%)",
                        legend_title_text="Grupo",
                    )
                    st.plotly_chart(fig_12m, use_container_width=True)

# ---- COLUNA DIREITA: tabela (grupo | peso | var mensal | var 12m | contribuição)
            with col_right:
                st.markdown(
                    f"**Tabela — pesos, variação e contribuição do IPCA (ref. {last_ref})**"
                )

                # dados do último mês
                last_month = ipca_g[ipca_g["date"] == last_date].copy()

                peso_last = (
                    last_month[last_month["indicador"] == "peso_mensal"][["grupo", "value"]]
                    .rename(columns={"value": "peso_mensal"})
                )
                vm_last = (
                    last_month[last_month["indicador"] == "variacao_mensal"][["grupo", "value"]]
                    .rename(columns={"value": "variacao_mensal"})
                )
                v12_last = (
                    last_month[last_month["indicador"] == "variacao_12m"][["grupo", "value"]]
                    .rename(columns={"value": "variacao_12m"})
                )

                # merge geral
                table = (
                    peso_last.merge(vm_last, on="grupo", how="outer")
                            .merge(v12_last, on="grupo", how="outer")
                )

                # filtra grupos selecionados (se houver)
                if grupos_sel:
                    table = table[table["grupo"].isin(grupos_sel)].copy()

                # garantir numéricos
                for c in ["peso_mensal", "variacao_mensal", "variacao_12m"]:
                    table[c] = pd.to_numeric(table[c], errors="coerce")

                # contribuição mensal em p.p.
                table["contrib_pp"] = table["peso_mensal"] * table["variacao_mensal"] / 100.0

                # rank por contribuição (maior impacto primeiro)
                table = table.sort_values("contrib_pp", ascending=False, na_position="last")
                table["Rank"] = range(1, len(table) + 1)

                # seleção e renomeação final
                table_view = table[
                    ["Rank", "grupo", "peso_mensal", "variacao_mensal", "variacao_12m", "contrib_pp"]
                ].rename(
                    columns={
                        "grupo": "Grupo",
                        "peso_mensal": "Peso (%)",
                        "variacao_mensal": "Variação mensal (%)",
                        "variacao_12m": "Variação 12m (%)",
                        "contrib_pp": "Contribuição (p.p.)",
                    }
                )

                # formatação visual
                table_view["Peso (%)"] = table_view["Peso (%)"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
                table_view["Variação mensal (%)"] = table_view["Variação mensal (%)"].map(
                    lambda x: f"{x:+.2f}" if pd.notna(x) else "—"
                )
                table_view["Variação 12m (%)"] = table_view["Variação 12m (%)"].map(
                    lambda x: f"{x:+.2f}" if pd.notna(x) else "—"
                )
                table_view["Contribuição (p.p.)"] = table_view["Contribuição (p.p.)"].map(
                    lambda x: f"{x:+.3f}" if pd.notna(x) else "—"
                )

                st.dataframe(table_view, use_container_width=True, hide_index=True)


                with st.expander("Dados (variação 12m por grupo)", expanded=False):
                    st.dataframe(ipca12_wide.tail(24), use_container_width=True)

    st.divider()

    # ============================================================
    # 2.2) Contribuições (barras empilhadas) + linha do índice geral (mensal)
    # ============================================================
    st.markdown("### Contribuições do IPCA mensal por grupo (p.p.)")

    contrib = ipca_contribuicoes(ipca_g)
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

    mask_geral = contrib_f["grupo"].str.contains(r"índice geral|geral|índice\s+cheio", case=False, na=False)
    contrib_f = contrib_f[~mask_geral].copy()

    with c_right:
        st.caption("Seleção de grupos (barras)")
        grupos_all_b = sorted(contrib_f["grupo"].dropna().unique().tolist())

        b1, b2 = st.columns(2)
        with b1:
            sel_all = st.button("Selecionar tudo", key="ipca_grupos_all_b")
        with b2:
            clr_all = st.button("Limpar", key="ipca_grupos_none_b")

        if sel_all:
            grupos_sel_b = grupos_all_b
        elif clr_all:
            grupos_sel_b = []
        else:
            grupos_sel_b = safe_multiselect(
                "Escolher grupos (empilhado)",
                grupos_all_b,
                default=grupos_all_b,
                key="ipca_grupos_multiselect_b",
            )

        agrupar_outros = st.checkbox(
            "Agrupar não selecionados como 'Outros'",
            value=True,
            key="ipca_agrupar_outros_b",
        )

    if not grupos_sel_b:
        st.warning("Nenhum grupo selecionado.")
        st.stop()

    if agrupar_outros:
        contrib_f["grupo_plot"] = contrib_f["grupo"].where(contrib_f["grupo"].isin(grupos_sel_b), "Outros")
    else:
        contrib_f = contrib_f[contrib_f["grupo"].isin(grupos_sel_b)].copy()
        contrib_f["grupo_plot"] = contrib_f["grupo"]

    plot_stack = (
        contrib_f.groupby(["ref", "grupo_plot"], as_index=False)["contrib_pp"]
        .sum()
        .sort_values("ref")
    )
    plot_stack["date"] = pd.to_datetime(plot_stack["ref"] + "-01") + pd.offsets.MonthEnd(0)

    total_pp = (
        plot_stack.groupby("ref", as_index=False)["contrib_pp"]
        .sum()
        .rename(columns={"contrib_pp": "ipca_calc"})
        .sort_values("ref")
    )
    total_pp["date"] = pd.to_datetime(total_pp["ref"] + "-01") + pd.offsets.MonthEnd(0)

    line_df = total_pp[["date", "ref", "ipca_calc"]].copy()
    line_df["indice_geral"] = line_df["ipca_calc"]

    if ipca_agg is not None and "ipca" in ipca_agg.columns:
        tmp = ipca_agg[["date", "ipca"]].dropna().copy()
        tmp["ref"] = tmp["date"].dt.to_period("M").astype(str)
        ipca_headline = tmp.groupby("ref", as_index=False)["ipca"].last()
        line_df = line_df.merge(ipca_headline, on="ref", how="left")
        line_df["indice_geral"] = line_df["ipca"].combine_first(line_df["ipca_calc"])

    fig_combo = px.bar(
        plot_stack,
        x="date",
        y="contrib_pp",
        color="grupo_plot",
        title="IPCA mensal — contribuições por grupo (p.p.) + índice geral (mensal)",
    )

    fig_combo.add_scatter(
        x=line_df["date"],
        y=line_df["indice_geral"],
        mode="lines+markers",
        name="Índice geral (mensal)",
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


# ============================================================
# TAB 3 — IPP
# ============================================================
with tabs[2]:
    st.subheader("Preços ao produtor (IPP)")

    if not IPP_PATH.exists():
        st.info(f"Arquivo não encontrado: {IPP_PATH}")
        st.stop()

    ipp = load_ipp_long(IPP_PATH)

    required_cols = {"date", "setor_ipp", "value"}
    if not required_cols.issubset(set(ipp.columns)):
        st.warning(f"IPP precisa ter as colunas {required_cols}. Colunas atuais: {list(ipp.columns)}")
        st.stop()

    setores = sorted(ipp["setor_ipp"].dropna().unique().tolist())
    if not setores:
        st.warning("Não encontrei setores em 'setor_ipp'.")
        st.stop()

    col1, col2 = st.columns([2, 1])
    with col1:
        setor_sel = st.selectbox("Selecionar setor do IPP", setores, index=0, key="ipp_setor_sel")
    with col2:
        d_last, v_last = last_value_for_sector(ipp, setor_sel)
        st.metric("Última observação", f"{v_last:.2f}%" if v_last is not None else "n/d")

    modo = st.radio("Visualização", ["Setor selecionado", "Comparar setores"], horizontal=True, key="ipp_mode")

    if modo == "Setor selecionado":
        plot_df = ipp[ipp["setor_ipp"] == setor_sel].sort_values("date")
        fig = px.line(plot_df, x="date", y="value", title=f"IPP — {setor_sel}")
        fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)")
    else:
        default_comp = setores[:3]
        comp_sel = st.multiselect(
            "Selecionar setores para comparar",
            setores,
            default=default_comp,
            key="ipp_comp",
        )
        if not comp_sel:
            st.warning("Selecione ao menos um setor.")
            st.stop()

        plot_df = ipp[ipp["setor_ipp"].isin(comp_sel)].sort_values("date")
        fig = px.line(plot_df, x="date", y="value", color="setor_ipp", title="IPP — comparação entre setores")
        fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)", legend_title_text="Setor")

    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Dados recentes (IPP)", expanded=False):
        st.dataframe(ipp.sort_values("date").tail(36), use_container_width=True)
