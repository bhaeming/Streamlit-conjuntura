from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# -----------------------
# Configuração da página - Cabeçalho da página (tem que se iniciar por aqui)
# -----------------------
st.set_page_config(page_title="Preços ao consumidor e ao produtor", layout="wide")
st.title("Preços ao consumidor e ao produtor")


# -----------------------
# Caminhos para o data set
# -----------------------

BASE_DIR = Path(__file__).resolve().parents[1]  # dados/ processed - acessa os dados tratados
DATA_DIR = BASE_DIR / "data" / "processed"

SGS_PATH = DATA_DIR / "sgs_dados.parquet"
IPP_PATH = DATA_DIR / "ipp_m.parquet"
IPCA_GRUPOS_PATH = DATA_DIR / "ipca_grupos.parquet"


# -----------------------
# Loaders
# -----------------------
@st.cache_data(show_spinner=False)

def load_monthly_parquet_flexible(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()

    # Caso 1: já existe coluna date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Caso 2: existe coluna Date
    elif "Date" in df.columns:
        df["date"] = pd.to_datetime(df["Date"], errors="coerce")

    # Caso 3: data está no índice (com qualquer nome)
    else:
        df = df.reset_index()

        # tenta achar uma coluna de data entre as primeiras mais prováveis
        candidates = [c for c in ["date", "Date", "index"] if c in df.columns]
        if candidates:
            df["date"] = pd.to_datetime(df[candidates[0]], errors="coerce")
        else:
            # fallback: tenta usar a primeira coluna e converter
            first_col = df.columns[0]
            df["date"] = pd.to_datetime(df[first_col], errors="coerce")

    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df



@st.cache_data(show_spinner=False)
def load_ipp_long(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()

    # garante colunas esperadas
    if "date" not in df.columns:
        df = df.reset_index()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["date", "value"]).sort_values("date").reset_index(drop=True)
    return df


def last_value(df: pd.DataFrame, col: str):
    s = df[["date", col]].dropna().sort_values("date")
    if s.empty:
        return None, None
    return s.iloc[-1]["date"], float(s.iloc[-1][col])


def metric_last(df: pd.DataFrame, label: str, col: str, fmt: str = "{:.2f}%"):
    d, v = last_value(df, col)
    if v is None:
        st.metric(label, "n/d")
    else:
        st.metric(label, fmt.format(v))


def build_line(df: pd.DataFrame, col: str, title: str, y_label: str):
    plot_df = df[["date", col]].dropna().rename(columns={col: "value"}).sort_values("date")
    fig = px.line(plot_df, x="date", y="value", title=title)
    fig.update_layout(xaxis_title="Data", yaxis_title=y_label)
    return fig


def wide_to_long(df: pd.DataFrame, cols: list[str], name_map: dict[str, str]) -> pd.DataFrame:
    out = df[["date"] + cols].copy()
    out = out.melt(id_vars=["date"], var_name="serie", value_name="value")
    out["serie"] = out["serie"].map(name_map).fillna(out["serie"])
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["date", "value"]).sort_values("date")
    return out


def last_value_for_sector(ipp: pd.DataFrame, setor: str):
    s = ipp.loc[ipp["setor_ipp"] == setor, ["date", "value"]].dropna().sort_values("date")
    if s.empty:
        return None, None
    return s.iloc[-1]["date"], float(s.iloc[-1]["value"])

def load_ipca_grupos_long(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()

    # esperado: date, grupo, indicador, value
    if "date" not in df.columns:
        df = df.reset_index()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["date", "grupo", "indicador", "value"]).sort_values(["date", "grupo"])
    return df

def ipca_contribuicoes(df_ipca_grupos: pd.DataFrame) -> pd.DataFrame:
    # separa var mensal e peso mensal
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

    # merge por date+grupo
    out = var_m.merge(peso_m, on=["date", "grupo"], how="inner")

    # contribuição em pontos percentuais (p.p.)
    out["contrib_pp"] = out["variacao_mensal"] * out["peso_mensal"] / 100.0

    return out

# -----------------------
# Página
# -----------------------
st.header("Inflação ao consumidor (IPCA)")

if not SGS_PATH.exists():
    st.error(f"Arquivo não encontrado: {SGS_PATH}")
    st.stop()

sgs = load_monthly_parquet_flexible(SGS_PATH)

needed = ["ipca", "ipca_12m"]
missing = [c for c in needed if c not in sgs.columns]
if missing:
    st.warning(f"Colunas ausentes em sgs_dados.parquet: {missing}")
else:
    c1, c2, c3 = st.columns(3)
    with c1:
        metric_last(sgs, "IPCA (mês)", "ipca", fmt="{:.2f}%")
    with c2:
        metric_last(sgs, "IPCA (12m)", "ipca_12m", fmt="{:.2f}%")
    with c3:
        d_last, _ = last_value(sgs, "ipca_12m")
        st.metric("Última referência", d_last.strftime("%Y-%m") if d_last is not None else "n/d")

    series_map = {"ipca": "IPCA (mês)", "ipca_12m": "IPCA (12m)"}
    options = list(series_map.values())

    colA, colB = st.columns([1, 1])
    with colA:
        select_all = st.button("Selecionar tudo", key="ipca_select_all")
    with colB:
        clear_all = st.button("Limpar seleção", key="ipca_clear_all")

    default_sel = ["IPCA (12m)"]
    if select_all:
        selected_labels = options
    elif clear_all:
        selected_labels = []
    else:
        selected_labels = st.multiselect(
            "Selecionar séries",
            options,
            default=default_sel,
            key="ipca_series",
        )

    inv = {v: k for k, v in series_map.items()}
    selected_cols = [inv[l] for l in selected_labels if l in inv]

    if not selected_cols:
        st.warning("Nenhuma série selecionada.")
    else:
        plot_long = wide_to_long(sgs, selected_cols, series_map)
        fig = px.line(plot_long, x="date", y="value", color="serie", title="IPCA — séries selecionadas")
        fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)", legend_title_text="Série")
        st.plotly_chart(fig, width="stretch")

    with st.expander("Dados recentes (IPCA)", expanded=False):
        st.dataframe(sgs[["date", "ipca", "ipca_12m"]].dropna().tail(12), width="stretch")

st.divider()


# =========================
# COMPOSIÇÃO DO IPCA MENSAL
# =========================
st.subheader("Composição do IPCA mensal (contribuições por grupo)")

if not IPCA_GRUPOS_PATH.exists():
    st.info(f"Arquivo não encontrado: {IPCA_GRUPOS_PATH}")
    st.info("Gere o ipca_grupos.parquet no pipeline para habilitar esta visualização.")
else:
    # 1) carrega e calcula contribuições
    ipca_g = load_ipca_grupos_long(IPCA_GRUPOS_PATH)
    contrib = ipca_contribuicoes(ipca_g)

    # ref mensal (robusto p/ merge e filtros)
    contrib["ref"] = contrib["date"].dt.to_period("M").astype(str)

    # 2) filtros de período (UI)
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

    # 3) remove “índice geral/cheio” do detalhamento (não entra no stack)
    mask_geral = contrib_f["grupo"].str.contains(
        r"índice geral|geral|índice\s+cheio",
        case=False,
        na=False,
    )
    contrib_f = contrib_f[~mask_geral].copy()

    # 4) seleção de grupos (UI) — AGORA sim contrib_f existe
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
        st.warning("Nenhum grupo selecionado. Selecione ao menos um para exibir o gráfico.")
        st.stop()

    # 5) aplica seleção ao dataset
    if agrupar_outros:
        contrib_f["grupo_plot"] = contrib_f["grupo"].where(contrib_f["grupo"].isin(grupos_sel), "Outros")
    else:
        contrib_f = contrib_f[contrib_f["grupo"].isin(grupos_sel)].copy()
        contrib_f["grupo_plot"] = contrib_f["grupo"]

    # 6) agrega para stack por mês
    plot_stack = (
        contrib_f.groupby(["ref", "grupo_plot"], as_index=False)["contrib_pp"]
        .sum()
        .sort_values("ref")
    )
    plot_stack["date"] = pd.to_datetime(plot_stack["ref"] + "-01") + pd.offsets.MonthEnd(0)

    # total do somatório (linha fallback)
    total_pp = (
        plot_stack.groupby("ref", as_index=False)["contrib_pp"]
        .sum()
        .rename(columns={"contrib_pp": "ipca_calc"})
        .sort_values("ref")
    )
    total_pp["date"] = pd.to_datetime(total_pp["ref"] + "-01") + pd.offsets.MonthEnd(0)

    # 7) índice geral: usa SGS se existir, senão usa ipca_calc
    line_df = total_pp[["date", "ref", "ipca_calc"]].copy()
    line_df["indice_geral"] = line_df["ipca_calc"]  # default

    if "ipca" in sgs.columns:
        tmp = sgs[["date", "ipca"]].dropna().copy()
        tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
        tmp["ref"] = tmp["date"].dt.to_period("M").astype(str)
        ipca_headline = tmp.groupby("ref", as_index=False)["ipca"].last()
        line_df = line_df.merge(ipca_headline, on="ref", how="left")
        # se houver SGS, prioriza
        line_df["indice_geral"] = line_df["ipca"].combine_first(line_df["ipca_calc"])

    # 8) métricas enxutas + highlights (última ref)
    last_ref = total_pp["ref"].iloc[-1]
    last_calc = float(total_pp.loc[total_pp["ref"] == last_ref, "ipca_calc"].iloc[0])

    m1, m2 = st.columns([1, 1])
    with m1:    
        st.metric("Última referência", last_ref)
    with m2:
        st.metric("Somatório das contribuições (p.p.)", f"{last_calc:.2f}%")

    # highlights do último mês
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
            st.caption("Maior pressão altista (em relação ao mês anterior)")
            st.write(f"**{best['grupo_plot'].iloc[0]}**: {float(best['contrib_pp'].iloc[0]):+.2f} p.p.")
        with h2:
            st.caption("Maior alívio (pressão baixista em relação ao mês anterior)")
            st.write(f"**{worst['grupo_plot'].iloc[0]}**: {float(worst['contrib_pp'].iloc[0]):+.2f} p.p.")

    # 9) gráfico combinado
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

    # rótulo do último ponto da linha
    last_line = line_df.dropna(subset=["indice_geral"]).sort_values("date").tail(1)
    if not last_line.empty:
        lx = last_line["date"].iloc[0]
        ly = float(last_line["indice_geral"].iloc[0])
        fig_combo.add_annotation(
            x=lx,
            y=ly,
            xref="x",
            yref="y2",
            text=f"{ly:.2f}%",
            showarrow=True,
            arrowhead=2,
            ax=20,
            ay=-20,
        )

    st.plotly_chart(fig_combo, width="stretch", key="ipca_combo")

    # 10) tabela de pesos — última referência
    weights_last = (
        contrib_f[contrib_f["ref"] == last_ref]
        .groupby("grupo", as_index=False)["peso_mensal"]
        .mean()
        .sort_values("peso_mensal", ascending=False)
    )

    st.caption(f"Pesos do IPCA por grupo — referência {last_ref}")
    st.dataframe(
        weights_last.rename(columns={"grupo": "Grupo", "peso_mensal": "Peso (%)"}),
        width="stretch",
        hide_index=True,
    )

    with st.expander("Dados (contribuições)", expanded=False):
        st.dataframe(plot_stack.sort_values(["date", "grupo_plot"]).tail(24), width="stretch")


st.divider()

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

    # gráfico: ou só o setor escolhido, ou múltiplos
    modo = st.radio("Visualização", ["Setor selecionado", "Comparar setores"], horizontal=True)

    if modo == "Setor selecionado":
        plot_df = ipp[ipp["setor_ipp"] == setor_sel].sort_values("date")
        fig = px.line(plot_df, x="date", y="value", title=f"IPP — {setor_sel}")
        fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)")
    else:
        # comparação: multiselect de setores
        default_comp = setores[:3]
        comp_sel = st.multiselect("Selecionar setores para comparar", setores, default=default_comp, key="ipp_comp")
        plot_df = ipp[ipp["setor_ipp"].isin(comp_sel)].sort_values("date")
        fig = px.line(plot_df, x="date", y="value", color="setor_ipp", title="IPP — comparação entre setores")
        fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)", legend_title_text="Setor")

    st.plotly_chart(fig, width="stretch")

    with st.expander("Dados recentes (IPP)", expanded=False):
        st.dataframe(ipp.sort_values("date").tail(24), width="stretch")
