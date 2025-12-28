from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# -----------------------
# Configuração da página
# -----------------------
st.set_page_config(page_title="Preços ao consumidor e ao produtor", layout="wide")
st.title("Preços ao consumidor e ao produtor")


# -----------------------
# Caminhos
# -----------------------

BASE_DIR = Path(__file__).resolve().parents[1]  # dashboard/ -> raiz do projeto
DATA_DIR = BASE_DIR / "data" / "processed"

SGS_PATH = DATA_DIR / "sgs_dados.parquet"
IPP_PATH = DATA_DIR / "ipp_m.parquet"


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
st.header("Preços ao produtor (IPP)")

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
        st.dataframe(ipp.sort_values("date").tail(36), width="stretch")
