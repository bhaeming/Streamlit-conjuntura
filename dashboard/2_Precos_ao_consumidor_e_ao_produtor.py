from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# -----------------------
# Configuração da página
# -----------------------
st.set_page_config(page_title="Preços ao consumidor e ao produtor", layout="wide")
st.title("Preços ao consumidor e ao produtor")


BASE_DIR = Path(__file__).resolve().parents[2]  # pages/ -> dashboard/ -> raiz do projeto
DATA_DIR = BASE_DIR / "data" / "processed"

SGS_PATH = DATA_DIR / "sgs_dados.parquet"
IPP_PATH = DATA_DIR / "ipp_m.parquet"

# -----------------------
# Loaders
# -----------------------
@st.cache_data(show_spinner=False)
def load_monthly_indexed_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()

    # Se Date está no índice, traz para coluna
    if "Date" not in df.columns:
        df = df.reset_index()

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["date"] = df["Date"]

    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


@st.cache_data(show_spinner=False)
def load_monthly_date_parquet(path: Path) -> pd.DataFrame:
    """Para parquets que já vêm com coluna 'date'."""
    df = pd.read_parquet(path).copy()
    if "date" not in df.columns:
        df = df.reset_index()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
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
    plot_df = df[["date", col]].dropna().rename(columns={col: "value"})
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


# -----------------------
# Página
# -----------------------
st.header("Inflação ao consumidor (IPCA)")

if not SGS_PATH.exists():
    st.error(f"Arquivo não encontrado: {SGS_PATH}")
    st.stop()

sgs = load_monthly_indexed_parquet(SGS_PATH)

needed = ["ipca", "ipca_12m"]
missing = [c for c in needed if c not in sgs.columns]
if missing:
    st.warning(f"Colunas ausentes em sgs_mensal.parquet: {missing}")
else:
    c1, c2, c3 = st.columns(3)
    with c1:
        metric_last(sgs, "IPCA (mês)", "ipca", fmt="{:.2f}%")
    with c2:
        metric_last(sgs, "IPCA (12m)", "ipca_12m", fmt="{:.2f}%")
    with c3:
        # só um extra útil: último mês disponível
        d_last, _ = last_value(sgs, "ipca_12m")
        st.metric("Última referência", d_last.strftime("%Y-%m") if d_last is not None else "n/d")

    # seleção para gráfico (mês vs 12m)
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
        fig = px.line(
            plot_long,
            x="date",
            y="value",
            color="serie",
            title="IPCA — séries selecionadas",
        )
        fig.update_layout(xaxis_title="Data", yaxis_title="Variação (%)", legend_title_text="Série")
        st.plotly_chart(fig, width="stretch")

    with st.expander("Dados recentes (IPCA)", expanded=False):
        st.dataframe(
            sgs[["date", "ipca", "ipca_12m"]].dropna().tail(24),
            width="stretch",
        )


st.divider()
st.header("Preços ao produtor (IPP)")

if not IPP_PATH.exists():
    st.info(f"Arquivo ainda não encontrado: {IPP_PATH}")
    st.info("Quando você gerar o parquet do IPP, este painel passa a exibir métricas e gráfico automaticamente.")
else:
    ipp = load_monthly_date_parquet(IPP_PATH)

    # aqui você pode ter várias colunas; por enquanto, vamos supor uma coluna 'ipp_12m'
    # (se seus nomes forem diferentes, eu ajusto rapidinho)
    possible_cols = [c for c in ipp.columns if c != "date"]
    if not possible_cols:
        st.warning("O arquivo de IPP não tem colunas além de 'date'.")
    else:
        # seletor de colunas disponíveis
        col_sel = st.selectbox("Selecionar série do IPP", possible_cols, index=0)

        c1, c2 = st.columns(2)
        with c1:
            metric_last(ipp, f"IPP — última observação ({col_sel})", col_sel, fmt="{:.2f}%")
        with c2:
            d_last, _ = last_value(ipp, col_sel)
            st.metric("Última referência", d_last.strftime("%Y-%m") if d_last is not None else "n/d")

        fig = build_line(ipp, col_sel, title=f"IPP — {col_sel}", y_label="Variação (%)")
        st.plotly_chart(fig, width="stretch")

        with st.expander("Dados recentes (IPP)", expanded=False):
            st.dataframe(ipp[["date", col_sel]].dropna().tail(24), width="stretch")