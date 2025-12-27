from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# -----------------------
# Configuração da página
# -----------------------
st.set_page_config(page_title="Dinâmica econômica", layout="wide")
st.title("Dinâmica econômica")


# -----------------------
# Caminhos (robusto para rodar/deploy)
# -----------------------
BASE_DIR = Path(__file__).resolve().parents[1]  # dashboard/ -> raiz do projeto
DATA_DIR = BASE_DIR / "data" / "processed"
PIB_PATH = DATA_DIR / "pibs_quarterly.parquet"


@st.cache_data(show_spinner=False)
def load_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


def add_quarter_label(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["trimestre"] = out["date"].dt.to_period("Q").astype(str)
    return out


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


def main() -> None:
    if not PIB_PATH.exists():
        st.error(f"Arquivo não encontrado: {PIB_PATH}")
        st.info("Verifique se o parquet foi gerado em data/processed com o nome correto.")
        st.stop()

    try:
        pib = load_parquet(PIB_PATH)
    except Exception as e:
        st.error("Erro ao ler o parquet do PIB.")
        st.exception(e)
        st.info("Se o erro mencionar pyarrow/fastparquet, instale um deles (ex.: `pip install pyarrow`).")
        st.stop()

    pib = add_quarter_label(pib)

    possible_dim_cols = [c for c in ["setor", "grupo"] if c in pib.columns]
    dim_col = possible_dim_cols[0] if possible_dim_cols else None

    # -----------------------
    # Controles (fora do expander)
    # -----------------------
    if dim_col is not None:
        options = sorted(pib[dim_col].dropna().unique().tolist())

        default_candidates = [
            "PIB a preços de mercado",
            "Serviços - total",
            "Indústria - total",
            "Agropecuária - total",
            "Formação bruta de capital fixo",
            "Consumo das famílias",
        ]
        default_sel = [s for s in default_candidates if s in options]
        if not default_sel:
            default_sel = options[:4]

        col1, col2 = st.columns([1, 1])
        with col1:
            select_all = st.button("Selecionar tudo")
        with col2:
            clear_all = st.button("Limpar seleção")

        if select_all:
            selected = options
        elif clear_all:
            selected = []
        else:
            selected = st.multiselect("Selecionar séries", options, default=default_sel)
    else:
        selected = None

    # -----------------------
    # Expander (drill-down): tabela recente
    # -----------------------
    with st.expander("Dados mais recentes do PIB e seus setores", expanded=False):
        st.write("Colunas:", list(pib.columns))

        if dim_col is None:
            df_view = pib.sort_values("date").tail(12)
        else:
            df_view = (
                pib[pib[dim_col].isin(selected if selected is not None else [])]
                .sort_values("date")
                .groupby(dim_col, as_index=False)
                .tail(4)
            )

        st.dataframe(df_view, width="stretch")

    # -----------------------
    # Gráfico (sempre fora do expander)
    # -----------------------
    plot_df = pib.copy()
    if dim_col is not None:
        plot_df = plot_df[plot_df[dim_col].isin(selected)]

    if plot_df.empty:
        st.warning("Nenhuma série selecionada. Selecione ao menos uma série para exibir o gráfico.")
        st.stop()

    fig = build_pib_bar_figure(plot_df, dim_col=dim_col)
    st.plotly_chart(fig, width="stretch")


# Execução
main()

############ IBCr ############
IBC_PATH = DATA_DIR / "sgs_mensal.parquet"

