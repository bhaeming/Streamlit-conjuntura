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


def build_pib_figure(pib: pd.DataFrame) -> px.line:
    """
    Espera um dataframe em formato long, com colunas:
      - date (datetime)
      - value (numérico)
      - setor ou grupo (opcional; se existir, vira a dimensão do gráfico)
    """
    possible_dim_cols = [c for c in ["setor", "grupo"] if c in pib.columns]
    dim_col = possible_dim_cols[0] if possible_dim_cols else None

    if dim_col is None:
        fig = px.line(
            pib.sort_values("date"),
            x="date",
            y="value",
            title="PIB"
        )
        return fig

    options = sorted(pib[dim_col].dropna().unique().tolist())

    default_candidates = [
        "PIB a preços de mercado",
        "Serviços - total",
        "Indústria - total",
        "Agropecuária - total",
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

    plot_df = pib[pib[dim_col].isin(selected)].sort_values("date")

    fig = px.line(
        plot_df,
        x="data",
        y="valor",
        color=dim_col,
        title="PIB — em relação ao mesmo trimestre do ano anterior (%)"
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

    with st.expander("Ver amostra dos dados (PIB)", expanded=False):
        st.write("Colunas:", list(pib.columns))
        st.dataframe(pib.tail(20), width="stretch")

    fig = build_pib_figure(pib)
    st.plotly_chart(fig, width="stretch")


# Execução (sem indentação)
main()



