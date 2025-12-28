from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# -----------------------
# Configuração da página
# -----------------------
st.set_page_config(page_title="Emprego e dados socioeconômicos", layout="wide")
st.title("Emprego e dados socioeconômicos")


# -----------------------
# Caminhos
# -----------------------
BASE_DIR = Path(__file__).resolve().parents[1]  # dashboard/ -> raiz do projeto
DATA_DIR = BASE_DIR / "data" / "processed"
SOCIO_PATH = DATA_DIR / "socioeconomico_quarterly.parquet"


# -----------------------
# Utilitários
# -----------------------
@st.cache_data(show_spinner=False)
def load_quarterly_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()

    # garantir datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    elif "Date" in df.columns:
        df["date"] = pd.to_datetime(df["Date"], errors="coerce")
    else:
        df = df.reset_index()
        first = df.columns[0]
        df["date"] = pd.to_datetime(df[first], errors="coerce")

    # converter numéricos com try/except (sem errors="ignore")
    for c in df.columns:
        if c == "date":
            continue
        try:
            df[c] = pd.to_numeric(df[c])
        except (ValueError, TypeError):
            pass

    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def format_br_number(x, decimals=0):
    if x is None or pd.isna(x):
        return "n/d"
    s = f"{x:,.{decimals}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def quarter_label(d: pd.Timestamp) -> str:
    if pd.isna(d):
        return ""
    q = ((d.month - 1) // 3) + 1
    return f"{d.year}T{q}"


def last_value(df: pd.DataFrame, col: str):
    s = df[["date", col]].dropna().sort_values("date")
    if s.empty:
        return None, None
    return s.iloc[-1]["date"], float(s.iloc[-1][col])


def add_quarter_col(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["trimestre"] = out["date"].dt.to_period("Q").astype(str)
    return out


def wide_to_long(df: pd.DataFrame, cols: list[str], name_map: dict[str, str]) -> pd.DataFrame:
    out = df[["date", "trimestre"] + cols].copy()
    out = out.melt(id_vars=["date", "trimestre"], var_name="serie", value_name="value")
    out["serie"] = out["serie"].map(name_map).fillna(out["serie"])
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["date", "value"]).sort_values("date")
    return out


def series_selector(title: str, options: list[str], default: list[str], key_prefix: str) -> list[str]:
    col1, col2 = st.columns([1, 1])
    with col1:
        select_all = st.button("Selecionar tudo", key=f"{key_prefix}_all")
    with col2:
        clear_all = st.button("Limpar seleção", key=f"{key_prefix}_clear")

    if select_all:
        selected = options
    elif clear_all:
        selected = []
    else:
        selected = st.multiselect(title, options, default=default, key=f"{key_prefix}_ms")

    return selected


# -----------------------
# Carregamento
# -----------------------
if not SOCIO_PATH.exists():
    st.error(f"Arquivo não encontrado: {SOCIO_PATH}")
    st.stop()

df = load_quarterly_parquet(SOCIO_PATH)
df = add_quarter_col(df)

# colunas esperadas (ajusta aqui se necessário)
cols_expected = [
    "taxa_desemprego",
    "taxa_ocupacao",
    "renda_media",
    "informalidade",
    "desalentadas",
]
cols_available = [c for c in cols_expected if c in df.columns]

if not cols_available:
    st.warning("Nenhuma das colunas esperadas foi encontrada no parquet.")
    st.write("Colunas disponíveis:", list(df.columns))
    st.stop()

name_map = {
    "taxa_desemprego": "Desemprego (%)",
    "taxa_ocupacao": "Ocupação (%)",
    "renda_media": "Renda média (R$)",
    "informalidade": "Informalidade (%)",
    "desalentadas": "Desalentadas (%)",
}

# -----------------------
# 1) Métricas (última observação)
# -----------------------
st.header("Resumo (último trimestre)")

metric_cols = st.columns(len(cols_available) + 1)

# última referência
last_ref = df["date"].max()
metric_cols[0].metric("Última referência", quarter_label(last_ref))

for i, col in enumerate(cols_available, start=1):
    d_last, v_last = last_value(df, col)

    if col == "renda_media":
        metric_cols[i].metric(name_map[col], f"R$ {format_br_number(v_last, 0)}")
    else:
        metric_cols[i].metric(name_map[col], f"{format_br_number(v_last, 1)}%")


st.divider()

# -----------------------
# 2) Gráfico principal (linhas)
# -----------------------
st.header("Evolução trimestral")

options = [name_map[c] for c in cols_available]

# defaults “narrativos”
default_labels = [lbl for lbl in ["Desemprego (%)", "Ocupação (%)", "Renda média (R$)"] if lbl in options]
if not default_labels:
    default_labels = options[:3]

selected_labels = series_selector("Selecionar séries", options, default_labels, key_prefix="socio_lines")

inv_map = {v: k for k, v in name_map.items()}
selected_cols = [inv_map[lbl] for lbl in selected_labels if lbl in inv_map]

if selected_cols:
    long_df = wide_to_long(df, selected_cols, name_map)

    # como unidades diferem (R$ vs %), oferecemos 2 modos:
    mode = st.radio(
        "Modo de visualização",
        ["Linhas (todas juntas)", "Linhas por variável (facets)"],
        horizontal=True,
    )

    if mode == "Linhas (todas juntas)":
        fig = px.line(long_df, x="date", y="value", color="serie", title="Séries selecionadas")
        fig.update_layout(xaxis_title="Trimestre", yaxis_title="Valor", legend_title_text="Série")
        st.plotly_chart(fig, width="stretch")
    else:
        fig = px.line(long_df, x="date", y="value", facet_row="serie", title="Séries selecionadas (painéis)")
        fig.update_layout(xaxis_title="Trimestre", yaxis_title="Valor")
        st.plotly_chart(fig, width="stretch")
else:
    st.warning("Selecione ao menos uma série para exibir o gráfico.")


st.divider()

# -----------------------
# 3) Gráfico secundário (barras)
# -----------------------
st.header("Destaque em barras")

bar_options = options
default_bar = "Renda média (R$)" if "Renda média (R$)" in bar_options else bar_options[0]
bar_label = st.selectbox("Selecionar série para barras", bar_options, index=bar_options.index(default_bar))

bar_col = inv_map[bar_label]
bar_df = df[["date", "trimestre", bar_col]].dropna().rename(columns={bar_col: "value"})
bar_df["value"] = pd.to_numeric(bar_df["value"], errors="coerce")
bar_df = bar_df.dropna(subset=["value"])

fig_bar = px.bar(bar_df, x="trimestre", y="value", title=f"{bar_label} — barras (trimestral)")
fig_bar.update_layout(xaxis_title="Trimestre", yaxis_title="Valor")

# melhora legibilidade do eixo x
fig_bar.update_xaxes(type="category")

st.plotly_chart(fig_bar, width="stretch")


st.divider()

# -----------------------
# 4) Tabela recente
# -----------------------
st.header("Dados recentes")

view_labels = st.multiselect(
    "Selecionar variáveis para a tabela",
    options,
    default=selected_labels if selected_labels else options[:3],
    key="table_vars",
)

view_cols = ["date", "trimestre"] + [inv_map[l] for l in view_labels if l in inv_map]
view = df[view_cols].dropna().sort_values("date").tail(16).copy()

st.dataframe(view, width="stretch")
