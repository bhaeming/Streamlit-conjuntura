from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# -----------------------
# Configuração da página
# -----------------------
st.set_page_config(page_title="Juros e crédito", layout="wide")
st.title("Juros e crédito")


# -----------------------
# Caminhos
# -----------------------
BASE_DIR = Path(__file__).resolve().parents[1]  # dashboard/ -> raiz do projeto
DATA_DIR = BASE_DIR / "data" / "processed"

SGS_PATH = DATA_DIR / "sgs_dados.parquet"
SELIC_PATH = DATA_DIR / "selic_mensal.parquet"


# -----------------------
# Loaders / utilitários
# -----------------------
@st.cache_data(show_spinner=False)
def load_monthly_parquet_flexible(path: Path) -> pd.DataFrame:
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

    for c in df.columns:
        if c == "date":
            continue
        try:
            df[c] = pd.to_numeric(df[c])
        except (ValueError, TypeError):
            pass

    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def last_value(df: pd.DataFrame, col: str):
    s = df[["date", col]].dropna().sort_values("date")
    if s.empty:
        return None, None
    return s.iloc[-1]["date"], float(s.iloc[-1][col])


def metric_last(df: pd.DataFrame, label: str, col: str, fmt: str = "{:.2f}"):
    d, v = last_value(df, col)
    if v is None:
        st.metric(label, "n/d")
    else:
        st.metric(label, fmt.format(v))


def wide_to_long(df: pd.DataFrame, cols: list[str], name_map: dict[str, str]) -> pd.DataFrame:
    out = df[["date"] + cols].copy()
    out = out.melt(id_vars=["date"], var_name="serie", value_name="value")
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


def build_line_from_long(long_df: pd.DataFrame, title: str, y_label: str):
    fig = px.line(long_df, x="date", y="value", color="serie", title=title)
    fig.update_layout(xaxis_title="Data", yaxis_title=y_label, legend_title_text="Série")
    return fig


def build_area_from_long(long_df: pd.DataFrame, title: str, y_label: str):
    fig = px.area(long_df, x="date", y="value", color="serie", title=title)
    fig.update_layout(xaxis_title="Data", yaxis_title=y_label, legend_title_text="Série")
    return fig


def build_bar_from_long(long_df: pd.DataFrame, title: str, y_label: str):
    fig = px.bar(long_df, x="date", y="value", color="serie", barmode="group", title=title)
    fig.update_layout(xaxis_title="Data", yaxis_title=y_label, legend_title_text="Série")
    return fig


def filter_last_months(df: pd.DataFrame, months: int) -> pd.DataFrame:
    if df.empty:
        return df
    end = df["date"].max()
    start = end - pd.DateOffset(months=months)
    return df[df["date"] >= start].copy()

def format_br_number(x, decimals=0):
    if x is None or pd.isna(x):
        return "n/d"
    s = f"{x:,.{decimals}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")



# -----------------------
# 1) Selic (parquet separado)
# -----------------------
st.header("Política monetária (Selic)")

if not SELIC_PATH.exists():
    st.error(f"Arquivo não encontrado: {SELIC_PATH}")
else:
    selic_df = load_monthly_parquet_flexible(SELIC_PATH)

    # tenta achar automaticamente a coluna da selic
    possible_selic_cols = [c for c in ["selic", "Selic", "selic_mensal"] if c in selic_df.columns]
    if not possible_selic_cols:
        # fallback: pega a primeira coluna numérica diferente de date
        numeric_cols = [c for c in selic_df.columns if c != "date"]
        if numeric_cols:
            selic_col = numeric_cols[0]
        else:
            selic_col = None
    else:
        selic_col = possible_selic_cols[0]

    if selic_col is None:
        st.warning("Não consegui identificar a coluna da Selic no parquet.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            metric_last(selic_df, "Selic (última)", selic_col, fmt="{:.2f}%")
        with c2:
            d_last, _ = last_value(selic_df, selic_col)
            st.metric("Última referência", d_last.strftime("%Y-%m") if d_last is not None else "n/d")

        plot_selic = selic_df[["date", selic_col]].dropna().rename(columns={selic_col: "value"})
        plot_selic["serie"] = "Selic"
        plot_selic = filter_last_months(plot_selic, 120)  # último 10 anos (ajuste se quiser)

        fig_selic = build_line_from_long(plot_selic, title="Selic — taxa (% a.a.)", y_label="Taxa (% a.a.)")
        st.plotly_chart(fig_selic, width="stretch")

        with st.expander("Dados recentes (Selic)", expanded=False):
            st.dataframe(selic_df[["date", selic_col]].dropna().tail(24), width="stretch")


st.divider()

# -----------------------
# 2) Crédito / Juros / Inadimplência (SGS)
# -----------------------
st.header("Mercado de crédito")

if not SGS_PATH.exists():
    st.error(f"Arquivo não encontrado: {SGS_PATH}")
    st.stop()

sgs = load_monthly_parquet_flexible(SGS_PATH)
sgs = filter_last_months(sgs, 180)  # último 15 anos (ajuste)

# -------------------
# 2.1 Crédito (área)
# -------------------
st.subheader("Crédito (estoques)")

credit_cols = [c for c in ["credito_pf", "credito_pj", "credito_total"] if c in sgs.columns]
if not credit_cols:
    st.warning("Não encontrei colunas de crédito esperadas (credito_pf, credito_pj, credito_total).")
else:
    credit_map = {
        "credito_pf": "Crédito PF",
        "credito_pj": "Crédito PJ",
        "credito_total": "Crédito total",
    }
    options = [credit_map[c] for c in credit_cols]
    default = options

    selected_labels = series_selector("Selecionar séries (crédito)", options, default, key_prefix="cred")
    inv = {v: k for k, v in credit_map.items()}
    selected_cols = [inv[l] for l in selected_labels if l in inv]

    if selected_cols:
        cols_ui = st.columns(len(selected_cols))
        for i, col in enumerate(selected_cols):
            d_last, v_last = last_value(sgs, col)
            label = credit_map.get(col, col)
            cols_ui[i].metric(label, format_br_number(v_last, 0))

        long_credit = wide_to_long(sgs, selected_cols, credit_map)
        fig_credit = build_area_from_long(long_credit, title="Crédito — estoques (séries selecionadas)", y_label="Saldo (nível)")
        st.plotly_chart(fig_credit, width="stretch")

        with st.expander("Dados recentes (crédito)", expanded=False):
            view_cols = ["date"] + selected_cols
            st.dataframe(sgs[view_cols].dropna().tail(24), width="stretch")
    else:
        st.warning("Selecione ao menos uma série de crédito.")


st.divider()

# -------------------
# 2.2 Juros (barra)
# -------------------
st.subheader("Taxas de juros")

juros_cols = [c for c in ["taxa_juros_pf", "taxa_juros_pj", "taxa_juros_total"] if c in sgs.columns]
if not juros_cols:
    st.warning("Não encontrei colunas de taxa de juros (taxa_juros_pf, taxa_juros_pj, taxa_juros_total).")
else:
    juros_map = {
        "taxa_juros_pf": "Juros PF",
        "taxa_juros_pj": "Juros PJ",
        "taxa_juros_total": "Juros total",
    }
    options = [juros_map[c] for c in juros_cols]
    default = options

    selected_labels = series_selector("Selecionar séries (juros)", options, default, key_prefix="juros")
    inv = {v: k for k, v in juros_map.items()}
    selected_cols = [inv[l] for l in selected_labels if l in inv]

    if selected_cols:
        cols_ui = st.columns(len(selected_cols))
        for i, col in enumerate(selected_cols):
            d_last, v_last = last_value(sgs, col)
            label = juros_map.get(col, col)
            cols_ui[i].metric(label, f"{format_br_number(v_last, 2)}%")

        long_juros = wide_to_long(sgs, selected_cols, juros_map)
        fig_juros = build_bar_from_long(long_juros, title="Taxas de juros — séries selecionadas", y_label="Taxa (%)")
        st.plotly_chart(fig_juros, width="stretch")

        with st.expander("Dados recentes (juros)", expanded=False):
            view_cols = ["date"] + selected_cols
            st.dataframe(sgs[view_cols].dropna().tail(24), width="stretch")
    else:
        st.warning("Selecione ao menos uma série de juros.")


st.divider()

# -------------------
# 2.3 Inadimplência (barra)
# -------------------
st.subheader("Inadimplência")

inad_cols = [c for c in ["inadimplencia_pf", "inadimplencia_pj", "inadimplencia_total"] if c in sgs.columns]
if not inad_cols:
    st.warning("Não encontrei colunas de inadimplência (inadimplencia_pf, inadimplencia_pj, inadimplencia_total).")
else:
    inad_map = {
        "inadimplencia_pf": "Inadimplência PF",
        "inadimplencia_pj": "Inadimplência PJ",
        "inadimplencia_total": "Inadimplência total",
    }
    options = [inad_map[c] for c in inad_cols]
    default = options

    selected_labels = series_selector("Selecionar séries (inadimplência)", options, default, key_prefix="inad")
    inv = {v: k for k, v in inad_map.items()}
    selected_cols = [inv[l] for l in selected_labels if l in inv]

    if selected_cols:
        cols_ui = st.columns(len(selected_cols))
        for i, col in enumerate(selected_cols):
            d_last, v_last = last_value(sgs, col)
            label = inad_map.get(col, col)
            cols_ui[i].metric(label, f"{format_br_number(v_last, 2)}%")


        long_inad = wide_to_long(sgs, selected_cols, inad_map)
        fig_inad = build_bar_from_long(long_inad, title="Inadimplência — séries selecionadas", y_label="Taxa (%)")
        st.plotly_chart(fig_inad, width="stretch")

        with st.expander("Dados recentes (inadimplência)", expanded=False):
            view_cols = ["date"] + selected_cols
            st.dataframe(sgs[view_cols].dropna().tail(24), width="stretch")
    else:
        st.warning("Selecione ao menos uma série de inadimplência.")
