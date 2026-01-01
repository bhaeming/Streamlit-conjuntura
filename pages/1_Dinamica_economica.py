from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Dinâmica econômica", layout="wide")
st.title("Dinâmica econômica")


# -----------------------
# Caminhos para o data set
# -----------------------
BASE_DIR = Path(__file__).resolve().parents[1]  # # dados/ processed - acessa os dados tratados
DATA_DIR = BASE_DIR / "data" / "processed"

PIB_PATH = DATA_DIR / "pibs_quarterly.parquet"
IBC_PATH = DATA_DIR / "sgs_dados.parquet"
PPP_PATH = DATA_DIR / "indust_comer_serv.parquet"

# -----------------------
# Loaders- funções utilizadas (building features) para carregar dados e tratar os dados
# -----------------------
@st.cache_data(show_spinner=False)
def load_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


@st.cache_data(show_spinner=False)
def load_sgs_monthly(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()

    # Se Date está no índice, traz para coluna
    #if "Date" not in df.columns:
     #   df = df.reset_index()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["date"] = df["date"]

    return df

@st.cache_data(show_spinner=False)
def load_indus_comer_serv(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
 # Se a coluna date não existir, traze do índice e, após isso, converte para datetime.
    if "date" not in df.columns:
        df = df.reset_index()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # garante ordem e remove datas inválidas
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    return df

# -----------------------
# Transformações
# -----------------------
def add_quarter_label(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["trimestre"] = out["date"].dt.to_period("Q").astype(str)
    return out


# -----------------------
# Gráficos
# -----------------------
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


# -----------------------
# Métricas em Destaque
# -----------------------
import pandas as pd

def compute_ibc_metrics(df: pd.DataFrame, col: str = "ibc_br") -> dict:
    # Série temporal limpa e ordenada
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
        prev_date = s.index[-2]
        prev_value = s.iloc[-2]
        if prev_value != 0:
            mom = ((last_value / prev_value) - 1) * 100

    # 12m acumulado (%): soma dos últimos 12 / soma dos 12 imediatamente anteriores
    acc12 = None
    if len(s) >= 24:
        last_12 = s.iloc[-12:].sum()
        prev_12 = s.iloc[-24:-12].sum()
        if prev_12 != 0:
            acc12 = ((last_12 / prev_12) - 1) * 100

    # YTD (%): soma jan..m_ref do ano atual / soma jan..m_ref do ano anterior
    ytd = None
    year = last_date.year
    month_ref = last_date.month

    cur_period = s[(s.index.year == year) & (s.index.month <= month_ref)]
    prev_period = s[(s.index.year == (year - 1)) & (s.index.month <= month_ref)]

    if (not cur_period.empty) and (not prev_period.empty):
        cur_sum = cur_period.sum()
        prev_sum = prev_period.sum()
        if prev_sum != 0:
            ytd = ((cur_sum / prev_sum) - 1) * 100

    return {
        "mom": mom,            # variação mensal (%)
        "acc12": acc12,        # 12m acumulado (%), pelo seu critério de somas
        "ytd": ytd,            # acumulado no ano (%)
        "last_date": last_date,
        "last_value": last_value,
    }


def last_value(df: pd.DataFrame, col: str):
    s = df[["date", col]].dropna().sort_values("date")
    if s.empty:
        return None, None
    return s.iloc[-1]["date"], float(s.iloc[-1][col])



def render_last_value_metrics(df: pd.DataFrame, cols: list[tuple[str, str]]):
    # cols = [(col_name, label), ...]
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


# -----------------------
# App
# -----------------------
def main() -> None:
    st.header("Atividade econômica")

    # =====================
    # PIB (trimestral)
    # =====================
    st.subheader("Produto Interno Bruto (trimestral)")

    if not PIB_PATH.exists():
        st.error(f"Arquivo não encontrado: {PIB_PATH}")
        st.stop()

    pib = load_parquet(PIB_PATH)
    pib = add_quarter_label(pib)

    possible_dim_cols = [c for c in ["setor", "grupo"] if c in pib.columns]
    dim_col = possible_dim_cols[0] if possible_dim_cols else None

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
        default_sel = [s for s in default_candidates if s in options] or options[:4]

        col1, col2 = st.columns([1, 1])
        with col1:
            select_all = st.button("Selecionar tudo", key="pib_select_all")
        with col2:
            clear_all = st.button("Limpar seleção", key="pib_clear_all")

        if select_all:
            selected = options
        elif clear_all:
            selected = []
        else:
            selected = st.multiselect("Selecionar séries", options, default=default_sel, key="pib_series")
    else:
        selected = None

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

    plot_df = pib.copy()
    if dim_col is not None:
        plot_df = plot_df[plot_df[dim_col].isin(selected)]

    if plot_df.empty:
        st.warning("Nenhuma série selecionada. Selecione ao menos uma série para exibir o gráfico do PIB.")
    else:
        fig_pib = build_pib_bar_figure(plot_df, dim_col=dim_col)
        st.plotly_chart(fig_pib, width="stretch")

    st.divider()

    # =====================
    # IBC (mensal)
    # =====================
    st.subheader("Índice de Atividade Econômica (IBC-Br)")

    if not IBC_PATH.exists():
        st.info(f"Arquivo mensal não encontrado: {IBC_PATH}")
        return

    sgs_m = load_sgs_monthly(IBC_PATH)

    if "ibc_br" not in sgs_m.columns:
        st.warning("A coluna 'ibc_br' não foi encontrada em sgs_mensal.parquet.")
        return

    m = compute_ibc_metrics(sgs_m, col="ibc_br")

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        c1.metric(
            "Variação mensal (m/m)",
            f"{m['mom']:.2f}%" if m["mom"] is not None else "n/d",
        )

    with c2:
        c2.metric(
            "12 meses (soma / soma)",
            f"{m['acc12']:.2f}%" if m["acc12"] is not None else "n/d",
        )

    with c3:
        c3.metric(
            "Acumulado no ano (YTD)",
            f"{m['ytd']:.2f}%" if m["ytd"] is not None else "n/d",
        )

    with c4:
        last_ref = m["last_date"].strftime("%Y-%m") if m.get("last_date") is not None else "n/d"
        c4.metric("Última referência", last_ref)

    with st.expander("Dados mais recentes do IBC-Br", expanded=False):
        st.dataframe(
            sgs_m[["date", "ibc_br"]].dropna().sort_values("date").tail(24),
            width="stretch",
        )

    fig_ibc = build_line_figure(
        sgs_m,
        col="ibc_br",
        title="IBC-Br — índice (sem ajuste sazonal)",
        y_label="Índice",
    )
    st.plotly_chart(fig_ibc, width="stretch")


    # =====================
    # Indústria, comércio e serviços
    # =====================

    st.divider()
    st.header("Indústria, comércio e serviços")

    if not PPP_PATH.exists():
        st.info(f"Arquivo não encontrado: {PPP_PATH}")
    else:
        ppp = load_indus_comer_serv(PPP_PATH)

        # --- métricas: último valor observado ---
        st.subheader("Indicadores (% 12 meses)")
        render_last_value_metrics(
            ppp,
            cols=[
                ("pim_12m", "PIM (12m)"),
                ("pmc_12m", "PMC (12m)"),
                ("pms_12m", "PMS (12m)"),
            ],
        )

        # --- seleção de séries (igual PIB) ---
        series_map = {
            "pim_12m": "PIM (12m)",
            "pmc_12m": "PMC (12m)",
            "pms_12m": "PMS (12m)",
        }
        series_cols = list(series_map.keys())
        options = [series_map[c] for c in series_cols]

        default_sel = ["Produção Industrial mensal (% PIM 12 meses)", " Pesquisa Mensal de Comércio (% PMC 12 meses)", "Pesquisa Mensal de Serviços (% PMS 12 meses)"]  # já começa com as 3

        col1, col2 = st.columns([1, 1])
        with col1:
            select_all = st.button("Selecionar tudo", key="ppp_select_all")
        with col2:
            clear_all = st.button("Limpar seleção", key="ppp_clear_all")

        if select_all:
            selected_labels = options
        elif clear_all:
            selected_labels = []
        else:
            selected_labels = st.multiselect(
                "Selecionar séries",
                options,
                default=default_sel,
                key="ppp_series",
            )

        # mapeia labels escolhidos de volta para colunas
        inv_map = {v: k for k, v in series_map.items()}
        selected_cols = [inv_map[l] for l in selected_labels if l in inv_map]

        if len(selected_cols) == 0:
            st.warning("Nenhuma série selecionada. Selecione ao menos uma série para exibir o gráfico.")
        else:
            # --- gráfico único (long) ---
            plot_long = wide_to_long(
                df=ppp,
                date_col="date",
                value_cols=selected_cols,
                name_map=series_map,
            )

            fig_ppp = px.line(
                plot_long,
                x="date",
                y="value",
                color="serie",
                title="Produção, comércio e serviços — variação em 12 meses (%)",
            )
            fig_ppp.update_layout(
                xaxis_title="Data",
                yaxis_title="Variação (%)",
                legend_title_text="Série",
            )

            st.plotly_chart(fig_ppp, width="stretch")

        with st.expander("Dados mais recentes (PIM/PMC/PMS — 12m)", expanded=False):
            st.dataframe(
                ppp[["date", "pim_12m", "pmc_12m", "pms_12m"]].dropna().tail(12),
                width="stretch",
            )

# Execução
main()



