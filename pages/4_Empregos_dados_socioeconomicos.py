from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import plotly.graph_objects as go
import json



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
DESEMP_UF_PATH = DATA_DIR / "desemp_uf.parquet"
GEOJSON_UF_PATH = BASE_DIR / "assets" / "geo" / "BR_UF_2023.geojson"



# -----------------------
# Funções Utilitárias
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

def load_desemp_uf_long(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["taxa_desemprego"] = pd.to_numeric(df["taxa_desemprego"], errors="coerce")
    df["uf"] = df["uf"].astype(str).str.strip()
    df = df.dropna(subset=["date", "uf", "taxa_desemprego"]).sort_values(["uf", "date"]).reset_index(drop=True)
    df["trimestre"] = df["date"].dt.to_period("Q").astype(str)  # ex: 2025Q3
    return df

def load_desemp_uf(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["taxa_desemprego"] = pd.to_numeric(df["taxa_desemprego"], errors="coerce")
    df["uf"] = df["uf"].astype(str).str.strip()
    df = df.dropna(subset=["date", "uf", "taxa_desemprego"]).sort_values(["date", "uf"]).reset_index(drop=True)
    df["trimestre"] = df["date"].dt.to_period("Q").astype(str)
    return df

def load_geojson(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# -----------------------
# Mapa de siglas
# -----------------------
UF_NOME_TO_SIGLA = {
    "Acre": "AC", "Alagoas": "AL", "Amapá": "AP", "Amazonas": "AM", "Bahia": "BA",
    "Ceará": "CE", "Distrito Federal": "DF", "Espírito Santo": "ES", "Goiás": "GO",
    "Maranhão": "MA", "Mato Grosso": "MT", "Mato Grosso do Sul": "MS", "Minas Gerais": "MG",
    "Pará": "PA", "Paraíba": "PB", "Paraná": "PR", "Pernambuco": "PE", "Piauí": "PI",
    "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN", "Rio Grande do Sul": "RS",
    "Rondônia": "RO", "Roraima": "RR", "Santa Catarina": "SC", "São Paulo": "SP",
    "Sergipe": "SE", "Tocantins": "TO",
}

def normalize_uf_to_sigla(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    # se já vier sigla (2 letras), mantém
    is_sigla = s.str.len().eq(2)
    out = s.where(is_sigla, s.map(UF_NOME_TO_SIGLA))
    return out

# -----------------------
# Carregamento (loaders)
# -----------------------
if not SOCIO_PATH.exists():
    st.error(f"Arquivo não encontrado: {SOCIO_PATH}")
    st.stop()

df = load_quarterly_parquet(SOCIO_PATH)
df = add_quarter_col(df)

# Tratamento das colunas
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

def load_desemp_uf(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["trimestre"] = df["date"].dt.to_period("Q").astype(str)
    df["taxa_desemprego"] = pd.to_numeric(df["taxa_desemprego"], errors="coerce")
    return df



# -----------------------
# 1) Métricas de Destaque
# -----------------------
st.header("Resumo dos Dados Sócioeconômicos - Brasil")

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

PCT_LABELS = {"Desemprego (%)", "Ocupação (%)", "Informalidade (%)"}
BRL_LABELS = {"Renda média (R$)"}

options = [name_map[c] for c in cols_available]
default_labels = [lbl for lbl in ["Desemprego (%)", "Ocupação (%)"] if lbl in options]
if not default_labels:
    default_labels = options[:3]

selected_labels = series_selector("Selecionar séries", options, default_labels, key_prefix="socio_lines")

inv_map = {v: k for k, v in name_map.items()}
selected_cols = [inv_map[lbl] for lbl in selected_labels if lbl in inv_map]

if selected_cols:
    long_df = wide_to_long(df, selected_cols, name_map)

    mode = st.radio(
        "Modo de visualização",
        ["Linhas (todas juntas)", "Linhas com eixo secundário (por unidade)"],
        horizontal=True,
        key="socio_mode",
    )

    if mode == "Linhas (todas juntas)":
        fig = px.line(long_df, x="date", y="value", color="serie", title="Séries selecionadas")
        fig.update_layout(xaxis_title="Trimestre", yaxis_title="Valor", legend_title_text="Série")
        st.plotly_chart(fig, use_container_width=True, key="socio_line_all")

    else:
        df_pct = long_df[long_df["serie"].isin(PCT_LABELS)].copy()
        df_brl = long_df[long_df["serie"].isin(BRL_LABELS)].copy()

        fig = go.Figure()

        for s_name, g in df_pct.groupby("serie"):
            fig.add_trace(go.Scatter(x=g["date"], y=g["value"], mode="lines", name=s_name, yaxis="y"))

        for s_name, g in df_brl.groupby("serie"):
            fig.add_trace(go.Scatter(x=g["date"], y=g["value"], mode="lines", name=s_name, yaxis="y2"))

        fig.update_layout(
            title="Séries selecionadas — eixo secundário por unidade",
            xaxis=dict(title="Trimestre"),
            yaxis=dict(title="Percentual (%)"),
            yaxis2=dict(title="R$ (nível)", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="top", y=-0.25, xanchor="left", x=0),
            margin=dict(b=100),
        )

        st.plotly_chart(fig, use_container_width=True, key="socio_line_dual")
else:
    st.warning("Selecione ao menos uma série para exibir o gráfico.")

st.divider()

# -----------------------
# 2) Mapa Desemprego por UF
# -----------------------
st.divider()
st.header("Desemprego por UF")

if not DESEMP_UF_PATH.exists():
    st.info(f"Arquivo não encontrado: {DESEMP_UF_PATH}")
elif not GEOJSON_UF_PATH.exists():
    st.info(f"GeoJSON não encontrado: {GEOJSON_UF_PATH}")
else:
    desemp_uf = load_desemp_uf(DESEMP_UF_PATH)
    geojson_uf = load_geojson(GEOJSON_UF_PATH)

    # 1) escolher trimestre (default: último)
    trimestres = sorted(desemp_uf["trimestre"].dropna().unique().tolist())
    tri_sel = st.selectbox(
        "Selecionar trimestre",
        trimestres,
        index=len(trimestres) - 1,
        key="uf_tri_sel",
    )

    df_tri = desemp_uf[desemp_uf["trimestre"] == tri_sel].copy()
    if df_tri.empty:
        st.warning("Sem dados para o trimestre selecionado.")
        st.stop()

    # 2) padroniza UF para SIGLA 
    df_tri["uf_sigla"] = normalize_uf_to_sigla(df_tri["uf"])

    # 3) Descobrir automaticamente o campo do GeoJSON
    #    (prioriza campos de SIGLA; se não achar, cai para nome)

    props0 = geojson_uf["features"][0]["properties"]
    sample_keys = list(props0.keys())

    # ordem de preferência
    candidate_sigla = ["SIGLA_UF", "SIGLA", "SG_UF", "UF", "uf", "sigla"]
    candidate_nome = ["NM_UF", "NOME_UF", "NOME", "NAME"]

    geo_key = next((k for k in candidate_sigla if k in sample_keys), None)
    use_sigla = True

    if geo_key is None:
        geo_key = next((k for k in candidate_nome if k in sample_keys), None)
        use_sigla = False

    if geo_key is None:
        st.error(f"Não encontrei no GeoJSON um campo de UF (sigla/nome). Campos disponíveis: {sample_keys}")
        st.stop()

    featureidkey = f"properties.{geo_key}"

    # 4) Define qual coluna do df vai “casar” com o geojson
    if use_sigla:
        loc_col = "uf_sigla"
        df_tri = df_tri.dropna(subset=[loc_col, "taxa_desemprego"]).copy()
    else:
        # se o geojson não tem sigla, tenta casar por nome (NM_UF etc.)
        loc_col = "uf"
        df_tri = df_tri.dropna(subset=[loc_col, "taxa_desemprego"]).copy()

    if df_tri.empty:
        st.error("Após limpeza, não restaram linhas (checagem de siglas/nomes).")
        st.stop()

    # 5) Métricas (max/min) no trimestre selecionado
    top = df_tri.loc[df_tri["taxa_desemprego"].idxmax()]
    bot = df_tri.loc[df_tri["taxa_desemprego"].idxmin()]

    brasil_val = None
    if "taxa_desemprego" in df.columns:
        tmp_br = df.copy()
        tmp_br["trimestre"] = tmp_br["date"].dt.to_period("Q").astype(str)
        row_br = tmp_br[tmp_br["trimestre"] == tri_sel]
        if not row_br.empty:
            brasil_val = float(row_br["taxa_desemprego"].iloc[-1])

    m1, m2, m3 = st.columns(3)
    m1.metric("Trimestre", tri_sel)
    m2.metric("Maior desemprego", f"{top[loc_col]} — {top['taxa_desemprego']:.1f}%")
    m3.metric("Menor desemprego", f"{bot[loc_col]} — {bot['taxa_desemprego']:.1f}%")

    if brasil_val is not None:
        st.caption(f"Brasil (PNAD): **{brasil_val:.1f}%** no trimestre {tri_sel}")

   
featureidkey = f"properties.{geo_key}"

fig_map = px.choropleth_mapbox(
    df_tri,
    geojson=geojson_uf,
    locations="uf_sigla",              
    featureidkey=featureidkey,          
    color="taxa_desemprego",
    color_continuous_scale="YlOrRd",
    range_color=(
        float(df_tri["taxa_desemprego"].min()),
        float(df_tri["taxa_desemprego"].max())
    ),
    mapbox_style="carto-darkmatter",  
    zoom=2.7,
    center={"lat": -14.2, "lon": -52.9},
    opacity=0.88,
    hover_name="uf_sigla",
    hover_data={"taxa_desemprego": ":.1f"},
    labels={"taxa_desemprego": "Desemprego (%)"},
    title=f"Taxa de desemprego por UF — {tri_sel}",
)

fig_map.update_traces(
    marker_line_width=0.6,
    marker_line_color="rgba(255,255,255,0.35)",
    hovertemplate="<b>%{hovertext}</b><br>Desemprego: %{z:.1f}%<extra></extra>",
)

fig_map.update_layout(
    height=520,
    margin=dict(l=0, r=0, t=55, b=0),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    coloraxis_colorbar=dict(
        title="Desemprego",
        ticksuffix="%",
        thickness=12,
        len=0.75,
        x=0.98,
    ),
)

st.plotly_chart(fig_map, use_container_width=True, key="map_desemp_uf")


st.divider()

st.header("Dados recentes")

view_labels = st.multiselect(
    "Selecionar variáveis para a tabela",
    options,
    default=selected_labels if selected_labels else options[:3],
    key="table_vars",
)

view_cols = ["date", "trimestre"] + [inv_map[l] for l in view_labels if l in inv_map]
view = df[view_cols].dropna().sort_values("date").tail(12).copy()

st.dataframe(view, width="stretch")
