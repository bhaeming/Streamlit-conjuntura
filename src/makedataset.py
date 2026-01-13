import pandas as pd
import numpy as np
import sidrapy as sd
from bcb import sgs
import sidrapy as sidra
from sidrapy import get_table
import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf
from functools import reduce
from pathlib import Path   
from __future__ import annotations

###################################################################
### Dados SGS ###

## Selic
selic = sgs.get({'selic' : '432'},
               start = '2020-01-31')
selic
# Tratamento dos dados da selic para mensal
selic_mensal = selic.resample('ME').last().reset_index()
selic_mensal.rename(columns={"Date": "date"}, inplace=True)
selic_mensal
#Exportando os dados processados
BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
out_dir = BASE_DIR / "data" / "processed"

out_dir.mkdir(parents=True, exist_ok=True)

selic_mensal.to_parquet(out_dir / "selic_mensal.parquet", index=False)


# -----------------------------
# IPCA - consolida todos os ipcas relevantes do SGS
# -----------------------------

# Funções de tratamento e transformação #

def ensure_month_end_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que o DataFrame tenha índice Datetime no fim do mês, ordenado.
    Aceita:
      - índice datetime
      - ou coluna 'date'
    """
    out = df.copy()

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"]).set_index("date")

    # força fim do mês
    out.index = pd.to_datetime(out.index, errors="coerce") + pd.offsets.MonthEnd(0)
    out = out[~out.index.isna()].sort_index()

    return out


def acc_12m_curve_rate(s: pd.Series) -> pd.Series:
    """
    Converte uma série mensal em % m/m para inflação acumulada em 12 meses (%),
    via composição multiplicativa (rolling 12).
    Retorna série alinhada ao índice original.
    """
    s = pd.to_numeric(s, errors="coerce")
    return ((1 + s / 100).rolling(12).apply(np.prod, raw=True) - 1) * 100


def add_12m_from_monthly_rates(df: pd.DataFrame, cols: list[str], suffix_12m: str = "_12m_calc") -> pd.DataFrame:
    """
    Para cada coluna em cols (mensal % m/m), cria uma coluna 12m composta.
    """
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[f"{c}{suffix_12m}"] = acc_12m_curve_rate(out[c])
    return out


# -----------------------------
# Coleta (você já tem sgs.get)
# -----------------------------
ipca_mensal = sgs.get({"ipca": "433"}, start="2014-01-31")
ipca_12m    = sgs.get({"ipca_12m": "13522"}, start="2014-01-31")

ipca_ld_m = sgs.get(
    {"ipca_livres": "11428", "ipca_administrados": "4449"},
    start="2011-01-31",
)

# -----------------------------
# Padroniza índices (fim do mês)
# -----------------------------
ipca_mensal = ensure_month_end_index(ipca_mensal)
ipca_12m    = ensure_month_end_index(ipca_12m)
ipca_ld_m   = ensure_month_end_index(ipca_ld_m)

# -----------------------------
# Junta tudo num único DF
# (alinha por data; ficará NaN onde não existir histórico)
# -----------------------------
df_ipca_all = (
    ipca_mensal.join(ipca_12m, how="outer")
               .join(ipca_ld_m, how="outer")
               .sort_index()
)

# -----------------------------
# Cria 12m calculado para as séries mensais (opcional para ipca)
# - livres e administrados: essencial
# - ipca: opcional (útil para auditoria vs ipca_12m oficial)
# -----------------------------
df_ipca_all = add_12m_from_monthly_rates(
    df_ipca_all,
    cols=["ipca_livres", "ipca_administrados", "ipca"],  # pode tirar "ipca" se não quiser
    suffix_12m="_12m_calc"
)

# -----------------------------
# Export dos dfs
# -----------------------------
df_ipca_all = df_ipca_all.dropna(how="all")

# volta date pra coluna e exporta parquet
df_ipca_all_out = df_ipca_all.reset_index().rename(columns={"index": "date"})

df_ipca_all_out

BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
out_dir = BASE_DIR / "data" / "processed"
out_dir.mkdir(parents=True, exist_ok=True)

df_ipca_all_out.to_parquet(out_dir / "ipca_all.parquet", index=False)
# -----------------------------

ibc_br= sgs.get({'ibc_br' : '24363'},
               start = '2014-03-31')
ibc_br

saldo_cred = sgs.get(
    {
        'credito_pf': '20570',
        'credito_pj': '20543',
        'credito_total': '20542'
    },
    start='2014-01-31'
)
saldo_cred

## inadimplencia anual
inadimplencia =  sgs.get(
    {
        'inadimplencia_total' : '21085',
        'inadimplencia_pj' : '21086',
        'inadimplencia_pf' : '21112',
    },
    start='2014-01-31'
)
inadimplencia


## taxa de juros anual
taxa_de_juros = sgs.get(
    {
        'taxa_juros_pf' : '20748',
        'taxa_juros_pj' : '20718',
        'taxa_juros_total' : '20717',
    },
    start='2014-01-31'
)
taxa_de_juros


df_sgs = [
    ibc_br,
    saldo_cred,
    inadimplencia,
    taxa_de_juros]

df_sgs

sgs_wide = reduce(
    lambda left, right: pd.merge(left, right, on="Date", how="outer"),
    df_sgs
)

sgs_wide=sgs_wide.reset_index()
sgs_wide

sgs_wide = sgs_wide.rename(columns={"Date": "date"})
sgs_wide.dropna(inplace=True)
sgs_wide.head()
sgs_wide.tail()

BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
out_dir = BASE_DIR / "data" / "processed"

out_dir.mkdir(parents=True, exist_ok=True)

sgs_wide.to_parquet(out_dir / "sgs_dados.parquet", index=False)

#####################################################################################

#  --- Dados SIDRA ---

## PIB por setores trimestral

pibs = sidra.get_table(
    table_code=5932,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='6562',
    period='all',
    classifications={
        '11255': '90687,90691,90707,90696,90704,93404,93405,93406'
    },
    header='n'
)

pibs
##################################################################################

# Funções de limpeza e transformação trimestrais SIDRA
def sidra_quarter_code_to_date(s: pd.Series) -> pd.Series:
    """
    Converte código trimestral SIDRA (YYYYQQ) em datetime:
    QQ=01..04 => último dia do trimestre.
    Ex.: 199601 -> 1996-03-31
    """
    s = s.astype(str).str.strip()
    year = s.str.slice(0, 4).astype(int)
    q = s.str.slice(4, 6).astype(int)

    # fim do trimestre: Q1=03-31, Q2=06-30, Q3=09-30, Q4=12-31
    month = q.map({1: 3, 2: 6, 3: 9, 4: 12})
    # dia final por mês (3,6,9,12)
    day = month.map({3: 31, 6: 30, 9: 30, 12: 31})

    return pd.to_datetime(
        dict(year=year, month=month, day=day),
        errors="coerce"
    )

def tidy_sidra_setores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # valor numérico (SIDRA às vezes vem como string)
    out["V"] = pd.to_numeric(out["V"], errors="coerce")

    # data a partir de D2C (YYYYQQ)
    out["date"] = sidra_quarter_code_to_date(out["D2C"])

    # colunas-alvo
    out = out.rename(columns={
        "D4N": "setor",
        "V": "value",
    })[["date", "setor","value"]]

    # limpeza básica
    out = out.dropna(subset=["date", "setor", "value"]).sort_values(["setor", "date"]).reset_index(drop=True)

    return out

# uso:
pib_long = tidy_sidra_setores(pibs)
pib_long


setores = pib_long["setor"].unique().tolist()
setores

#Exportando os dados pib

out_dir = BASE_DIR / "data" / "processed"

out_dir.mkdir(parents=True, exist_ok=True)

pib_long.to_parquet(out_dir / "pibs_quarterly.parquet", index=False)

## IPCA detalhado

IPCA_TABLE = "7060"

# Grupos (315) — índice cheio e grupos (seus códigos)
IPCA_GRUPOS_315 = "7169,7170,7445,7486,7558,7625,7660,7712,7766,7786"

# Variáveis (tabela 7060):
# 63  = variação mensal
# 2265 = variação acumulada em 12 meses
# 66  = peso mensal
IPCA_VARS = ["63", "2265", "66"]

VAR_LABELS = {
    "IPCA - Variação mensal": "variacao_mensal",
    "IPCA - Variação acumulada em 12 meses": "variacao_12m",
    "IPCA - Peso mensal": "peso_mensal",
}

def fetch_ipca_grupos(period: str = "all") -> pd.DataFrame:
    """
    Busca no SIDRA a tabela 7060 para Brasil (nível 1, código 1),
    para o IPCA cheio e grupos (classificação 315) e variáveis definidas em IPCA_VARS.
    """
    frames = []
    for var in IPCA_VARS:
        df = sidra.get_table(
            table_code=IPCA_TABLE,
            territorial_level="1",
            ibge_territorial_code="1",
            variable=var,
            classifications={"315": IPCA_GRUPOS_315},
            period=period,
            header="n",
        )
        frames.append(df)

    return pd.concat(frames, ignore_index=True)

#Tratamento ipca detalhado

def tidy_ipca_grupos(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    rename_map = {
        "D2C": "periodo",   # YYYYMM
        "D3N": "variavel",
        "D4N": "grupo",
        "V": "value",
    }

    for k, v in rename_map.items():
        if k in df.columns:
            df = df.rename(columns={k: v})

    # valor numérico
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # data: YYYYMM -> fim do mês
    df["date"] = (
        pd.to_datetime(df["periodo"].astype(str), format="%Y%m", errors="coerce")
        + pd.offsets.MonthEnd(0)
    )

    # normaliza nomes das variáveis
    df["indicador"] = df["variavel"].replace(VAR_LABELS)

    # limpa nome dos grupos (remove "1. ")
    df["grupo"] = (
        df["grupo"]
        .astype(str)
        .str.replace(r"^\d+\.\s*", "", regex=True)
        .str.strip()
    )

    out = (
        df[["date", "grupo", "indicador", "value"]]
        .dropna(subset=["date", "grupo", "indicador", "value"])
        .sort_values(["grupo", "indicador", "date"])
        .reset_index(drop=True)
    )

    return out




# coleta
raw_ipca = fetch_ipca_grupos(period="all")

# tratamento
ipca_grupos = tidy_ipca_grupos(raw_ipca)

# visualização rápida (debug)
print(ipca_grupos.head(10))
print(ipca_grupos.tail(10))
print(ipca_grupos["indicador"].value_counts())
print(ipca_grupos["grupo"].unique())



# Exportação de dados

# Exportação do IPCA grupos (parquet)
BASE_DIR = Path(__file__).resolve().parents[1]
out_dir = BASE_DIR / "data" / "processed"
out_dir.mkdir(parents=True, exist_ok=True)

ipca_grupos.to_parquet(out_dir / "ipca_grupos.parquet", index=False)



def tidy_ipca_grupos(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna dataframe long com:
      date (datetime, fim do mês),
      grupo (str),
      indicador (variacao_mensal | variacao_12m | peso_mensal),
      value (float)
    """
    df = df_raw.copy()

    # padrão sidrapy quando header='n': colunas como D2C, D2N, D4N e V
    # - D2C: período (YYYYMM)
    # - D4N: nome do grupo (geral/grupo/subgrupo/etc.)
    # - D3N: variável (nome)
    # - V  : valor
    rename_map = {
        "D2C": "periodo",
        "D3N": "variavel",
        "D4N": "grupo",
        "V": "value",
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df = df.rename(columns={k: v})

    # remove linhas estranhas
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # data: YYYYMM -> último dia do mês
    df["date"] = pd.to_datetime(df["periodo"].astype(str), format="%Y%m", errors="coerce") + pd.offsets.MonthEnd(0)

    # normaliza nomes de variável
    df["indicador"] = df["variavel"].replace(VAR_LABELS)

    # limpeza do nome do grupo (remove "1." etc.)
    df["grupo"] = df["grupo"].astype(str).str.replace(r"^\d+\.\s*", "", regex=True).str.strip()

    out = df[["date", "grupo", "indicador", "value"]].dropna(subset=["date", "grupo", "indicador", "value"])
    out = out.sort_values(["grupo", "indicador", "date"]).reset_index(drop=True)

    return out

# Coleta de dados IPCA detalhados

def build_ipca_grupos_dataset(period: str = "all") -> pd.DataFrame:
    raw = fetch_ipca_grupos(period=period)
    return tidy_ipca_grupos(raw)


def save_ipca_grupos_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "data" / "processed"

OUT_DIR.mkdir(parents=True, exist_ok=True)

ipca_grupos.to_parquet(
    OUT_DIR / "ipca_grupos.parquet",
    index=False
)

ipca_grupos.head()


#---------------------------

# Indice de preços ao produtor - IPP

ipp = sidra.get_table(
    table_code= 6904,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='1394',
    period='201501'+'-''202510',
    classifications={
        '543': '33586,33583,33585,33584,33580,33579'
    },
    header='n'
)

ipp.head()

def tidy_sidra_ipp(df: pd.DataFrame) -> pd.DataFrame:
    out = ipp.copy()

    # valor numérico (SIDRA às vezes vem como string)
    out["V"] = pd.to_numeric(out["V"], errors="coerce")

    # data a partir de D1C (YYYYMM)
    out["date"] = pd.to_datetime(out["D2C"].astype(str), format="%Y%m", errors="coerce") + pd.offsets.MonthEnd(0)

    # colunas-alvo
    out = out.rename(columns={
        "D4N": "setor_ipp",
        "V": "value",
    })[["date", "setor_ipp","value"]]

    # limpeza básica
    out = out.dropna(subset=["date", "setor_ipp", "value"]).sort_values(["setor_ipp", "date"]).reset_index(drop=True)

    return out

ipp_long = tidy_sidra_ipp(ipp)
ipp_long


#conferência
ipp_long["setor_ipp"].unique().tolist()

#Exportando os dados processados

BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
out_dir = BASE_DIR / "data" / "processed"

out_dir.mkdir(parents=True, exist_ok=True)

ipp_long.to_parquet(out_dir / "ipp_m.parquet", index=False)

#---------------------------
#   DADOS MENSAIS DA INDÚSTRIA, COMÉRCIO E SERVIÇOS - PMC, PMS e PIM
#PIM
pim_raw = sidra.get_table(
    table_code= 8888,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='11604',
    period='all',
    classifications={'544': '129314'},
    header='n'
)
pim_raw


def tidy_sidra_pim(df: pd.DataFrame) -> pd.DataFrame:
    out = pim_raw.copy()

    # valor numérico (SIDRA às vezes vem como string)
    out["V"] = pd.to_numeric(out["V"], errors="coerce")

    # data a partir de D2C (YYYYMM)
    out["date"] = pd.to_datetime(out["D2C"].astype(str), format="%Y%m", errors="coerce") + pd.offsets.MonthEnd(0)

    # colunas-alvo
    out = out.rename(columns={
        "V": "pim_12m",
    })[["date","pim_12m"]]

    # limpeza básica
    out = out.dropna(subset=["date", "pim_12m"]).sort_values(["date"]).reset_index(drop=True)
    return out

pim_long = tidy_sidra_pim(pim_raw)
pim_long


#PMS

pms_raw = sidra.get_table(
    table_code= 5906,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='11626',
    period='all',
    classifications={'11046': '56726'},
    header='n'
)
pms_raw

def tidy_sidra_pms(df: pd.DataFrame) -> pd.DataFrame:
    out = pms_raw.copy()

    # valor numérico (SIDRA às vezes vem como string)
    out["V"] = pd.to_numeric(out["V"], errors="coerce")

    # data a partir de D2C (YYYYMM)
    out["date"] = pd.to_datetime(out["D2C"].astype(str), format="%Y%m", errors="coerce") + pd.offsets.MonthEnd(0)

    # colunas-alvo
    out = out.rename(columns={
        "V": "pms_12m",
    })[["date","pms_12m"]]

    # limpeza básica
    out = out.dropna(subset=["date", "pms_12m"]).sort_values(["date"]).reset_index(drop=True)
    return out

pms_long = tidy_sidra_pms(pms_raw)
pms_long


#PMC
pmc_raw = sidra.get_table(
    table_code= 8880,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='11711',
    period='all',
    classifications={'11046':'56734'},
    header='n'
)
pmc_raw


def tidy_sidra_pmc(df: pd.DataFrame) -> pd.DataFrame:
    out = pmc_raw.copy()

    # valor numérico (SIDRA às vezes vem como string)
    out["V"] = pd.to_numeric(out["V"], errors="coerce")

    # data a partir de D2C (YYYYMM)
    out["date"] = pd.to_datetime(out["D2C"].astype(str), format="%Y%m", errors="coerce") + pd.offsets.MonthEnd(0)

    # colunas-alvo
    out = out.rename(columns={
        "V": "pmc_12m",
    })[["date","pmc_12m"]]

    # limpeza básica
    out = out.dropna(subset=["date", "pmc_12m"]).sort_values(["date"]).reset_index(drop=True)
    return out

pmc_long = tidy_sidra_pmc(pmc_raw)
pmc_long

#exportando os dados pim/pms/pmc

df_ind_com_ser = [
    pim_long,
    pms_long,
    pmc_long]

df_ind_com_ser

df_ind_com_ser_2 = reduce(
    lambda left, right: pd.merge(left, right, on="date", how="outer"),
    df_ind_com_ser
)
df_ind_com_ser_final = df_ind_com_ser_2.copy()
df_ind_com_ser_final.dropna(inplace=True)
df_ind_com_ser_final


BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
out_dir = BASE_DIR / "data" / "processed"

out_dir.mkdir(parents=True, exist_ok=True)

df_ind_com_ser_final.to_parquet(out_dir / "indust_comer_serv.parquet", index=False)


# ---------------------------------------------------------
# Dados Sócioeconômicos - SIDRA
# ---------------------------------------------------------

# Para onverter código trimestral SIDRA (YYYYQQ) em datetime
# ---------------------------------------------------------
def sidra_quarter_code_to_date(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    year = s.str.slice(0, 4).astype(int)
    q = s.str.slice(4, 6).astype(int)  # 01..04
    month_end = q.map({1: 3, 2: 6, 3: 9, 4: 12})
    return pd.to_datetime(
        dict(year=year, month=month_end, day=1),
        errors="coerce"
    ) + pd.offsets.MonthEnd(0)


# Funções de limpeza e transformação trimestrais 
# ---------------------------------------------------------
def tidy_sidra_brasil(df: pd.DataFrame, out_col: str) -> pd.DataFrame:
    out = df.copy()

    out["V"] = pd.to_numeric(out["V"], errors="coerce")
    out["date"] = sidra_quarter_code_to_date(out["D2C"])

    # Se existir algum recorte adicional (ex: D4N), tenta filtrar "Total"
    # (isso evita múltiplas linhas por trimestre)
    if "D4N" in out.columns:
        # tenta manter apenas "Total" (ajuste aqui se o texto for diferente)
        mask_total = out["D4N"].astype(str).str.contains("Total", case=False, na=False)
        if mask_total.any():
            out = out.loc[mask_total].copy()

    out = out.rename(columns={"V": out_col})[["date", out_col]]
    out = out.dropna(subset=["date", out_col]).sort_values("date")

    # blindagem: se ainda sobrar duplicata por trimestre, agrega (média)
    out = out.groupby("date", as_index=False)[out_col].mean()

    return out


# ---------------------------------------------------------
# UF (nível 3)
# ---------------------------------------------------------
def tidy_sidra_desemp_uf(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["V"] = pd.to_numeric(out["V"], errors="coerce")
    out["date"] = sidra_quarter_code_to_date(out["D2C"])

    if "D1N" in out.columns:
        out["uf"] = out["D1N"].astype(str).str.strip()
    elif "D1C" in out.columns:
        out["uf"] = out["D1C"].astype(str).str.strip()
    else:
        raise ValueError("Não encontrei a coluna de UF (D1N ou D1C) no dataframe do SIDRA.")

    out = (
        out.rename(columns={"V": "taxa_desemprego"})
           .loc[:, ["date", "uf", "taxa_desemprego"]]
           .dropna(subset=["date", "uf", "taxa_desemprego"])
           .sort_values(["uf", "date"])
           .reset_index(drop=True)
    )

    # blindagem: 1 obs por (uf, date)
    out = out.groupby(["uf", "date"], as_index=False)["taxa_desemprego"].mean()
    out["trimestre"] = out["date"].dt.to_period("Q").astype(str)

    return out


# ---------------------------------------------------------
# COLETA 
# ---------------------------------------------------------

# desemprego BR
desemp = sidra.get_table(
    table_code=4099,
    territorial_level="1",
    ibge_territorial_code="1",
    variable="4099",
    period="all",
    classifications="",
    header="n",
)

# desemprego UF
desemp_uf = sidra.get_table(
    table_code=4099,
    territorial_level="3",
    ibge_territorial_code="all",
    variable="4099",
    period="all",
    classifications="",
    header="n",
)

# ocupação BR
ocup = sidra.get_table(
    table_code=6466,
    territorial_level="1",
    ibge_territorial_code="1",
    variable="4097",
    period="all",
    classifications="",
    header="n",
)

# renda média BR
renda = sidra.get_table(
    table_code=5439,
    territorial_level="1",
    ibge_territorial_code="1",
    variable="5932",
    period="all",
    classifications={"12029": "99383"},
    header="n",
)

# informalidade BR
infor = sidra.get_table(
    table_code=8529,
    territorial_level="1",
    ibge_territorial_code="1",
    variable="12466",
    period="all",
    classifications="",
    header="n",
)

# desalentadas BR
desalent = sidra.get_table(
    table_code=6813,
    territorial_level="1",
    ibge_territorial_code="1",
    variable="9869",
    period="all",
    classifications="",
    header="n",
)

# ---------------------------------------------------------
# TIDY (agora correto)
# ---------------------------------------------------------
desemprego_long = tidy_sidra_brasil(desemp, "taxa_desemprego")
ocupacao_long = tidy_sidra_brasil(ocup, "taxa_ocupacao")
renda_media_long = tidy_sidra_brasil(renda, "renda_media")
informalidade_long = tidy_sidra_brasil(infor, "informalidade")
desalentadas_long = tidy_sidra_brasil(desalent, "desalentadas")

desemprego_uf_long = tidy_sidra_desemp_uf(desemp_uf)


# ---------------------------------------------------------
# MERGE (wide)
# ---------------------------------------------------------
dfs = [
    desemprego_long,
    ocupacao_long,
    renda_media_long,
    informalidade_long,
    desalentadas_long,
]

socioeco_wide = reduce(
    lambda left, right: pd.merge(left, right, on="date", how="outer"),
    dfs,
).sort_values("date").reset_index(drop=True)

# blindagem final: 1 obs por date
socioeco_wide = socioeco_wide.groupby("date", as_index=False).mean(numeric_only=True).sort_values("date")

# (opcional) não recomendo dropna total; prefira manter e tratar na página
# socioeco_wide.dropna(inplace=True)

# ---------------------------------------------------------
# Export
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
out_dir = BASE_DIR / "data" / "processed"
out_dir.mkdir(parents=True, exist_ok=True)

socioeco_wide.to_parquet(out_dir / "socioeconomico_quarterly.parquet", index=False)
desemprego_uf_long.to_parquet(out_dir / "desemp_uf.parquet", index=False)
