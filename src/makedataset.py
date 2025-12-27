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

###################################################################
# Dados SGS

selic = sgs.get({'selic' : '432'},
               start = '2020-01-31')
selic
#tratamento dos dados da selic para mensal
selic_mensal = selic.resample('M').last().reset_index()
selic_mensal.rename(columns={"Date": "date"}, inplace=True)
selic_mensal
#Exportando os dados processados
BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
out_dir = BASE_DIR / "data" / "processed"

out_dir.mkdir(parents=True, exist_ok=True)

selic_mensal.to_parquet(out_dir / "selic_mensal.parquet", index=False)


ipca_mensal= sgs.get({'ipca' : '433'},
               start = '2014-01-31')
ipca_mensal

ipca_12m = sgs.get({'ipca_12m' : '13522'},
               start = '2014-01-31')
ipca_12m

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

#inadimplencia anual
inadimplencia =  sgs.get(
    {
        'inadimplencia_total' : '21085',
        'inadimplencia_pj' : '21086',
        'inadimplencia_pf' : '21112',
    },
    start='2014-01-31'
)
inadimplencia


#taxa de juros anual
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
    ipca_mensal,
    ipca_12m,
    saldo_cred,
    inadimplencia,
    taxa_de_juros]

df_sgs

sgs_wide = reduce(
    lambda left, right: pd.merge(left, right, on="Date", how="outer"),
    df_sgs
)
sgs_wide
sgs_wide=sgs_wide.reset_index()

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

# PIB por setores trimestral

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


#---------------------------------------------------------------------
# Sócio econômicos

#- desemprego

desemp = sidra.get_table(
    table_code= 4099,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='4099',
    period='all',
    classifications='',
    header='n'
)
desemp

def tidy_sidra_desemp(df: pd.DataFrame) -> pd.DataFrame:
    desemprego = desemp.copy()

    # valor numérico (SIDRA às vezes vem como string)
    desemprego["V"] = pd.to_numeric(desemprego["V"], errors="coerce")

    # data a partir de D2C (YYYYQQ)
    desemprego["date"] = sidra_quarter_code_to_date(desemprego["D2C"])

    # colunas-alvo
    desemprego = desemprego.rename(columns={
        "D4N": "setor",
        "V": "taxa_desemprego",
    })[["date", "taxa_desemprego"]]

    return desemprego

desemprego_long = tidy_sidra_desemp(desemp)
desemprego_long
#ocupação

ocup = sidra.get_table(
    table_code= 6466,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='4097',
    period='all',
    classifications='',
    header='n'
)
ocup


def tidy_sidra_ocup(df: pd.DataFrame) -> pd.DataFrame:
    ocupacao = ocup.copy()

    # valor numérico (SIDRA às vezes vem como string)
    ocupacao["V"] = pd.to_numeric(ocupacao["V"], errors="coerce")

    # data a partir de D2C (YYYYQQ)
    ocupacao["date"] = sidra_quarter_code_to_date(ocupacao["D2C"])

    # colunas-alvo
    ocupacao = ocupacao.rename(columns={
        "D4N": "setor",
        "V": "taxa_ocupacao",
    })[["date", "taxa_ocupacao"]]

    return ocupacao

ocupacao_long =  tidy_sidra_ocup(ocup)
ocupacao_long


# renda média

renda = sidra.get_table(
    table_code= 5439,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='5932',
    period='all',
    classifications={'12029': '99383'},
    header='n'
)
renda

def tidy_sidra_renda(df: pd.DataFrame) -> pd.DataFrame:
    renda_media = renda.copy()

    # valor numérico (SIDRA às vezes vem como string)
    renda_media["V"] = pd.to_numeric(renda_media["V"], errors="coerce")

    # data a partir de D2C (YYYYQQ)
    renda_media["date"] = sidra_quarter_code_to_date(renda_media["D2C"])

    # colunas-alvo
    renda_media = renda_media.rename(columns={
        "V": "renda_media",
    })[["date", "renda_media"]]

    return renda_media

renda_media_long = tidy_sidra_renda(renda)
renda_media_long


# informalidade

infor= sidra.get_table(
    table_code= 8529,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='12466',
    period='all',
    classifications='',
    header='n'
)
infor

def tidy_sidra_informalidade(df: pd.DataFrame) -> pd.DataFrame:
    informalidade = infor.copy()

    # valor numérico (SIDRA às vezes vem como string)
    informalidade["V"] = pd.to_numeric(informalidade["V"], errors="coerce")

    # data a partir de D2C (YYYYQQ)
    informalidade["date"] = sidra_quarter_code_to_date(informalidade["D2C"])

    # colunas-alvo
    informalidade = informalidade.rename(columns={
        "V": "informalidade",
    })[["date", "informalidade"]]

    return informalidade
informalidade_long = tidy_sidra_informalidade(infor)
informalidade_long

#Pessoas desalentadas

desalent = sidra.get_table(
    table_code= 6813,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='9869',
    period='all',
    classifications='',
    header='n'
)
desalent

def tidy_sidra_desalent(df: pd.DataFrame) -> pd.DataFrame:

    desalentadas = desalent.copy()

    # valor numérico (SIDRA às vezes vem como string)
    desalentadas["V"] = pd.to_numeric(desalentadas["V"], errors="coerce")
    # data a partir de D2C (YYYYQQ)
    desalentadas["date"] = sidra_quarter_code_to_date(desalentadas["D2C"])

    # colunas-alvo
    desalentadas = desalentadas.rename(columns={
        "V": "desalentadas",
    })[["date", "desalentadas"]]

    return desalentadas

desalentadas_long = tidy_sidra_desalent(desalent)
desalentadas_long

#juntando os dados sócioeconômicos

dfs = [
    desemprego_long,
    ocupacao_long,
    renda_media_long,
    informalidade_long,
    desalentadas_long,
]

socioeco_wide = reduce(
    lambda left, right: pd.merge(left, right, on="date", how="outer"),
    dfs
).sort_values("date").reset_index(drop=True)

socioeco_wide.dropna(inplace=True)
socioeco_wide.head()

#Exportando os dados processados

BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
out_dir = BASE_DIR / "data" / "processed"

out_dir.mkdir(parents=True, exist_ok=True)

socioeco_wide.to_parquet(out_dir / "socioeconomico_quarterly.parquet", index=False)