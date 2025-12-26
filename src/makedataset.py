import pandas as pd
import numpy as np
import sidrapy as sd
from bcb import sgs
import sidrapy as sidra
from sidrapy import get_table
import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf   


# Dados SGS



selic = sgs.get({'selic' : '432'},
               start = '2023-01-31')
selic

ipca = sgs.get({'ipca' : '433'},
               start = '2014-01-31')

ipca_12m = sgs.get({'ipca_12m' : '13522'},
               start = '2014-01-31')

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


# Dados SIDRA

#PIB por setores trimestral

pibs = sidra.get_table(
    table_code=5932,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='6561',
    period='all',
    classifications={
        '11255': '90687,90691,90696,90704,93404,93405,93406'
    },
    header='n'
)

pibs


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

# Indice de preços ao produtor - IPP

ipp = sidra.get_table(
    table_code= 6904,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='1394',
    period='all',
    classifications={
        '543': '33586,33583,33585,33584,33580,33579'
    },
    header='n'
)

ipp

def tidy_sidra_ipp(df: pd.DataFrame) -> pd.DataFrame:
    out = ipp.copy()

    # valor numérico (SIDRA às vezes vem como string)
    out["V"] = pd.to_numeric(out["V"], errors="coerce")

    # data a partir de D1C (YYYYMM)
    out["date"] = pd.to_datetime(out["D1C"].astype(str), format="%Y%m", errors="coerce") + pd.offsets.MonthEnd(0)

    # colunas-alvo
    out = out.rename(columns={
        "D4N": "setorfe_ipp",
        "V": "value",
    })[["date", "setor_ipp","value"]]

    # limpeza básica
    out = out.dropna(subset=["date", "setor_ipp", "value"]).sort_values(["setor_ipp", "date"]).reset_index(drop=True)

    return out

ipp_long = tidy_sidra_ipp(ipp)
ipp_long