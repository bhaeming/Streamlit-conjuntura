import pandas as pd
import numpy as np
from bcb import sgs
import sidrapy as sidra
from sidrapy import get_table

VAR_LABELS = {
    "IPCA - Variação mensal": "variacao_mensal",
    "IPCA - Variação acumulada em 12 meses": "variacao_12m",
    "IPCA - Peso mensal": "peso_mensal",
}



###################################################################
### Funções para Makedateset ###
###################################################################

#------------------------------------------------------------
# Trata DataFrame para garantir índice datetime no fim do mês
#------------------------------------------------------------

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




# --------------------------------------------------
# Função que acumula séries de taxas mensais em 12 meses #
#(utilizada nas séries de inflação  e variação de preços) #
# --------------------------------------------------

# Funções de tratamento e transformação #

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

#------------------------------------------------------------
# Trata SIDRA  com colunas padrão
#------------------------------------------------------------

# Trata dados mensais do sidra com coluna de período YYYYMM e valor

def tidy_sidra_monthly_single(
    df: pd.DataFrame,
    value_name: str,
    period_col: str = "D2C",
    value_col: str = "V",
) -> pd.DataFrame:
    """
    Trata um SIDRA mensal (header='n') com colunas padrão:
      - D2C: YYYYMM (período)
      - V: valor

    Retorna df com colunas:
      - date (fim do mês)
      - <value_name> (numérico)
    """
    out = df.copy()

    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
    out["date"] = pd.to_datetime(out[period_col].astype(str), format="%Y%m", errors="coerce") + pd.offsets.MonthEnd(0)

    out = out.rename(columns={value_col: value_name})[["date", value_name]]
    out = out.dropna(subset=["date", value_name]).sort_values("date").reset_index(drop=True)

    return out

# Trata dados trimestrais do sidra com colunas padrão
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

    out = out.dropna(subset=["date", "setor", "value"]).sort_values(["setor", "date"]).reset_index(drop=True)

    return out

#Tratamento IPP
def tidy_sidra_ipp(df: pd.DataFrame, label_col: str = "setor_ipp") -> pd.DataFrame:
    out = df.copy()

    # valor numérico (SIDRA às vezes vem como string)
    out["V"] = pd.to_numeric(out["V"], errors="coerce")

    # data a partir de D1C (YYYYMM)
    out["date"] = pd.to_datetime(out["D2C"].astype(str), format="%Y%m", errors="coerce") + pd.offsets.MonthEnd(0)

    # colunas-alvo
    rename_map = {
        "D4C": "codigo_ipp",
        "D4N": label_col,
        "V": "value",
    }
    out = out.rename(columns={k: v for k, v in rename_map.items() if k in out.columns})
    keep_cols = ["date", label_col, "value"]
    if "codigo_ipp" in out.columns:
        keep_cols.insert(2, "codigo_ipp")
    out = out[keep_cols]

    # limpeza básica
    out[label_col] = (
        out[label_col]
        .astype(str)
        .str.replace(r"^\d+\s*", "", regex=True)
        .str.strip()
    )
    out = out.dropna(subset=["date", label_col, "value"]).sort_values([label_col, "date"]).reset_index(drop=True)

    return out

# Tratamento dos dados do IPCA por grupo (mensal, header='n', colunas D2C, D3N, D4N, V)
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

