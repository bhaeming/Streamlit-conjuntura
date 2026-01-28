import pandas as pd
import numpy as np
from bcb import sgs
import sidrapy as sidra
from sidrapy import get_table



###################################################################
### Funções para Makedateset ###

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