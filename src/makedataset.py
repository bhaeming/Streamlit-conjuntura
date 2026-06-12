import pandas as pd
import numpy as np
from bcb import sgs
import sidrapy as sidra
from sidrapy import get_table
import statsmodels.api as sm
import statsmodels.formula.api as smf
from functools import reduce
from pathlib import Path   
from building_features import tidy_sidra_monthly_single
from building_features import ensure_month_end_index
from building_features import acc_12m_curve_rate
from building_features import add_12m_from_monthly_rates
from building_features import tidy_sidra_setores
from building_features import sidra_quarter_code_to_date
from building_features import tidy_sidra_ipp
from building_features import tidy_ipca_grupos

BASE_DIR = Path(__file__).resolve().parents[1]


def make_sgs():
    ###################################################################
    ### Dados SGS ###
    ###################################################################

    # -----------------------------
    # Coleta dados Selic
    # ----------------------------
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
    # Coleta dados IPCA do SGS
    # -----------------------------
    ipca_mensal = sgs.get({"ipca": "433"}, start="2014-01-31")
    ipca_12m    = sgs.get({"ipca_12m": "13522"}, start="2014-01-31")

    ipca_ld_m = sgs.get(
        {"ipca_livres": "11428", "ipca_administrados": "4449"},
        start="2011-01-31",
    )


    # Tratamento e padronização dos índices (fim do mês)

    ipca_mensal = ensure_month_end_index(ipca_mensal)
    ipca_12m    = ensure_month_end_index(ipca_12m)
    ipca_ld_m   = ensure_month_end_index(ipca_ld_m)


    # Limpeza a alinhamento de datas

    df_ipca_all = (
        ipca_mensal.join(ipca_12m, how="outer")
                .join(ipca_ld_m, how="outer")
                .sort_index()
    )


    # Cria 12m calculado para as séries mensais

    df_ipca_all = add_12m_from_monthly_rates(
        df_ipca_all,
        cols=["ipca_livres", "ipca_administrados", "ipca"],  # pode tirar "ipca" se não quiser
        suffix_12m="_12m_calc"
    )


    # Export dos dfs

    df_ipca_all = df_ipca_all.dropna(how="all")

    df_ipca_all_out = df_ipca_all.reset_index().rename(columns={"index": "date"})
    df_ipca_all_out

    BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
    out_dir = BASE_DIR / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    df_ipca_all_out.to_parquet(out_dir / "ipca_all.parquet", index=False)


    #####################################################################################

    # -----------------------------
    # Coleta dados IBC-Br e crédito do SGS
    # ----------------------------
    ##Brasil 
    ibc_br= sgs.get({'ibc_br' : '24363',
                    'ibc_br_dessaz': '24364'},
                start = '2014-03-31')
    ibc_br

    ##ufs 
    ibc_uf = sgs.get(
        {
            "ibc_se": "25393",        
            "ibc_se_dessaz": "25395",
            "ibc_mg": "25379",
            "ibc_mg_dessaz": "25380",
            "ibc_rj": "25396",
            "ibc_rj_dessaz": "25397",
            "ibc_es": "25398",
            "ibc_es_dessaz": "25399",
            "ibc_sp": "25392",
            "ibc_sp_dessaz": "25394",
            "ibc_co": "25381",
            "ibc_co_dessaz": "25382",
            "ibc_go": "25383",
            "ibc_go_dessaz": "25384",
            "ibc_sul": "25400",
            "ibc_sul_dessaz": "25403",        
            "ibc_sc": "25402",
            "ibc_sc_dessaz": "25405",
            "ibc_pr": "25408",
            "ibc_pr_dessaz": "25413",
            "ibc_rs": "25401",
            "ibc_rs_dessaz": "25404",       
            "ibc_norte": "25406",
            "ibc_norte_dessaz": "25407",        
            "ibc_pa": "25409",
            "ibc_pa_dessaz": "25410",
            "ibc_am": "25411",
            "ibc_am_dessaz": "25412",
            "ibc_ne": "25388",
            "ibc_ne_dessaz": "25389",
            "ibc_ce": "25390",
            "ibc_ce_dessaz": "25391",        
            "ibc_ba": "25415",
            "ibc_ba_dessaz": "25416",
            "ibc_pe": "25417",
            "ibc_pe_dessaz": "25418",
        },
        start="2014-03-31",
    )
    ibc_uf
    ibc_uf.columns

    saldo_cred = sgs.get(
        {
            'credito_pf': '20570',
            'credito_pj': '20543',
            'credito_total': '20542'
        },
        start='2014-01-31'
    )
    saldo_cred

    # inadimplencia da carteira de credito com recursos livres (%)
    # SGS 21085: Total | 21086: Pessoas juridicas | 21112: Pessoas fisicas
    inadimplencia =  sgs.get(
        {
            'inadimplencia_total' : '21085',
            'inadimplencia_pj' : '21086',
            'inadimplencia_pf' : '21112',
        },
        start='2014-01-31'
    )
    inadimplencia


    # taxa media de juros das operacoes de credito com recursos livres (% a.a.)
    # SGS 20717: Total | 20718: Pessoas juridicas | 20740: Pessoas fisicas
    taxa_de_juros = sgs.get(
        {
            'taxa_juros_pf' : '20740',
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
        lambda left, right: left.join(right, how="outer"),
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

    credito_condicoes = sgs_wide[
        [
            "date",
            "taxa_juros_total",
            "taxa_juros_pf",
            "taxa_juros_pj",
            "inadimplencia_total",
            "inadimplencia_pf",
            "inadimplencia_pj",
        ]
    ].copy()

    credito_condicoes.to_parquet(out_dir / "credito_condicoes.parquet", index=False)

    #exportação IBC-UF

    BASE_DIR = Path(__file__).resolve().parents[1]
    out_dir = BASE_DIR / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    ibc_uf_out = ibc_uf.copy()

    # Se 'Date' está no índice, traz para coluna
    if isinstance(ibc_uf_out.index, pd.DatetimeIndex):
        ibc_uf_out = ibc_uf_out.reset_index()

    # Padroniza nome
    if "Date" in ibc_uf_out.columns and "date" not in ibc_uf_out.columns:
        ibc_uf_out = ibc_uf_out.rename(columns={"Date": "date"})

    ibc_uf_out["date"] = pd.to_datetime(ibc_uf_out["date"], errors="coerce")

    # Ordena e limpa
    ibc_uf_out = ibc_uf_out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    ibc_uf_out.to_parquet(out_dir / "ibc_uf.parquet", index=False)


make_sgs()



###################################################################
### Dados SIDRA ###
###################################################################

# ------------------------------------
#PIB por setores trimestral
# ------------------------------------

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
# uso:
pib_long = tidy_sidra_setores(pibs)
pib_long.describe()
setores = pib_long["setor"].unique().tolist()
setores

#Exportando os dados pib

out_dir = BASE_DIR / "data" / "processed"

out_dir.mkdir(parents=True, exist_ok=True)

pib_long.to_parquet(out_dir / "pibs_quarterly.parquet", index=False)


# ------------------------------------
# coleta IPCA detalhado (grupos e pesos)
# ------------------------------------

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

# coleta
raw_ipca = fetch_ipca_grupos(period="all")

# tratamento
ipca_grupos = tidy_ipca_grupos(raw_ipca)

# visualização rápida (debug)
print(ipca_grupos.head(10))
print(ipca_grupos.tail(10))
print(ipca_grupos["indicador"].value_counts())
print(ipca_grupos["indicador"].unique())

print(ipca_grupos["grupo"].unique())



# Exportação de dados

# Exportação do IPCA grupos (parquet)
BASE_DIR = Path(__file__).resolve().parents[1]
out_dir = BASE_DIR / "data" / "processed"
out_dir.mkdir(parents=True, exist_ok=True)

ipca_grupos.to_parquet(out_dir / "ipca_grupos.parquet", index=False)




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


#------------------------------------
# Indice de preços ao produtor - IPP
#------------------------------------

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
# Tratamento IPP
ipp_long = tidy_sidra_ipp(ipp)
ipp_long

#conferência
ipp_long["setor_ipp"].unique().tolist()

#Exportando os dados processados

BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
out_dir = BASE_DIR / "data" / "processed"

out_dir.mkdir(parents=True, exist_ok=True)

ipp_long.to_parquet(out_dir / "ipp_m.parquet", index=False)

# IPP por atividade CNAE 2.0
# O repositorio Painel-Infla--o usa a tabela 6904 (grandes categorias economicas).
# Para a perspectiva por CNAE, a tabela correta do SIDRA e a 6903, classificacao 842.
ipp_cnae = sidra.get_table(
    table_code=6903,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='1394',
    period='all',
    classifications={
        '842': 'all'
    },
    header='n'
)

ipp_cnae_long = tidy_sidra_ipp(ipp_cnae, label_col="cnae_ipp")
ipp_cnae_long = ipp_cnae_long[ipp_cnae_long["date"] >= pd.Timestamp("2015-01-01")].reset_index(drop=True)

ipp_cnae_long.to_parquet(out_dir / "ipp_cnae.parquet", index=False)



#---------------------------------------------------------------------
#   DADOS MENSAIS DA INDÚSTRIA, COMÉRCIO E SERVIÇOS - PMC, PMS e PIM
#---------------------------------------------------------------------

#PIM - Pesquisa Industrial Mensal (12 meses)


#PIM -  (nivel)
pim_n_raw = sidra.get_table(
    table_code= 8888,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='12606',
    period='all',
    classifications={'544': '129314'},
    header='n'
)
pim_n_raw


#PIM - (dessazonalizada)
pim_dessaz_raw = sidra.get_table(
    table_code= 8888,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='12607',
    period='all',
    classifications={'544': '129314'},
    header='n'
)
pim_dessaz_raw

#PIM - (12 meses)
pim_12_raw = sidra.get_table(
    table_code= 8888,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='11604',
    period='all',
    classifications={'544': '129314'},
    header='n'
)
pim_12_raw

#PMS - Pesquisa Mensal de Serviços

#PMS - (nível)
pms_n_raw = sidra.get_table(
    table_code= 5906,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='7167',
    period='all',
    classifications={'11046': '56726'},
    header='n'
)
pms_n_raw

#PMS - Pesquisa Mensal de Serviços (dessazonalizada)

pms_dessaz_raw = sidra.get_table(
    table_code= 5906,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='7168',
    period='all',
    classifications={'11046': '56726'},
    header='n'
)
pms_dessaz_raw

## PMS em nível (acumulado 12 meses)

###BRASIL
pms_raw_12 = sidra.get_table(
    table_code= 5906,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='11626',
    period='all',
    classifications={'11046': '56726'},
    header='n'
)
pms_raw_12
###SC



#PMC - Pesquisa Mensal do Comércio

# PMC - (nível)
pmc_n_raw = sidra.get_table(
    table_code= 8881,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='7169',
    period='all',
    classifications={'11046':'56736'},
    header='n'
)
pmc_n_raw

# PMC - (dessazonalizada)
pmc_dessaz_raw = sidra.get_table(
    table_code= 8881,
    territorial_level='1',
    ibge_territorial_code='1',
    variable='7170',
    period='all',
    classifications={'11046':'56736'},
    header='n'
)
pmc_dessaz_raw

#PMC - (12 meses)
pmc_12_raw = sidra.get_table(
    table_code= 8881,
    territorial_level='1',  
    ibge_territorial_code='1',
    variable='11711',
    period='all',
    classifications={'11046':'56736'},
    header='n'
)
pmc_12_raw

pim_n = tidy_sidra_monthly_single(pim_n_raw, value_name="pim_nivel")
pim_dessaz= tidy_sidra_monthly_single(pim_dessaz_raw, value_name="pim_dessaz")
pim_12 = tidy_sidra_monthly_single(pim_12_raw, value_name="pim_12m")
pms_n = tidy_sidra_monthly_single(pms_n_raw, value_name="pms_nivel")
pms_dessaz = tidy_sidra_monthly_single(pms_dessaz_raw, value_name="pms_dessaz")
pms_12 = tidy_sidra_monthly_single(pms_raw_12, value_name="pms_12m")
pmc_n = tidy_sidra_monthly_single(pmc_n_raw, value_name="pmc_nivel")
pmc_dessaz= tidy_sidra_monthly_single(pmc_dessaz_raw, value_name="pmc_dessaz")
pmc_12 = tidy_sidra_monthly_single(pmc_12_raw, value_name="pmc_12m")

# Agregando DFs
df_ind_com_ser = [
    pim_n,
    pim_dessaz,
    pim_12,
    pms_n,
    pms_dessaz,
    pms_12,
    pmc_n,
    pmc_dessaz,
    pmc_12,
]


#exportando os dados pim/pms/pmc

df_ind_com_ser

df_ind_com_ser_2 = reduce(
    lambda left, right: pd.merge(left, right, on="date", how="outer"),
    df_ind_com_ser
)
df_ind_com_ser_2

BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
out_dir = BASE_DIR / "data" / "processed"

out_dir.mkdir(parents=True, exist_ok=True)

df_ind_com_ser_2.to_parquet(out_dir / "indust_comer_serv.parquet", index=False)


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
def tidy_sidra_desemp_uf(df: pd.DataFrame, value_name: str = "taxa_desemprego") -> pd.DataFrame:
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
        out.rename(columns={"V": value_name})
           .loc[:, ["date", "uf", value_name]]
           .dropna(subset=["date", "uf", value_name])
           .sort_values(["uf", "date"])
           .reset_index(drop=True)
    )

    # blindagem: 1 obs por (uf, date)
    out = out.groupby(["uf", "date"], as_index=False)[value_name].mean()
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

# ocupacao UF
ocup_uf = sidra.get_table(
    table_code=6466,
    territorial_level="3",
    ibge_territorial_code="all",
    variable="4097",
    period="all",
    classifications="",
    header="n",
)

# renda media UF
renda_uf = sidra.get_table(
    table_code=5439,
    territorial_level="3",
    ibge_territorial_code="all",
    variable="5932",
    period="all",
    classifications={"12029": "99383"},
    header="n",
)

# informalidade UF
infor_uf = sidra.get_table(
    table_code=8529,
    territorial_level="3",
    ibge_territorial_code="all",
    variable="12466",
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
# Tragamento para formato tidy (long)
# ---------------------------------------------------------
desemprego_long = tidy_sidra_brasil(desemp, "taxa_desemprego")
ocupacao_long = tidy_sidra_brasil(ocup, "taxa_ocupacao")
renda_media_long = tidy_sidra_brasil(renda, "renda_media")
informalidade_long = tidy_sidra_brasil(infor, "informalidade")
desalentadas_long = tidy_sidra_brasil(desalent, "desalentadas")

desemprego_uf_long = tidy_sidra_desemp_uf(desemp_uf, "taxa_desemprego")
ocupacao_uf_long = tidy_sidra_desemp_uf(ocup_uf, "taxa_ocupacao")
renda_media_uf_long = tidy_sidra_desemp_uf(renda_uf, "renda_media")
informalidade_uf_long = tidy_sidra_desemp_uf(infor_uf, "informalidade")

socioeco_uf = reduce(
    lambda left, right: pd.merge(left, right, on=["date", "uf", "trimestre"], how="outer"),
    [
        desemprego_uf_long,
        ocupacao_uf_long,
        renda_media_uf_long,
        informalidade_uf_long,
    ],
).sort_values(["uf", "date"]).reset_index(drop=True)


# ---------------------------------------------------------
# Transformação para formato long
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
socioeco_uf.to_parquet(out_dir / "socioeconomico_uf.parquet", index=False)
