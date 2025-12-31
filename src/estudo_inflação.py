import sidrapy as sidra
import pandas as pd
from bcb import sgs


#####################################################################################
#Importa dados do IPCA (índice cheio e grupos, a partir de janeiro/2020)

# Variação mensal, acumulada em 12 meses e peso mensal

dados_brutos_ipca = list(
    map(
        # função que será repetida
        lambda variavel: (
            sidra.get_table(
                table_code = "7060",        # tabela mais recente
                territorial_level = "1",
                ibge_territorial_code = "1",
                variable = variavel,
                classifications = { # índice cheio e grupos
                    "315": "7169,7170,7445,7486,7558,7625,7660,7712,7766,7786"
                    },
                period = "all"
                )
            ),

        # códigos da variável dentro da tabela (para o argumento variavel)
        ["63", "2265", "66"]
        )
    )
dados_brutos_ipca


# Tratamento dos dados

# Tratamento de dados do IPCA
dados_ipca = (
    pd.concat(dados_brutos_ipca)
    .rename(columns = dados_brutos_ipca[0].iloc[0])
    .rename(
        columns = {
            "Mês (Código)": "data",
            "Valor": "valor",
            "Variável": "variavel",
            "Geral, grupo, subgrupo, item e subitem": "grupo"
            }
        )
    .query("valor not in ['Valor', '...']")
    .filter(items = ["data", "variavel", "grupo", "valor"], axis = "columns")
    .replace(
        to_replace = {
            "variavel": {
                "IPCA - Variação mensal": "Variação % mensal",
                "IPCA - Variação acumulada em 12 meses": "Variação % acum. 12 meses",
                "IPCA - Peso mensal": "Peso mensal"
                },
             "grupo": {"\d.": ""}
              },
        regex = True
        )
    .assign(
        data = lambda x: pd.to_datetime(x.data, format = "%Y%m"),
        valor = lambda x: x.valor.astype(float)
    )
)
dados_ipca

dados_ipca.to_parquet("data/processed/  ipca_tratado.parquet", index = False) 