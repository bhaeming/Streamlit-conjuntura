import streamlit as st

st.set_page_config(
    page_title="Painel de Conjuntura da Economia do Brasil",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Painel de Conjuntura da Economia do Brasil")
st.write("Selecione um painel abaixo ou use o menu lateral para navegar.")

# Caminhos das páginas (relativos ao app)
PAGES = {
    "Dinâmica econômica": "pages/1_Dinamica_economica.py",
    "Preços ao consumidor e ao produtor": "pages/2_Precos.py",
    "Juros e crédito": "pages/3_Juros_e_credito.py",
    "Emprego e dados socioeconômicos": "pages/4_Empregos_dados_socio.py",
    "Balança comercial": "pages/5_Balanca_comercial.py",
}

def card(title: str, subtitle: str, target: str):
    with st.container(border=True):
        st.subheader(title)
        st.write(subtitle)
        if st.button("Abrir", key=f"open_{target}"):
            st.switch_page(target)

# Layout em grid
c1, c2 = st.columns(2)

with c1:
    card(
        "Dinâmica econômica",
        "Atividade: PIB, IBC-Br, PIM, PMC e PMS.",
        PAGES["Dinâmica econômica"],
    )
    card(
        "Juros e crédito",
        "Selic, taxas, crédito, inadimplência e spreads.",
        PAGES["Juros e crédito"],
    )

with c2:
    card(
        "Preços ao consumidor e ao produtor",
        "Inflação ao consumidor (IPCA) e preços ao produtor (IPP).",
        PAGES["Preços ao consumidor e ao produtor"],
    )
    card(
        "Emprego e dados socioeconômicos",
        "Desemprego, ocupação, renda, informalidade e desalento.",
        PAGES["Emprego e dados socioeconômicos"],
    )

st.divider()

# Uma caixa em largura total
with st.container(border=True):
    st.subheader("Balança comercial")
    st.write("Exportações, importações, saldo e destaques por produto/mercado.")
    if st.button("Abrir", key="open_trade"):
        st.switch_page(PAGES["Balança comercial"])

