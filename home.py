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
    "Emprego e dados socioeconômicos": "pages/4_Empregos_dados_socioeconomicos.py",
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
        "Selic, taxas de juros, crédito e inadimplência",
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
    st.subheader("Balança comercial (Em contrução)")
    st.write("Exportações, importações, saldo e destaques por produto/mercado.")
    if st.button("Abrir", key="open_trade"):
        st.switch_page(PAGES["Balança comercial (Em contrução)"])

st.divider()


col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Sobre o autor", divider=True)
    st.subheader("Bruno Haeming")
    st.write("""
     
    Economista | Doutor em Relações Internacionais com ênfase em Economia Política Internacional  

    Especialista em conjuntura macroeconômica, análise setorial,modelagem econométrica aplicada,
    política e risco político internacional.
    

    """)
    st.subheader("Sobre o Painel", divider=True)
    st.write("""
                 
        Este painel oferece  uma leitura ágil, integrada e atualizada da economia brasileira.
    A aplicação está em constante evolução, com novos dados, análises e funcionalidades, incorporando 
    feedbacks dos usuários.
             
    O público-alvo são analistas, gestores, acadêmicos, estudantes e pessoas interessadas em acompanhar a 
    conjuntura econômica do Brasil.
             
    O painel é construído com Streamlit e Python, utilizando dados oficiais de fontes como IBGE, 
    Banco Central do Brasil, MDIC, entre outras.  

             """)

with col2:
    st.subheader("Contato")
    st.markdown("""
    🔗 [LinkedIn](https://www.linkedin.com/in/bruno-haeming-87528b142//)  
    📄 [Lattes](http://lattes.cnpq.br/4249387473108996/)  
    ✉️ bhaeming@gmail.com
    """)
