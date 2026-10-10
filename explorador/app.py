"""Explorador do datalake: arquivos, partições e consultas SQL no S3 do floci.

    docker compose up -d explorador      # e abra http://localhost:8501

Somente leitura: nada aqui grava ou apaga no bucket.
"""

from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import pandas as pd
import streamlit as st

from streamlit.runtime.scriptrunner_utils.exceptions import RerunException, StopException

from explorador import consultas, particoes, s3

EXEMPLOS = {
    "Eventos por nome (landing)": (
        "SELECT event_name, count(*) AS eventos\nFROM landing_eventos\n"
        "GROUP BY event_name\nORDER BY eventos DESC"
    ),
    "Linhas por dia (Silver)": (
        "SELECT year, month, day, count(*) AS linhas\nFROM silver_eventos\nGROUP BY ALL\nORDER BY ALL"
    ),
    "Colunas de uma view": "DESCRIBE silver_eventos",
}


@st.cache_data(ttl=30, show_spinner="Listando o bucket…")
def _listar() -> list[particoes.Objeto]:
    return s3.listar()


@st.cache_resource(show_spinner="Conectando ao S3 e criando as views…")
def _conexao(datasets: tuple[particoes.Dataset, ...], raiz: str):
    con = consultas.conectar(endpoint=s3.ENDPOINT, chave=s3.CHAVE, segredo=s3.SEGREDO, regiao=s3.REGIAO)
    views = consultas.criar_views(con, raiz, list(datasets))
    colunas = {nome: con.sql(f'DESCRIBE "{nome}"').df()["column_name"].tolist() for nome in views}
    consultas.travar(con, raiz)
    return con, colunas


def _aba_arquivos(objetos, con, raiz):
    pasta = st.session_state.get("pasta", "")
    st.markdown(f"**Pasta atual:** `/{pasta}`")
    if pasta and st.button("⬆ Voltar uma pasta", key="voltar"):
        partes = pasta.rstrip("/").split("/")[:-1]
        st.session_state["pasta"] = "/".join(partes) + "/" if partes else ""
        st.rerun()
    subpastas, arquivos = particoes.navegar(objetos, pasta)
    colunas = st.columns(4)
    for i, sub in enumerate(subpastas):
        if colunas[i % 4].button(f"📁 {sub}", key=f"pasta:{pasta}{sub}"):
            st.session_state["pasta"] = f"{pasta}{sub}/"
            st.rerun()
    if not arquivos:
        st.caption("Nenhum arquivo direto nesta pasta. Entre numa subpasta.")
        return
    st.dataframe(pd.DataFrame([{"arquivo": o.chave[len(pasta):], "tamanho": particoes.tamanho_legivel(o.tamanho),
                                "modificado": o.modificado} for o in arquivos]), hide_index=True)
    legiveis = [o.chave[len(pasta):] for o in arquivos if particoes.formato_do_arquivo(o.chave)]
    escolhido = st.selectbox("Prévia de um arquivo", legiveis, index=None, key="previa",
                             placeholder="Escolha um arquivo .parquet ou .jsonl")
    if escolhido:
        try:
            st.dataframe(consultas.previa(con.cursor(), raiz, pasta + escolhido), hide_index=True)
            st.caption(f"Primeiras {consultas.LINHAS_PREVIA} linhas, no máximo.")
        except duckdb.Error as erro:
            st.error(f"Não consegui ler o arquivo: {erro}")


def _aba_particoes(objetos, datasets, con):
    if not datasets:
        st.info("Ainda não há datasets (pastas com .parquet ou .jsonl) no bucket.")
        return
    escolha = st.selectbox("Dataset", [d.prefixo for d in datasets], key="dataset")
    linhas = particoes.tabela_particoes(objetos, escolha)
    if not linhas:
        st.info("Nenhum arquivo de dados neste dataset.")
        return
    tabela = pd.DataFrame([asdict(l) for l in linhas])
    tabela["tamanho"] = tabela["bytes"].map(particoes.tamanho_legivel)
    st.dataframe(tabela, hide_index=True)
    st.caption(f"{len(linhas)} partição(ões) · {int(tabela['arquivos'].sum())} arquivo(s) · "
               f"{particoes.tamanho_legivel(int(tabela['bytes'].sum()))}")
    if st.button("Contar linhas por partição", key="contar"):
        view = next(d.nome_view for d in datasets if d.prefixo == escolha)
        particionado = any(l.year != particoes.SEM_PARTICAO for l in linhas)
        sql = (f'SELECT year, month, day, count(*) AS linhas FROM "{view}" GROUP BY ALL ORDER BY ALL'
               if particionado else f'SELECT count(*) AS linhas FROM "{view}"')
        try:
            st.dataframe(consultas.executar(con.cursor(), sql, consultas.LIMITE_MAXIMO).tabela, hide_index=True)
        except duckdb.Error as erro:
            st.error(f"Não consegui contar: {erro}")


def _aba_sql(colunas, con):
    with st.expander(f"Views disponíveis ({len(colunas)})"):
        for nome, cols in colunas.items():
            st.markdown(f"**`{nome}`** — " + ", ".join(f"`{c}`" for c in cols))
    st.caption("Exemplos:")
    botoes = st.columns(len(EXEMPLOS))
    for coluna, (titulo, sql) in zip(botoes, EXEMPLOS.items()):
        coluna.button(titulo, key=f"exemplo:{titulo}", on_click=st.session_state.update, kwargs={"sql": sql})
    st.text_area("SQL (somente leitura)", key="sql", height=160,
                 placeholder="SELECT * FROM silver_eventos LIMIT 10")
    limite = st.number_input("Limite de linhas", min_value=1, max_value=consultas.LIMITE_MAXIMO,
                             value=consultas.LIMITE_PADRAO, step=100, key="limite")
    if st.button("Executar", key="executar", type="primary"):
        try:
            st.session_state["ultimo_resultado"] = (
                consultas.executar(con.cursor(), st.session_state.get("sql", ""), limite), limite)
        except consultas.SqlNaoPermitido as erro:
            st.session_state.pop("ultimo_resultado", None)
            st.warning(str(erro))
        except duckdb.Error as erro:
            st.session_state.pop("ultimo_resultado", None)
            st.error(f"Erro no SQL: {erro}")
            st.info("Confira os nomes em \"Views disponíveis\", logo acima.")
    if "ultimo_resultado" not in st.session_state:
        return
    resultado, limite_usado = st.session_state["ultimo_resultado"]
    if resultado.cortado:
        st.warning(f"Resultado cortado em {int(limite_usado)} linhas. Aumente o limite ou filtre a consulta.")
    st.dataframe(resultado.tabela, hide_index=True)
    st.download_button("Baixar CSV", resultado.tabela.to_csv(index=False).encode("utf-8"),
                       file_name="resultado.csv", mime="text/csv", key="baixar", on_click="ignore")


def _pagina() -> None:
    st.set_page_config(page_title="Explorador do datalake", layout="wide")
    st.title("Explorador do datalake")
    topo, botao = st.columns([5, 1])
    topo.caption(f"Bucket `{s3.BUCKET}` em `{s3.ENDPOINT}` · somente leitura")
    if botao.button("Recarregar", key="recarregar"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    try:
        objetos = _listar()
    except s3.S3Indisponivel as erro:
        st.error(str(erro))
        st.stop()
    if not objetos:
        st.info("O bucket está vazio. Gere os eventos com "
                "`docker compose run --rm app python gerador/gerar_eventos.py` e clique em Recarregar.")
        st.stop()

    datasets = particoes.datasets(o.chave for o in objetos)
    raiz = s3.raiz()
    try:
        con, colunas = _conexao(tuple(datasets), raiz)
    except duckdb.Error as erro:
        st.error(f"Não consegui preparar as views no DuckDB: {erro}")
        st.stop()

    arquivos, particoes_, sql = st.tabs(["Arquivos", "Partições", "Consultas SQL"])
    with arquivos:
        _aba_arquivos(objetos, con, raiz)
    with particoes_:
        _aba_particoes(objetos, datasets, con)
    with sql:
        _aba_sql(colunas, con)


def main() -> None:
    try:
        _pagina()
    except (StopException, RerunException):
        raise
    except Exception as erro:
        st.error("Algo deu errado no explorador. Clique em Recarregar; se continuar, veja `docker compose logs explorador`.")
        st.caption(f"Detalhe técnico: {type(erro).__name__}: {erro}")


main()
