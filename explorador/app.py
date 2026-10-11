"""Console do datalake: telas no estilo do S3 (buckets e objetos) e do Athena (editor de consultas).

    docker compose up -d explorador      # e abra http://localhost:8501

Somente leitura: nada aqui grava ou apaga no bucket. É uma ferramenta de estudo
que imita o fluxo do console da AWS; não usa nenhuma marca nem serviço da AWS.
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import pandas as pd
import streamlit as st

from streamlit.runtime.scriptrunner_utils.exceptions import RerunException, StopException

from explorador import athena, consultas, glue, particoes, s3

TELA_S3 = "S3 · Buckets"
TELA_CATALOGO = "Glue · Data Catalog"
TELA_JOBS = "Glue · Jobs ETL"
TELA_ATHENA = "Athena · Editor de consultas"

EXEMPLOS = {
    "Eventos por nome (landing)": (
        "SELECT event_name, count(*) AS eventos\nFROM landing.eventos\n"
        "GROUP BY event_name\nORDER BY eventos DESC"
    ),
    "Linhas por dia (Silver)": (
        "SELECT year, month, day, count(*) AS linhas\nFROM silver.eventos\nGROUP BY ALL\nORDER BY ALL"
    ),
    "Colunas de uma tabela": "DESCRIBE silver.eventos",
}

# Cores e formas inspiradas no console: barra escura, destaque laranja, links azuis, tabelas com cabeçalho cinza.
CSS = """
<style>
  /* o cabeçalho do Streamlit fica transparente, logo abaixo da barra: some a barra de ferramentas,
     mas ficam os botões de abrir/fechar o menu lateral (importantes em telas estreitas) */
  [data-testid="stHeader"] { background: transparent !important; top: 44px !important; height: 2.5rem !important; }
  [data-testid="stToolbarActions"], [data-testid="stMainMenu"], [data-testid="stAppDeployButton"],
  [data-testid="stDecoration"], #MainMenu, footer {
    display: none !important; }
  .block-container { padding-top: 3.6rem !important; }
  .barra-console { position: fixed; top: 0; left: 0; right: 0; z-index: 1000002; height: 44px;
    background: #232f3e; color: #fff; display: flex; align-items: center; gap: 16px; padding: 0 20px;
    font: 600 15px/1 "Amazon Ember", "Helvetica Neue", Arial, sans-serif; }
  .barra-console .marca { color: #ff9900; font-weight: 800; letter-spacing: .02em; }
  .barra-console .detalhe { margin-left: auto; color: #d1d5db; font-weight: 400; font-size: 13px; }
  .barra-console span { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  @media (max-width: 760px) { .barra-console span:not(.marca) { display: none; } }
  [data-testid="stSidebar"] { background: #f2f3f3; border-right: 1px solid #d5dbdb;
    top: 44px !important; height: calc(100vh - 44px) !important; }
  [data-testid="stHeaderActionElements"] { display: none; }
  [data-testid="stSidebarHeader"] { height: 2.2rem; padding: 0 8px; }
  h1, h2, h3 { color: #16191f; font-weight: 700 !important; }
  .stButton button[kind="primary"] { background: #ec7211; border-color: #ec7211; color: #fff; border-radius: 2px; }
  .stButton button[kind="primary"]:hover { background: #eb5f07; border-color: #eb5f07; }
  .stButton button[kind="secondary"] { border-radius: 2px; border-color: #545b64; color: #545b64; }
  .stButton button[kind="tertiary"] { color: #0073bb; padding: 0 2px; min-height: 0; }
  .stButton button[kind="tertiary"]:hover { color: #0a4a74; text-decoration: underline; }
  .cabecalho-tabela { background: #fafafa; border-top: 1px solid #eaeded; border-bottom: 1px solid #eaeded;
    font-weight: 700; color: #545b64; font-size: 14px; padding: 6px 0; }
  .linha-tabela { border-bottom: 1px solid #eaeded; }
  .stTextArea textarea { font-family: Menlo, Consolas, "Courier New", monospace; font-size: 14px;
    background: #fbfbfb; }
  .painel { border: 1px solid #d5dbdb; border-radius: 2px; padding: 12px 16px; background: #fff; }
</style>
"""


@st.cache_data(ttl=30, show_spinner="Listando o bucket…")
def _listar() -> list[particoes.Objeto]:
    return s3.listar()


def _fechar(valor) -> None:
    """Fecha a conexão DuckDB quando ela sai do cache (Recarregar ou datasets novos)."""
    valor[0].close()


def _mostrar_erro(titulo: str, erro: Exception) -> None:
    st.error(f"{titulo} {consultas.explicar_erro(erro)}")
    st.caption(f"Mensagem original do DuckDB: {erro}")


@st.cache_resource(max_entries=2, on_release=_fechar, show_spinner="Conectando ao S3 e criando as tabelas…")
def _conexao(datasets: tuple[particoes.Dataset, ...], raiz: str):
    con = consultas.conectar(endpoint=s3.ENDPOINT, chave=s3.CHAVE, segredo=s3.SEGREDO, regiao=s3.REGIAO)
    views = consultas.criar_views(con, raiz, list(datasets))
    colunas = {}
    for nome in views:
        descricao = con.sql(f"DESCRIBE {particoes.sql_nome(nome)}").df()
        colunas[nome] = list(zip(descricao["column_name"], descricao["column_type"]))
    consultas.travar(con, raiz)
    return con, colunas


# ------------------------------------------------------------------ S3 ------------------------------------------------------------------

def _ir_para(pasta: str | None) -> None:
    """None = lista de buckets; "" = raiz do bucket; "silver/…/" = uma pasta."""
    st.session_state["bucket_aberto"] = pasta is not None
    st.session_state["pasta"] = pasta or ""
    st.session_state.pop("arquivo", None)


def _cabecalho(colunas, titulos) -> None:
    for coluna, titulo in zip(colunas, titulos):
        coluna.markdown(f'<div class="cabecalho-tabela">{titulo}</div>', unsafe_allow_html=True)


def _lista_de_buckets(objetos) -> None:
    st.caption("Buckets")
    st.subheader(f"Buckets (1)")
    st.caption("Os buckets são contêineres de objetos armazenados no S3. Aqui há um só, emulado pelo floci.")
    pesos = [3, 2, 1, 1]
    _cabecalho(st.columns(pesos), ["Nome", "Região", "Objetos", "Tamanho"])
    nome, regiao, qtd, tamanho = st.columns(pesos)
    nome.button(f"🪣 {s3.BUCKET}", key="abrir_bucket", type="tertiary", on_click=_ir_para, args=("",))
    regiao.write(s3.REGIAO)
    qtd.write(str(len(objetos)))
    tamanho.write(particoes.tamanho_legivel(sum(o.tamanho for o in objetos)))


def _breadcrumb(pasta: str) -> None:
    trilha = [("Buckets", "migalha:buckets", None), (s3.BUCKET, "migalha:", "")]
    trilha += [(rotulo, f"migalha:{caminho}", caminho) for rotulo, caminho in particoes.migalhas(pasta)]
    linha = st.container(horizontal=True, vertical_alignment="center", gap="small")
    for i, (rotulo, chave, destino) in enumerate(trilha):
        linha.button(rotulo, key=chave, type="tertiary", on_click=_ir_para, args=(destino,))
        if i < len(trilha) - 1:
            linha.markdown("›", width="content")


def _tipo(chave: str) -> str:
    return particoes.formato_do_arquivo(chave) or (chave.rsplit(".", 1)[-1] if "." in chave.rsplit("/", 1)[-1] else "—")


def _aba_objetos(objetos, con, raiz, pasta: str) -> None:
    subpastas, arquivos = particoes.navegar(objetos, pasta)
    st.subheader(f"Objetos ({len(subpastas) + len(arquivos)})")
    if not subpastas and not arquivos:
        st.info("Nenhum objeto nesta pasta.")
        return
    pesos = [4, 1, 2, 1]
    _cabecalho(st.columns(pesos), ["Nome", "Tipo", "Última modificação", "Tamanho"])
    for sub in subpastas:
        nome, tipo, data, tamanho = st.columns(pesos)
        nome.button(f"📁 {sub}/", key=f"obj:{pasta}{sub}/", type="tertiary", on_click=_ir_para, args=(f"{pasta}{sub}/",))
        tipo.write("Pasta")
        data.write("—")
        tamanho.write("—")
    for o in arquivos:
        nome, tipo, data, tamanho = st.columns(pesos)
        nome.button(f"📄 {o.chave[len(pasta):]}", key=f"obj:{o.chave}", type="tertiary",
                    on_click=st.session_state.__setitem__, args=("arquivo", o.chave))
        tipo.write(_tipo(o.chave))
        data.write(o.modificado.strftime("%d/%m/%Y %H:%M:%S") if o.modificado else "—")
        tamanho.write(particoes.tamanho_legivel(o.tamanho))

    arquivo = st.session_state.get("arquivo")
    if arquivo and arquivo.startswith(pasta):
        st.divider()
        st.subheader(f"📄 {arquivo.rsplit('/', 1)[-1]}")
        st.caption(f"s3://{s3.BUCKET}/{arquivo}")
        if particoes.formato_do_arquivo(arquivo) is None:
            st.info("Prévia só para arquivos .parquet e .jsonl.")
            return
        try:
            st.dataframe(consultas.previa(con.cursor(), raiz, arquivo), hide_index=True)
            st.caption(f"Primeiras {consultas.LINHAS_PREVIA} linhas, no máximo.")
        except ValueError as erro:
            st.warning(str(erro))
        except duckdb.Error as erro:
            _mostrar_erro("Não consegui ler o arquivo.", erro)


def _aba_particoes(objetos, datasets, con) -> None:
    if not datasets:
        st.info("Ainda não há datasets (pastas com .parquet ou .jsonl) no bucket.")
        return
    repetidos = {d.prefixo for d in datasets if sum(o.prefixo == d.prefixo for o in datasets) > 1}
    rotulos = {d.nome_view: d.prefixo + (f" ({d.formato})" if d.prefixo in repetidos else "") for d in datasets}
    nome = st.selectbox("Dataset", list(rotulos), format_func=rotulos.get, key="dataset")
    dataset = next(d for d in datasets if d.nome_view == nome)
    linhas = particoes.tabela_particoes(objetos, dataset.prefixo, dataset.formato)
    if not linhas:
        st.info("Nenhum arquivo de dados neste dataset.")
        return
    tabela = pd.DataFrame([asdict(l) for l in linhas])
    tabela["tamanho"] = tabela["bytes"].map(particoes.tamanho_legivel)
    st.dataframe(tabela, hide_index=True)
    st.caption(f"{len(linhas)} partição(ões) · {int(tabela['arquivos'].sum())} arquivo(s) · "
               f"{particoes.tamanho_legivel(int(tabela['bytes'].sum()))}")
    if st.button("Contar linhas por partição", key="contar"):
        campos = [c for c in particoes.CAMPOS if any(getattr(l, c) != particoes.SEM_PARTICAO for l in linhas)]
        sql = (f'SELECT {", ".join(campos)}, count(*) AS linhas FROM {particoes.sql_nome(nome)} GROUP BY ALL ORDER BY ALL'
               if campos else f'SELECT count(*) AS linhas FROM {particoes.sql_nome(nome)}')
        try:
            with st.spinner("Contando linhas… (lê todos os arquivos do dataset)"):
                contagem = consultas.executar(con.cursor(), sql, consultas.LIMITE_MAXIMO).tabela
            st.dataframe(contagem, hide_index=True)
        except duckdb.Error as erro:
            _mostrar_erro("Não consegui contar.", erro)


def _aba_propriedades(objetos, datasets) -> None:
    st.subheader("Visão geral do bucket")
    dados = {
        "Nome": s3.BUCKET,
        "Região": s3.REGIAO,
        "Endpoint": s3.ENDPOINT,
        "Objetos": str(len(objetos)),
        "Tamanho total": particoes.tamanho_legivel(sum(o.tamanho for o in objetos)),
        "Tabelas no editor de consultas": str(len(datasets)),
        "Acesso": "somente leitura",
    }
    st.dataframe(pd.DataFrame({"Propriedade": list(dados), "Valor": list(dados.values())}), hide_index=True)


def _tela_s3(objetos, datasets, con, raiz) -> None:
    if not st.session_state.get("bucket_aberto"):
        _lista_de_buckets(objetos)
        return
    pasta = st.session_state.get("pasta", "")
    _breadcrumb(pasta)
    st.header(s3.BUCKET)
    aba_objetos, aba_particoes, aba_propriedades = st.tabs(["Objetos", "Partições", "Propriedades"])
    with aba_objetos:
        _aba_objetos(objetos, con, raiz, pasta)
    with aba_particoes:
        _aba_particoes(objetos, datasets, con)
    with aba_propriedades:
        _aba_propriedades(objetos, datasets)


# ---------------------------------------------------------------- Athena ----------------------------------------------------------------

def _abas() -> list[athena.Aba]:
    if "abas" not in st.session_state:
        st.session_state["abas"] = athena.nova_aba([])
        st.session_state["indice_aba"] = 0
    return st.session_state["abas"]


def _aba_ativa() -> athena.Aba:
    return _abas()[st.session_state.get("indice_aba", 0)]


def _chave_sql(aba: athena.Aba) -> str:
    return f"sql:{aba.nome}"


def _por_no_editor(texto: str, substituir: bool = False) -> None:
    aba = _aba_ativa()
    atual = st.session_state.get(_chave_sql(aba), aba.sql)
    aba.sql = texto if substituir else athena.inserir(atual, texto)
    st.session_state[_chave_sql(aba)] = aba.sql


def _nova_aba() -> None:
    st.session_state["abas"] = athena.nova_aba(_abas())
    st.session_state["indice_aba"] = len(st.session_state["abas"]) - 1


def _fechar_aba() -> None:
    abas, indice = athena.fechar_aba(_abas(), st.session_state.get("indice_aba", 0))
    st.session_state["abas"], st.session_state["indice_aba"] = abas, indice


def _trocar_aba() -> None:
    nomes = [a.nome for a in _abas()]
    st.session_state["indice_aba"] = nomes.index(st.session_state["aba_ativa"])


def _arvore(datasets, colunas) -> None:
    st.markdown("**Fonte de dados**")
    st.caption("Data Catalog de estudo · floci (S3 local)")
    bancos = glue.bancos(datasets)
    banco = st.selectbox("Banco de dados", list(bancos), key="athena_banco")
    nomes = [d.nome_view for d in bancos.get(banco, []) if d.nome_view in colunas]
    st.markdown(f"**Tabelas ({len(nomes)})**")
    for nome in nomes:
        cols = colunas[nome]
        with st.expander(f"▦ {particoes.banco_e_tabela(nome)[1]}"):
            st.button("Inserir no editor", key=f"inserir:{nome}", type="tertiary", on_click=_por_no_editor, args=(nome,))
            st.markdown("\n".join(f"- `{c}` <span style='color:#687078'>{t.lower()}</span>" for c, t in cols),
                        unsafe_allow_html=True)
    st.markdown("**Consultas de exemplo**")
    for titulo, sql in EXEMPLOS.items():
        st.button(titulo, key=f"exemplo:{titulo}", type="tertiary", on_click=_por_no_editor, args=(sql, True))


def _executar(con, aba: athena.Aba, objetos, datasets, limite: int) -> None:
    sql = st.session_state.get(_chave_sql(aba), "")
    aba.sql, aba.limite, aba.erro_original = sql, limite, None
    inicio, quando = time.perf_counter(), datetime.now().strftime("%H:%M:%S")
    try:
        with st.spinner("Executando a consulta…"):
            resultado = consultas.executar(con.cursor(), sql, limite)
        lidos = athena.bytes_lidos(athena.views_usadas(sql, [d.nome_view for d in datasets]), datasets, objetos, sql)
        aba.resultado = resultado
        aba.execucao = athena.Execucao(sql, "Concluída", quando, time.perf_counter() - inicio,
                                       len(resultado.tabela), lidos)
    except consultas.SqlNaoPermitido as erro:
        aba.resultado = None
        aba.execucao = athena.Execucao(sql, "Falhou", quando, time.perf_counter() - inicio, 0, 0, str(erro))
    except duckdb.Error as erro:
        aba.resultado = None
        aba.execucao = athena.Execucao(sql, "Falhou", quando, time.perf_counter() - inicio, 0, 0,
                                       consultas.explicar_erro(erro))
        aba.erro_original = str(erro)
    st.session_state["historico"] = athena.registrar(st.session_state.get("historico", []), aba.execucao)


def _resultados(aba: athena.Aba) -> None:
    if aba.execucao is None:
        st.caption("Execute uma consulta para ver os resultados aqui.")
        return
    if aba.execucao.status == "Concluída":
        st.success(athena.resumo(aba.execucao))
    else:
        st.error(athena.resumo(aba.execucao))
        if aba.erro_original:
            st.caption(f"Mensagem original do DuckDB: {aba.erro_original}")
        return
    resultado = aba.resultado
    if resultado.cortado:
        st.warning(f"Resultado cortado em {int(aba.limite)} linhas. Aumente o limite ou filtre a consulta.")
    st.dataframe(resultado.tabela, hide_index=True)
    st.download_button("Baixar resultados (CSV)", resultado.tabela.to_csv(index=False).encode("utf-8"),
                       file_name="resultado.csv", mime="text/csv", key="baixar", on_click="ignore")


def _recentes() -> None:
    historico = st.session_state.get("historico", [])
    if not historico:
        st.caption("Nenhuma consulta executada nesta sessão do navegador.")
        return
    st.dataframe(pd.DataFrame([{
        "Hora": e.quando, "Status": e.status, "Tempo (s)": round(e.segundos, 2), "Linhas": e.linhas,
        "Dados lidos (estimado)": particoes.tamanho_legivel(e.bytes), "Consulta": " ".join(e.sql.split())[:120],
    } for e in historico]), hide_index=True)
    st.caption("Abrir uma consulta no editor:")
    for i, e in enumerate(historico):
        st.button(f"{e.quando} · {' '.join(e.sql.split())[:80]}", key=f"abrir_hist:{i}", type="tertiary",
                  on_click=_por_no_editor, args=(e.sql, True))
    st.caption("O histórico vale enquanto esta aba do navegador estiver aberta; recarregar a página zera.")


def _tela_athena(objetos, datasets, con, colunas) -> None:
    st.header("Editor de consultas")
    esquerda, direita = st.columns([1, 3])
    with esquerda:
        _arvore(datasets, colunas)
    with direita:
        abas = _abas()
        nomes = [a.nome for a in abas]
        aba = _aba_ativa()
        st.session_state["aba_ativa"] = aba.nome
        seletor, mais, fechar = st.columns([8, 1, 1])
        seletor.radio("Consultas abertas", nomes, key="aba_ativa", horizontal=True,
                      label_visibility="collapsed", on_change=_trocar_aba)
        mais.button("＋", key="nova_aba", help="Nova consulta", on_click=_nova_aba)
        fechar.button("✕", key="fechar_aba", help="Fechar esta consulta", on_click=_fechar_aba,
                      disabled=len(abas) == 1)
        if _chave_sql(aba) not in st.session_state:
            st.session_state[_chave_sql(aba)] = aba.sql
        st.text_area("Consulta", key=_chave_sql(aba), height=200, label_visibility="collapsed",
                     placeholder="SELECT * FROM silver.eventos LIMIT 10")
        executar, limite_col, _ = st.columns([1, 2, 3])
        limite = limite_col.number_input("Limite de linhas", min_value=1, max_value=consultas.LIMITE_MAXIMO,
                                         value=aba.limite, step=100, key=f"limite:{aba.nome}")
        if executar.button("Executar", key="executar", type="primary"):
            _executar(con, aba, objetos, datasets, limite)
        aba_resultados, aba_recentes = st.tabs(["Resultados", "Consultas recentes"])
        with aba_resultados:
            _resultados(aba)
        with aba_recentes:
            _recentes()


# ------------------------------------------------------------ Glue · Data Catalog ------------------------------------------------------------

def _escolher(**valores) -> None:
    for chave, valor in valores.items():
        st.session_state[chave] = valor


def _consultar_no_athena(nome: str) -> None:
    """Botão "Consultar no Athena": abre uma aba nova no editor com um SELECT da tabela."""
    _nova_aba()
    _por_no_editor(f"SELECT * FROM {nome} LIMIT 10", substituir=True)
    st.session_state["tela"] = TELA_ATHENA


def _tela_catalogo(objetos, datasets, colunas) -> None:
    bancos = glue.bancos(datasets)
    banco = st.session_state.get("cat_banco")
    tabela = st.session_state.get("cat_tabela")
    trilha = st.container(horizontal=True, vertical_alignment="center", gap="small")
    trilha.button("Data Catalog", key="cat:inicio", type="tertiary", on_click=_escolher,
                  kwargs={"cat_banco": None, "cat_tabela": None})
    if banco:
        trilha.markdown("›", width="content")
        trilha.button(banco, key="cat:banco", type="tertiary", on_click=_escolher, kwargs={"cat_tabela": None})
    if tabela:
        trilha.markdown("›", width="content")
        trilha.markdown(particoes.banco_e_tabela(tabela)[1], width="content")

    if not banco or banco not in bancos:
        st.header(f"Bancos de dados ({len(bancos)})")
        st.caption("No Glue, um banco de dados agrupa tabelas. Aqui há um por camada do medalhão.")
        pesos = [2, 1, 3]
        _cabecalho(st.columns(pesos), ["Nome", "Tabelas", "Localização"])
        for nome, tabelas in bancos.items():
            c_nome, c_qtd, c_local = st.columns(pesos)
            c_nome.button(f"🗄 {nome}", key=f"banco:{nome}", type="tertiary", on_click=_escolher,
                          kwargs={"cat_banco": nome, "cat_tabela": None})
            c_qtd.write(str(len(tabelas)))
            c_local.write(f"s3://{s3.BUCKET}/{nome}/")
        return

    if not tabela:
        st.header(f"Tabelas em {banco} ({len(bancos[banco])})")
        pesos = [3, 1, 4, 1, 1]
        _cabecalho(st.columns(pesos), ["Nome", "Formato", "Localização", "Partições", "Colunas"])
        for d in bancos[banco]:
            c_nome, c_fmt, c_local, c_part, c_cols = st.columns(pesos)
            c_nome.button(particoes.banco_e_tabela(d.nome_view)[1], key=f"tabela:{d.nome_view}", type="tertiary",
                          on_click=_escolher, kwargs={"cat_tabela": d.nome_view})
            c_fmt.write("Parquet" if d.formato == "parquet" else "JSON")
            c_local.write(glue.localizacao(d, s3.BUCKET))
            c_part.write(str(len(particoes.tabela_particoes(objetos, d.prefixo, d.formato))))
            c_cols.write(str(len(colunas.get(d.nome_view, []))))
        return

    dataset = next(d for d in datasets if d.nome_view == tabela)
    chaves = glue.chaves_particao(dataset, objetos)
    titulo, botao = st.columns([4, 1], vertical_alignment="center")
    titulo.header(tabela)
    botao.button("Consultar no Athena", key="consultar_athena", type="primary",
                 on_click=_consultar_no_athena, args=(tabela,))
    aba_esquema, aba_particoes, aba_props = st.tabs(["Esquema", "Partições", "Propriedades"])
    with aba_esquema:
        st.dataframe(pd.DataFrame([{"Coluna": c, "Tipo": t.lower(), "Chave de partição": "sim" if c in chaves else ""}
                                   for c, t in colunas.get(tabela, [])]), hide_index=True)
    with aba_particoes:
        linhas = particoes.tabela_particoes(objetos, dataset.prefixo, dataset.formato)
        if chaves:
            st.caption(f"Chaves de partição: {', '.join(chaves)}")
            st.dataframe(pd.DataFrame([{**{c: getattr(l, c) for c in chaves}, "arquivos": l.arquivos,
                                        "tamanho": particoes.tamanho_legivel(l.bytes)} for l in linhas]), hide_index=True)
        else:
            st.info("Tabela sem partições: todos os arquivos ficam direto na pasta.")
    with aba_props:
        dados = {"Banco de dados": banco, "Tabela": particoes.banco_e_tabela(tabela)[1],
                 "Localização": glue.localizacao(dataset, s3.BUCKET),
                 "Classificação": "parquet" if dataset.formato == "parquet" else "json",
                 "Chaves de partição": ", ".join(chaves) or "—",
                 "Arquivos": str(sum(l.arquivos for l in particoes.tabela_particoes(objetos, dataset.prefixo, dataset.formato))),
                 "Nome no Athena": tabela}
        st.dataframe(pd.DataFrame({"Propriedade": list(dados), "Valor": list(dados.values())}), hide_index=True)


# -------------------------------------------------------------- Glue · Jobs ETL --------------------------------------------------------------

def _pastas_jobs() -> tuple[Path, Path, Path]:
    raiz = Path(__file__).resolve().parents[1]
    jobs = Path(os.environ.get("EXPLORADOR_JOBS", "pipeline"))
    execucoes = Path(os.environ.get("EXPLORADOR_EXECUCOES", "/tmp/explorador-execucoes"))
    return raiz, (jobs if jobs.is_absolute() else raiz / jobs), execucoes


def _quando(instante: float) -> str:
    return datetime.fromtimestamp(instante).strftime("%d/%m %H:%M:%S")


def _executar_job(job: glue.Job, pasta: Path, raiz: Path) -> None:
    if not glue.em_execucao(pasta, job.nome):
        st.session_state["execucao_sel"] = glue.iniciar(job, pasta, raiz)


def _painel_execucoes(job: glue.Job, pasta: Path, rodando_antes: bool = False) -> None:
    if rodando_antes and not glue.em_execucao(pasta, job.nome):
        st.rerun()                                  # terminou: redesenha a página toda (reativa o botão Executar)
    execucoes = glue.execucoes(pasta, job.nome)
    if not execucoes:
        st.caption("Nenhuma execução ainda. Clique em Executar.")
        return
    st.dataframe(pd.DataFrame([{"Execução": e.id, "Status": e.status, "Início": _quando(e.inicio),
                                "Duração (s)": round(e.duracao, 1), "Código de saída": "—" if e.codigo is None else e.codigo}
                               for e in execucoes]), hide_index=True)
    ids = [e.id for e in execucoes]
    if st.session_state.get("execucao_sel") not in ids:
        st.session_state["execucao_sel"] = ids[0]
    escolhida = st.selectbox("Log da execução", ids, key="execucao_sel")
    st.code(glue.log(pasta, escolhida, ultimas=300) or "(sem saída ainda)", language="text")


def _tela_jobs() -> None:
    raiz, pasta_jobs, pasta_exec = _pastas_jobs()
    jobs = glue.listar_jobs(pasta_jobs)
    nome = st.session_state.get("job_sel")
    job = next((j for j in jobs if j.nome == nome), None)
    trilha = st.container(horizontal=True, vertical_alignment="center", gap="small")
    trilha.button("Jobs ETL", key="jobs:inicio", type="tertiary", on_click=_escolher, kwargs={"job_sel": None})
    if job:
        trilha.markdown("›", width="content")
        trilha.markdown(job.nome, width="content")

    if job is None:
        st.header(f"Jobs ({len(jobs)})")
        st.caption(f"Os scripts .py de `{pasta_jobs.relative_to(raiz) if pasta_jobs.is_relative_to(raiz) else pasta_jobs}/` "
                   "(menos o comum.py). Executar aqui é o mesmo que `docker compose run --rm app python <script>`.")
        if not jobs:
            st.info("Nenhum job ainda. Crie o seu primeiro, por exemplo `pipeline/bronze.py`, e clique em Recarregar.")
            return
        pesos = [2, 3, 1, 2]
        _cabecalho(st.columns(pesos), ["Nome", "Script", "Última execução", "Início"])
        for j in jobs:
            ultima = next(iter(glue.execucoes(pasta_exec, j.nome)), None)
            c_nome, c_script, c_status, c_inicio = st.columns(pesos)
            c_nome.button(f"⚙ {j.nome}", key=f"job:{j.nome}", type="tertiary", on_click=_escolher,
                          kwargs={"job_sel": j.nome})
            c_script.write(str(j.script.relative_to(raiz)) if j.script.is_relative_to(raiz) else str(j.script))
            c_status.write(ultima.status if ultima else "—")
            c_inicio.write(_quando(ultima.inicio) if ultima else "—")
        return

    rodando = glue.em_execucao(pasta_exec, job.nome)
    titulo, botao = st.columns([4, 1], vertical_alignment="center")
    titulo.header(job.nome)
    botao.button("Executar", key="executar_job", type="primary", disabled=rodando,
                 on_click=_executar_job, args=(job, pasta_exec, raiz))
    st.caption("Tipo: Spark (PySpark 3.5.3, local) · roda no contêiner do explorador · um job por vez")
    aba_exec, aba_script = st.tabs(["Execuções", "Script"])
    with aba_exec:
        # enquanto há execução em andamento, o painel se atualiza sozinho a cada 3 s
        st.fragment(run_every=3 if rodando else None)(_painel_execucoes)(job, pasta_exec, rodando)
    with aba_script:
        st.code(job.script.read_text(encoding="utf-8"), language="python")


# ---------------------------------------------------------------- página ----------------------------------------------------------------

def _pagina() -> None:
    st.set_page_config(page_title="Console do datalake", layout="wide", initial_sidebar_state="expanded")
    st.markdown(CSS, unsafe_allow_html=True)
    st.markdown(f'<div class="barra-console"><span class="marca">☁ Console do datalake</span>'
                f'<span>S3, Glue e Athena de estudo</span><span class="detalhe">{s3.BUCKET} · {s3.ENDPOINT} · '
                f'Athena só lê · Jobs ETL executam {os.environ.get("EXPLORADOR_JOBS", "pipeline").rstrip("/")}/'
                f'</span></div>', unsafe_allow_html=True)
    with st.sidebar:
        st.markdown("**Serviços**")
        tela = st.radio("Serviço", [TELA_S3, TELA_CATALOGO, TELA_JOBS, TELA_ATHENA], key="tela",
                        label_visibility="collapsed")
        st.divider()
        if st.button("Recarregar", key="recarregar", help="Lê o bucket de novo e recria as tabelas"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()
        st.caption("Ferramenta de estudo inspirada nos consoles do S3, do Glue e do Athena. Roda no seu computador.")

    if tela == TELA_JOBS:                      # os jobs não dependem do bucket já ter dados
        _tela_jobs()
        return

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
        _mostrar_erro("Não consegui preparar as tabelas no DuckDB.", erro)
        st.stop()

    if tela == TELA_S3:
        _tela_s3(objetos, datasets, con, raiz)
    elif tela == TELA_CATALOGO:
        _tela_catalogo(objetos, datasets, colunas)
    else:
        _tela_athena(objetos, datasets, con, colunas)


def main() -> None:
    try:
        _pagina()
    except (StopException, RerunException):
        raise
    except Exception as erro:
        st.error("Algo deu errado no explorador. Clique em Recarregar; se continuar, veja `docker compose logs explorador`.")
        st.caption(f"Detalhe técnico: {type(erro).__name__}: {erro}")


main()
