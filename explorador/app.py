"""Console do datalake: telas no estilo do S3, do Glue (Data Catalog e Jobs ETL) e do Athena.

    docker compose up -d explorador      # e abra http://localhost:8501

S3, Data Catalog e Athena só leem. Jobs ETL roda os scripts de pipeline/ (o mesmo que o
terminal faz). É uma ferramenta de estudo que imita o fluxo do console da AWS; não usa
nenhuma marca nem serviço da AWS.
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

from explorador import athena, consultas, glue, particoes, preferencias, s3, tutorial, ui
from explorador.tutorial import TELA_ATHENA, TELA_CATALOGO, TELA_JOBS, TELA_S3

RAIZ = Path(__file__).resolve().parents[1]
ICONES = {TELA_S3: "🪣  Buckets", TELA_CATALOGO: "🗂  Data Catalog", TELA_JOBS: "⚙  Jobs ETL",
          TELA_ATHENA: "🔎  Editor de consultas"}

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


def _escolher(**valores) -> None:
    for chave, valor in valores.items():
        st.session_state[chave] = valor


# ------------------------------------------------------------------ S3 ------------------------------------------------------------------

def _ir_para(pasta: str | None) -> None:
    """None = lista de buckets; "" = raiz do bucket; "silver/…/" = uma pasta."""
    st.session_state["bucket_aberto"] = pasta is not None
    st.session_state["pasta"] = pasta or ""
    st.session_state.pop("arquivo", None)


def _lista_de_buckets(objetos) -> None:
    _contexto("s3-buckets")
    ui.trilha([("Buckets", None, None, {})], "s3")
    ui.titulo("Buckets", 1, "Os buckets são contêineres de objetos armazenados no S3. Aqui há um só, emulado pelo floci.")
    with ui.painel("buckets", "Buckets", 1):
        ui.tabela("buckets", [("Nome", 3), ("Região", 2), ("Objetos", 1), ("Tamanho", 1)], [[
            ui.Link(s3.BUCKET, "abrir_bucket", _ir_para, ("",)),
            s3.REGIAO, str(len(objetos)), particoes.tamanho_legivel(sum(o.tamanho for o in objetos))]])


def _tipo(chave: str) -> str:
    nome = chave.rsplit("/", 1)[-1]
    return particoes.formato_do_arquivo(chave) or (nome.rsplit(".", 1)[-1] if "." in nome else "—")


def _aba_objetos(objetos, con, raiz, pasta: str) -> None:
    subpastas, arquivos = particoes.navegar(objetos, pasta)
    linhas = [[ui.Link(f"📁 {sub}/", f"obj:{pasta}{sub}/", _ir_para, (f"{pasta}{sub}/",)), "Pasta", "—", "—"]
              for sub in subpastas]
    linhas += [[ui.Link(f"📄 {o.chave[len(pasta):]}", f"obj:{o.chave}", _escolher, kwargs={"arquivo": o.chave}),
                _tipo(o.chave), o.modificado.strftime("%d/%m/%Y %H:%M:%S") if o.modificado else "—",
                particoes.tamanho_legivel(o.tamanho)] for o in arquivos]
    with ui.painel("objetos", "Objetos", len(linhas),
                   "Objetos são as entidades fundamentais guardadas no S3. Clique numa pasta para entrar ou num arquivo para ver a prévia."):
        ui.tabela("objetos", [("Nome", 5), ("Tipo", 1), ("Última modificação", 2), ("Tamanho", 1)], linhas,
                  localizar=len(linhas) > 8, vazio="Nenhum objeto nesta pasta.")

    arquivo = st.session_state.get("arquivo")
    if arquivo and arquivo.startswith(pasta):
        with ui.painel("previa", f"Prévia: {arquivo.rsplit('/', 1)[-1]}", descricao=f"s3://{s3.BUCKET}/{arquivo}"):
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
    with ui.painel("particoes", "Partições", descricao="Uma linha por partição: quantos arquivos e quantos bytes ela tem."):
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
    with ui.painel("bucket-visao", "Visão geral do bucket"):
        ui.pares({
            "Nome": s3.BUCKET, "Região": s3.REGIAO, "Endpoint": s3.ENDPOINT, "Objetos": str(len(objetos)),
            "Tamanho total": particoes.tamanho_legivel(sum(o.tamanho for o in objetos)),
            "Tabelas no catálogo": str(len(datasets)), "Acesso": "somente leitura",
        })


def _tela_s3(objetos, datasets, con, raiz) -> None:
    if not st.session_state.get("bucket_aberto"):
        _lista_de_buckets(objetos)
        return
    _contexto("s3-bucket")
    pasta = st.session_state.get("pasta", "")
    trilha = [("Buckets", "migalha:buckets", _ir_para, {"pasta": None}),
              (s3.BUCKET, "migalha:", _ir_para, {"pasta": ""})]
    trilha += [(rotulo, f"migalha:{caminho}", _ir_para, {"pasta": caminho}) for rotulo, caminho in particoes.migalhas(pasta)]
    ui.trilha(trilha, "s3")
    ui.titulo(s3.BUCKET)
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


def _abrir_no_athena(sql: str) -> None:
    """Abre uma aba nova no editor com esta SQL e troca para a tela do Athena."""
    _nova_aba()
    _por_no_editor(sql, substituir=True)
    st.session_state["tela"] = TELA_ATHENA


def _arvore(datasets, colunas) -> None:
    with ui.painel("athena-dados", "Dados"):
        st.caption("Fonte de dados: Data Catalog de estudo · floci (S3 local)")
        bancos = glue.bancos(datasets)
        banco = st.selectbox("Banco de dados", list(bancos), key="athena_banco")
        nomes = [d.nome_view for d in bancos.get(banco, []) if d.nome_view in colunas]
        st.markdown(f'<div class="painel-titulo" style="font-size:.9375rem">Tabelas <span class="contador">({len(nomes)})</span></div>',
                    unsafe_allow_html=True)
        for nome in nomes:
            with st.expander(f"▦ {particoes.banco_e_tabela(nome)[1]}"):
                st.button("Inserir no editor", key=f"inserir:{nome}", type="tertiary", on_click=_por_no_editor, args=(nome,))
                for c, t in colunas[nome]:
                    # struct aparece curto; o clique no tipo abre a tipagem interna num overlay
                    with st.container(horizontal=True, vertical_alignment="center", gap="small", key=f"arvcol-{nome}-{c}"):
                        st.markdown(f"`{c}`", width="content")
                        ui.tipo(st, c, t, f"arvore-{nome}-{c}")
    with ui.painel("athena-exemplos", "Consultas de exemplo"):
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
        st.markdown('<div class="tab-vazio">Execute uma consulta para ver os resultados aqui.</div>', unsafe_allow_html=True)
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
        st.markdown('<div class="tab-vazio">Nenhuma consulta executada nesta sessão do navegador.</div>',
                    unsafe_allow_html=True)
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
    _contexto("athena")
    ui.trilha([("Athena", None, None, {}), ("Editor de consultas", None, None, {})], "athena")
    ui.titulo("Editor de consultas")
    esquerda, direita = st.columns([1, 3], gap="medium")
    with esquerda:
        _arvore(datasets, colunas)
    with direita:
        abas = _abas()
        nomes = [a.nome for a in abas]
        aba = _aba_ativa()
        st.session_state["aba_ativa"] = aba.nome
        with ui.painel("athena-editor"):
            seletor, mais, fechar = st.columns([10, 1, 1], vertical_alignment="center")
            seletor.radio("Consultas abertas", nomes, key="aba_ativa", horizontal=True,
                          label_visibility="collapsed", on_change=_trocar_aba)
            mais.button("＋", key="nova_aba", help="Nova consulta", on_click=_nova_aba)
            fechar.button("✕", key="fechar_aba", help="Fechar esta consulta", on_click=_fechar_aba,
                          disabled=len(abas) == 1)
            if _chave_sql(aba) not in st.session_state:
                st.session_state[_chave_sql(aba)] = aba.sql
            st.text_area("Consulta", key=_chave_sql(aba), height=220, label_visibility="collapsed",
                         placeholder="SELECT * FROM silver.eventos LIMIT 10")
            executar, limite_col, _ = st.columns([1, 2, 3], vertical_alignment="bottom")
            limite = limite_col.number_input("Limite de linhas", min_value=1, max_value=consultas.LIMITE_MAXIMO,
                                             value=aba.limite, step=100, key=f"limite:{aba.nome}")
            if executar.button("Executar", key="executar", type="primary"):
                _executar(con, aba, objetos, datasets, limite)
        with ui.painel("athena-resultados"):
            aba_resultados, aba_recentes = st.tabs(["Resultados", "Consultas recentes"])
            with aba_resultados:
                _resultados(aba)
            with aba_recentes:
                _recentes()


# ------------------------------------------------------------ Glue · Data Catalog ------------------------------------------------------------

def _tela_catalogo(objetos, datasets, colunas) -> None:
    bancos = glue.bancos(datasets)
    banco = st.session_state.get("cat_banco")
    tabela = st.session_state.get("cat_tabela")
    if banco not in bancos:
        banco, tabela = None, None
    trilha = [("Glue", None, None, {}), ("Data Catalog", "cat:inicio", _escolher, {"cat_banco": None, "cat_tabela": None})]
    if banco:
        trilha.append((banco, "cat:banco", _escolher, {"cat_tabela": None}))
    if tabela:
        trilha.append((particoes.banco_e_tabela(tabela)[1], None, None, {}))
    ui.trilha(trilha if banco else trilha[:1] + [("Data Catalog", None, None, {})], "catalogo")

    if not banco:
        _contexto("catalogo-bancos")
        ui.titulo("Bancos de dados", len(bancos), "No Glue, um banco de dados agrupa tabelas. Aqui há um por camada do medalhão.")
        with ui.painel("bancos", "Bancos de dados", len(bancos)):
            ui.tabela("bancos", [("Nome", 2), ("Tabelas", 1), ("Localização", 3)], [
                [ui.Link(nome, f"banco:{nome}", _escolher, kwargs={"cat_banco": nome, "cat_tabela": None}),
                 str(len(tabelas)), f"s3://{s3.BUCKET}/{nome}/"] for nome, tabelas in bancos.items()])
        return

    if not tabela:
        _contexto("catalogo-tabelas")
        ui.titulo(banco, descricao=f"Banco de dados · s3://{s3.BUCKET}/{banco}/")
        linhas = [[ui.Link(particoes.banco_e_tabela(d.nome_view)[1], f"tabela:{d.nome_view}", _escolher,
                           kwargs={"cat_tabela": d.nome_view}),
                   "Parquet" if d.formato == "parquet" else "JSON", glue.localizacao(d, s3.BUCKET),
                   str(len(particoes.tabela_particoes(objetos, d.prefixo, d.formato))),
                   str(len(colunas.get(d.nome_view, [])))] for d in bancos[banco]]
        with ui.painel("tabelas", "Tabelas", len(linhas)):
            ui.tabela("tabelas", [("Nome", 3), ("Formato", 1), ("Localização", 4), ("Partições", 1), ("Colunas", 1)],
                      linhas, localizar=len(linhas) > 8)
        return

    _contexto("catalogo-tabela")
    dataset = next(d for d in datasets if d.nome_view == tabela)
    chaves = glue.chaves_particao(dataset, objetos)
    linhas_part = particoes.tabela_particoes(objetos, dataset.prefixo, dataset.formato)
    titulo, botao = st.columns([4, 1], vertical_alignment="center")
    with titulo:
        ui.titulo(tabela)
    botao.button("Consultar no Athena", key="consultar_athena", type="primary",
                 on_click=_abrir_no_athena, args=(f"SELECT * FROM {tabela} LIMIT 10",))
    with ui.painel("tabela-visao", "Visão geral da tabela"):
        ui.pares({"Banco de dados": banco, "Localização": glue.localizacao(dataset, s3.BUCKET),
                  "Classificação": "parquet" if dataset.formato == "parquet" else "json",
                  "Chaves de partição": ", ".join(chaves) or "—", "Arquivos": str(sum(l.arquivos for l in linhas_part)),
                  "Nome no Athena": tabela})
    aba_esquema, aba_particoes = st.tabs(["Esquema", "Partições"])
    with aba_esquema:
        # struct aparece curto; o clique no tipo abre a tipagem interna num overlay
        ui.tabela("esquema", [("Coluna", 2), ("Tipo", 2), ("Chave de partição", 1)],
                  [[c, ui.Tipo(c, t, f"esquema-{tabela}-{c}"), "sim" if c in chaves else ""]
                   for c, t in colunas.get(tabela, [])], localizar=len(colunas.get(tabela, [])) > 8)
    with aba_particoes:
        if chaves:
            st.caption(f"Chaves de partição: {', '.join(chaves)}")
            st.dataframe(pd.DataFrame([{**{c: getattr(l, c) for c in chaves}, "arquivos": l.arquivos,
                                        "tamanho": particoes.tamanho_legivel(l.bytes)} for l in linhas_part]),
                         hide_index=True)
        else:
            st.info("Tabela sem partições: todos os arquivos ficam direto na pasta.")


# -------------------------------------------------------------- Glue · Jobs ETL --------------------------------------------------------------

def _pastas_jobs() -> tuple[Path, Path]:
    jobs = Path(os.environ.get("EXPLORADOR_JOBS", "pipeline"))
    execucoes = Path(os.environ.get("EXPLORADOR_EXECUCOES", "/tmp/explorador-execucoes"))
    return (jobs if jobs.is_absolute() else RAIZ / jobs), execucoes


def _quando(instante: float) -> str:
    return datetime.fromtimestamp(instante).strftime("%d/%m/%Y %H:%M:%S")


def _relativo(caminho: Path) -> str:
    return str(caminho.relative_to(RAIZ)) if caminho.is_relative_to(RAIZ) else str(caminho)


def _executar_job(job: glue.Job, pasta: Path) -> None:
    if not glue.em_execucao(pasta, job.nome):
        st.session_state["execucao_sel"] = glue.iniciar(job, pasta, RAIZ)


def _painel_execucoes(job: glue.Job, pasta: Path, rodando_antes: bool = False) -> None:
    if rodando_antes and not glue.em_execucao(pasta, job.nome):
        st.rerun()                                  # terminou: redesenha a página toda (reativa o botão Executar)
    execucoes = glue.execucoes(pasta, job.nome)
    with ui.painel("execucoes", "Execuções", len(execucoes)):
        ui.tabela("execucoes", [("Execução", 3), ("Status", 1.4), ("Início", 2), ("Duração", 1), ("Código de saída", 1)],
                  [[e.id, ui.selo(e.status), _quando(e.inicio), f"{e.duracao:.1f} s".replace(".", ","),
                    "—" if e.codigo is None else str(e.codigo)] for e in execucoes],
                  vazio="Nenhuma execução ainda. Clique em Executar.")
    if not execucoes:
        return
    ids = [e.id for e in execucoes]
    if st.session_state.get("execucao_sel") not in ids:
        st.session_state["execucao_sel"] = ids[0]
    with ui.painel("log", "Log"):
        escolhida = st.selectbox("Execução", ids, key="execucao_sel")
        st.code(glue.log(pasta, escolhida, ultimas=300) or "(sem saída ainda)", language="text")


def _tela_jobs() -> None:
    pasta_jobs, pasta_exec = _pastas_jobs()
    jobs = glue.listar_jobs(pasta_jobs)
    job = next((j for j in jobs if j.nome == st.session_state.get("job_sel")), None)

    if job is None:
        _contexto("jobs-lista")
        ui.trilha([("Glue", None, None, {}), ("Jobs ETL", None, None, {})], "jobs")
        ui.titulo("Jobs", len(jobs), f"Os scripts .py de <code>{_relativo(pasta_jobs)}/</code> (menos o comum.py). "
                  "Executar aqui é o mesmo que <code>docker compose run --rm app python &lt;script&gt;</code>.")
        linhas = []
        for j in jobs:
            ultima = next(iter(glue.execucoes(pasta_exec, j.nome)), None)
            linhas.append([ui.Link(j.nome, f"job:{j.nome}", _escolher, kwargs={"job_sel": j.nome}), "Spark",
                           _relativo(j.script), ui.selo(ultima.status) if ultima else "—",
                           _quando(ultima.inicio) if ultima else "—"])
        with ui.painel("jobs", "Jobs", len(jobs)):
            ui.tabela("jobs", [("Nome do job", 2.8), ("Tipo", 0.7), ("Script", 3.2), ("Última execução", 1.4), ("Início", 1.6)],
                      linhas, localizar=len(linhas) > 8,
                      vazio="Nenhum job ainda. Crie o primeiro, por exemplo pipeline/bronze.py, e clique em Recarregar.")
        return

    _contexto("jobs-detalhe")
    ui.trilha([("Glue", None, None, {}), ("Jobs ETL", "jobs:inicio", _escolher, {"job_sel": None}),
               (job.nome, None, None, {})], "jobs")
    rodando = glue.em_execucao(pasta_exec, job.nome)
    titulo, botao = st.columns([4, 1], vertical_alignment="center")
    with titulo:
        ui.titulo(job.nome, descricao="Tipo: Spark (PySpark 3.5.3, local) · roda no contêiner do explorador · um job por vez")
    botao.button("Executar", key="executar_job", type="primary", disabled=rodando,
                 on_click=_executar_job, args=(job, pasta_exec))
    aba_exec, aba_script = st.tabs(["Execuções", "Script"])
    with aba_exec:
        # enquanto há execução em andamento, o painel se atualiza sozinho a cada 3 s
        st.fragment(run_every=3 if rodando else None)(_painel_execucoes)(job, pasta_exec, rodando)
    with aba_script:
        with ui.painel("script", _relativo(job.script)):
            st.code(job.script.read_text(encoding="utf-8"), language="python")


# --------------------------------------------------------------- Tutorial ---------------------------------------------------------------

def _arquivo_preferencias() -> Path:
    return Path(os.environ.get("EXPLORADOR_PREFERENCIAS", RAIZ / ".explorador" / "preferencias.json"))


def _salvas() -> dict:
    return preferencias.carregar(_arquivo_preferencias())


def _atuais() -> dict:
    """O que está valendo nesta sessão: o valor dos widgets `pref_*`, ou o salvo para os que ainda não existem."""
    return {k: st.session_state.get(f"pref_{k}", v) for k, v in _salvas().items()}


def _salvar_preferencia() -> None:
    atuais = _atuais()
    if atuais != _salvas():                                                  # só grava quando muda
        preferencias.salvar(_arquivo_preferencias(), atuais)


def _desligar_tutorial() -> None:
    st.session_state["pref_tutorial"] = False
    _salvar_preferencia()


def _restaurar_acessibilidade() -> None:
    for chave, valor in preferencias.PADRAO.items():
        if chave != "tutorial":
            st.session_state[f"pref_{chave}"] = valor
    _salvar_preferencia()


def _inicial(chave: str, salvas: dict, parametro: str = "value") -> dict:
    # o valor inicial vem do arquivo; depois, o próprio widget guarda o estado da sessão
    if f"pref_{chave}" in st.session_state:
        return {}
    valor = salvas[chave]
    return {parametro: preferencias.TAMANHOS.index(valor) if parametro == "index" else valor}


def _abrir_tour() -> None:
    # um número novo a cada clique, único também entre sessões: o navegador guarda o último que já abriu
    # (tour-forca) e um contador que recomeça em 1 depois de recarregar a página seria confundido com ele
    st.session_state["tour_forcar"] = time.time_ns()


def _contexto(nome: str) -> None:
    """Cada tela diz qual roteiro do tutorial vale para ela."""
    st.session_state["contexto_tour"] = nome


def _mostrar_tour() -> None:
    contexto = st.session_state.get("contexto_tour")
    if not contexto:
        return
    ligado = bool(st.session_state.get("pref_tutorial"))
    forcar = st.session_state.get("tour_forcar", 0)
    if ligado or forcar:
        ui.tour(contexto, tutorial.roteiro(contexto), auto=ligado, forcar=forcar)


# ---------------------------------------------------------------- página ----------------------------------------------------------------

TAMANHO_ROTULO = {100: "A", 115: "A+", 130: "A++"}


def _barra_flutuante(salvas: dict) -> None:
    """Três botões redondos no canto inferior direito: ⬆ topo (só depois de rolar), 🎓 tutorial desta tela
    e ♿, um menu com a chave do tutorial e as opções de acessibilidade."""
    with st.container(key="dock"):
        st.button("⬆", key="topo", help="Voltar ao topo da página")
        st.button("🎓", key="tour-abrir", on_click=_abrir_tour,
                  help="Abrir o tutorial desta tela: mostra, num modal, para que serve cada parte.")
        with st.popover("♿", help="Tutorial e acessibilidade"):
            st.markdown("**Tutorial**")
            st.toggle("Mostrar tutorial", key="pref_tutorial", on_change=_salvar_preferencia, **_inicial("tutorial", salvas),
                      help="Ligado, o tutorial abre sozinho na primeira visita a cada tela. Desligado, só pelo botão 🎓.")
            st.markdown("**Acessibilidade**")
            st.radio("Tamanho do texto", preferencias.TAMANHOS, key="pref_texto", horizontal=True,
                     format_func=TAMANHO_ROTULO.get, on_change=_salvar_preferencia,
                     **_inicial("texto", salvas, "index"))
            st.toggle("Alto contraste", key="pref_contraste", on_change=_salvar_preferencia,
                      **_inicial("contraste", salvas), help="Textos e bordas mais escuros.")
            st.toggle("Sublinhar links", key="pref_sublinhar", on_change=_salvar_preferencia,
                      **_inicial("sublinhar", salvas), help="Deixa claro o que é clicável sem depender da cor.")
            st.toggle("Mais espaço entre letras e linhas", key="pref_espacamento", on_change=_salvar_preferencia,
                      **_inicial("espacamento", salvas), help="Facilita a leitura, por exemplo para quem tem dislexia.")
            st.toggle("Reduzir movimento", key="pref_movimento", on_change=_salvar_preferencia,
                      **_inicial("movimento", salvas), help="Tira animações e a rolagem suave.")
            st.button("Voltar ao padrão", key="a11y-padrao", type="tertiary", on_click=_restaurar_acessibilidade)
        # botão invisível: o "Não mostrar mais o tutorial" do modal clica nele para gravar a preferência
        st.button("desligar tutorial", key="tour-desligar", on_click=_desligar_tutorial)


def _menu() -> str:
    opcoes = [TELA_S3, TELA_CATALOGO, TELA_JOBS, TELA_ATHENA]
    if st.session_state.get("tela") not in opcoes:
        st.session_state["tela"] = TELA_S3
    with st.sidebar:
        st.markdown('<div class="menu-titulo">Console do datalake</div>', unsafe_allow_html=True)
        tela = st.radio("Serviço", opcoes, key="tela", format_func=ICONES.get, label_visibility="collapsed")
        st.divider()
        if st.button("Recarregar", key="recarregar", help="Lê o bucket de novo e recria as tabelas"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()
        st.caption("Ferramenta de estudo inspirada nos consoles do S3, do Glue e do Athena. Roda no seu computador.")
    return tela


def _pagina() -> None:
    st.set_page_config(page_title="Console do datalake", layout="wide", initial_sidebar_state="expanded")
    st.markdown(ui.CSS, unsafe_allow_html=True)
    pasta_jobs = os.environ.get("EXPLORADOR_JOBS", "pipeline").rstrip("/")
    st.markdown(f'<div class="barra-console">'
                f'<span class="marca"><span aria-hidden="true">☁ </span>Console do datalake</span>'
                f'<span class="servicos">S3 · Glue · Athena</span>'
                f'<span class="detalhe">s3://{s3.BUCKET} · {s3.ENDPOINT} · Athena só lê · Jobs ETL executam {pasta_jobs}/</span>'
                f'<span class="regiao">{s3.REGIAO}</span></div>', unsafe_allow_html=True)
    tela = _menu()
    salvas = _salvas()
    _barra_flutuante(salvas)
    escolhas = _atuais()
    st.markdown(ui.css_acessibilidade(escolhas), unsafe_allow_html=True)
    ui.apoio_acessibilidade(reduzir_movimento=escolhas["movimento"])
    st.session_state.pop("contexto_tour", None)
    try:
        _conteudo(tela)
    finally:
        _mostrar_tour()


def _conteudo(tela: str) -> None:
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
