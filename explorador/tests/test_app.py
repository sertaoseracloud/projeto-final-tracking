import functools
import time
from pathlib import Path

import duckdb
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from explorador import consultas, s3
from explorador.particoes import Objeto

APP = str(Path(__file__).resolve().parents[1] / "app.py")


@pytest.fixture(autouse=True)
def limpar_cache():
    st.cache_data.clear()
    st.cache_resource.clear()
    yield
    st.cache_data.clear()
    st.cache_resource.clear()


def _rodar():
    return AppTest.from_file(APP, default_timeout=60).run()


def test_floci_desligado_mostra_mensagem_amigavel(monkeypatch):
    def desligado(*_, **__):
        raise s3.S3Indisponivel("Não consegui falar com o S3 em http://floci:4566. Rode `docker compose up -d floci`.")
    monkeypatch.setattr(s3, "listar", desligado)
    at = _rodar()
    assert not at.exception
    assert "docker compose up -d floci" in at.error[0].value


def test_bucket_vazio_orienta_a_rodar_o_gerador(monkeypatch):
    monkeypatch.setattr(s3, "listar", lambda *_, **__: [])
    at = _rodar()
    assert not at.exception
    assert "gerar_eventos.py" in at.info[0].value


@pytest.fixture
def bucket_local(tmp_path, monkeypatch):
    pasta = tmp_path / "silver" / "event_name=page_view" / "year=2026" / "month=09" / "day=01"
    pasta.mkdir(parents=True)
    duckdb.sql(f"COPY (SELECT 'e' || i AS event_id FROM range(4) t(i)) TO '{pasta}/part-0.parquet' (FORMAT parquet)")
    objetos = [Objeto(p.relative_to(tmp_path).as_posix(), p.stat().st_size)
               for p in tmp_path.rglob("*") if p.is_file()]
    monkeypatch.setattr(s3, "listar", lambda *_, **__: objetos)
    monkeypatch.setattr(s3, "raiz", lambda: str(tmp_path))
    monkeypatch.setattr(consultas, "conectar", functools.partial(consultas.conectar, s3=False))
    return tmp_path

ATHENA = "Athena · Editor de consultas"
SQL = "sql:Consulta 1"


def _athena(at):
    return at.radio(key="tela").set_value(ATHENA).run()


def _executar(at, sql, aba="Consulta 1"):
    at.text_area(key=f"sql:{aba}").input(sql).run()
    return at.button(key="executar").click().run()


def test_abre_no_s3_com_a_lista_de_buckets_e_entra_no_bucket(bucket_local):
    at = _rodar()
    assert not at.exception
    assert at.radio(key="tela").options == ["S3 · Buckets", "Glue · Data Catalog", "Glue · Jobs ETL", ATHENA]
    assert at.button(key="abrir_bucket").label.endswith(s3.BUCKET)
    at.button(key="abrir_bucket").click().run()
    assert [t.label for t in at.tabs] == ["Objetos", "Partições", "Propriedades"]


def test_navega_pelo_breadcrumb_e_mostra_a_previa_do_arquivo(bucket_local):
    at = _rodar()
    at.button(key="abrir_bucket").click().run()
    caminho = ""
    for pasta in ("silver", "event_name=page_view", "year=2026", "month=09", "day=01"):
        caminho += pasta + "/"
        at.button(key=f"obj:{caminho}").click().run()
    assert at.session_state["pasta"] == "silver/event_name=page_view/year=2026/month=09/day=01/"
    at.button(key=f"obj:{caminho}part-0.parquet").click().run()
    assert not at.exception
    previa = next(d.value for d in at.dataframe if "event_id" in d.value.columns)
    assert previa["event_id"].tolist() == ["e0", "e1", "e2", "e3"]
    at.button(key="migalha:silver/event_name=page_view/").click().run()
    assert at.session_state["pasta"] == "silver/event_name=page_view/"
    at.button(key="migalha:buckets").click().run()
    assert at.button(key="abrir_bucket")


def test_aba_particoes_lista_os_dias_e_conta_linhas(bucket_local):
    at = _rodar()
    at.button(key="abrir_bucket").click().run()
    assert at.selectbox(key="dataset").value == "silver.eventos"
    at.button(key="contar").click().run()
    assert not at.exception
    contagens = [d.value for d in at.dataframe if "linhas" in d.value.columns]
    assert contagens[-1]["linhas"].tolist() == [4]


def test_athena_executa_e_mostra_detalhes_da_execucao(bucket_local):
    at = _executar(_athena(_rodar()), "SELECT count(*) AS n FROM silver.eventos")
    assert not at.exception
    assert [t.label for t in at.tabs] == ["Resultados", "Consultas recentes"]
    assert at.dataframe[0].value["n"].tolist() == [4]
    resumo = next(s.value for s in at.success)
    assert resumo.startswith("Concluída · tempo de execução") and "dados lidos (estimado)" in resumo
    assert "0 B" not in resumo                       # a view silver tem arquivos: a estimativa não é zero


def test_athena_recusa_sql_de_escrita(bucket_local):
    at = _executar(_athena(_rodar()), "DROP VIEW silver")
    assert any("só faz consultas de leitura" in e.value for e in at.error)


def test_athena_avisa_quando_corta_o_resultado(bucket_local):
    at = _athena(_rodar())
    at.text_area(key=SQL).input("SELECT * FROM range(20)").run()
    at.number_input(key="limite:Consulta 1").set_value(5).run()
    at.button(key="executar").click().run()
    assert len(at.dataframe[0].value) == 5
    assert any("cortado em 5 linhas" in w.value for w in at.warning)


def test_athena_erro_de_sql_vem_explicado_em_portugues(bucket_local):
    at = _executar(_athena(_rodar()), "SELECT * FROM view_que_nao_existe")
    assert not at.exception
    assert any("não existe" in e.value for e in at.error)
    assert any("Mensagem original do DuckDB" in c.value for c in at.caption)


def test_athena_abas_guardam_cada_uma_a_sua_consulta(bucket_local):
    at = _executar(_athena(_rodar()), "SELECT 1 AS um")
    at.button(key="nova_aba").click().run()
    assert at.text_area(key="sql:Consulta 2").value == ""
    at = _executar(at, "SELECT 2 AS dois", aba="Consulta 2")
    assert at.dataframe[0].value.columns.tolist() == ["dois"]
    at.radio(key="aba_ativa").set_value("Consulta 1").run()
    assert at.text_area(key=SQL).value == "SELECT 1 AS um"
    assert at.dataframe[0].value.columns.tolist() == ["um"]
    at.button(key="fechar_aba").click().run()
    assert at.radio(key="aba_ativa").options == ["Consulta 2"]


def test_athena_arvore_insere_o_nome_da_tabela_no_editor(bucket_local):
    at = _athena(_rodar())
    at.text_area(key=SQL).input("SELECT * FROM").run()
    at.button(key="inserir:silver.eventos").click().run()
    assert at.text_area(key=SQL).value == "SELECT * FROM silver.eventos"


def test_athena_historico_guarda_as_consultas_e_reabre_no_editor(bucket_local):
    at = _executar(_athena(_rodar()), "SELECT 1 AS um")
    at = _executar(at, "SELECT * FROM nao_existe")
    tabela = at.dataframe[-1].value
    assert tabela["Status"].tolist() == ["Falhou", "Concluída"]
    at.button(key="abrir_hist:1").click().run()
    assert at.text_area(key=SQL).value == "SELECT 1 AS um"


def test_erro_inesperado_vira_mensagem_sem_stack_trace(monkeypatch):
    def negado(*_, **__):
        raise RuntimeError("AccessDenied")
    monkeypatch.setattr(s3, "listar", negado)
    at = _rodar()
    assert not at.exception
    assert "Algo deu errado" in at.error[0].value


CATALOGO = "Glue · Data Catalog"
JOBS = "Glue · Jobs ETL"


def test_catalogo_navega_banco_tabela_e_abre_no_athena(bucket_local):
    at = _rodar()
    at.radio(key="tela").set_value(CATALOGO).run()
    assert not at.exception
    at.button(key="banco:silver").click().run()
    at.button(key="tabela:silver.eventos").click().run()
    assert [t.label for t in at.tabs] == ["Esquema", "Partições", "Propriedades"]
    esquema = next(d.value for d in at.dataframe if "Coluna" in d.value.columns)
    assert "event_id" in esquema["Coluna"].tolist()
    assert esquema.set_index("Coluna").loc["event_name", "Chave de partição"] == "sim"
    at.button(key="consultar_athena").click().run()
    assert at.radio(key="tela").value == ATHENA
    assert at.text_area(key="sql:Consulta 2").value == "SELECT * FROM silver.eventos LIMIT 10"


def test_jobs_lista_executa_e_mostra_o_log(bucket_local, tmp_path, monkeypatch):
    jobs = tmp_path / "meus_jobs"
    jobs.mkdir()
    (jobs / "bronze.py").write_text("print('bronze gravada')\n")
    (jobs / "comum.py").write_text("")
    monkeypatch.setenv("EXPLORADOR_JOBS", str(jobs))
    monkeypatch.setenv("EXPLORADOR_EXECUCOES", str(tmp_path / "execucoes"))
    at = _rodar()
    at.radio(key="tela").set_value(JOBS).run()
    assert not at.exception
    assert [b.key for b in at.button if b.key and b.key.startswith("job:")] == ["job:bronze"]
    at.button(key="job:bronze").click().run()
    assert [t.label for t in at.tabs] == ["Execuções", "Script"]
    at.button(key="executar_job").click().run()
    for _ in range(100):
        at.run()
        if any("Sucesso" in str(d.value) for d in at.dataframe):
            break
        time.sleep(0.2)
    assert any("Sucesso" in d.value.to_string() for d in at.dataframe)
    assert any("bronze gravada" in c.value for c in at.code)
