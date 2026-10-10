import functools
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
    pasta = tmp_path / "silver" / "eventos" / "year=2026" / "month=09" / "day=01"
    pasta.mkdir(parents=True)
    duckdb.sql(f"COPY (SELECT 'e' || i AS event_id FROM range(4) t(i)) TO '{pasta}/part-0.parquet' (FORMAT parquet)")
    objetos = [Objeto(p.relative_to(tmp_path).as_posix(), p.stat().st_size)
               for p in tmp_path.rglob("*") if p.is_file()]
    monkeypatch.setattr(s3, "listar", lambda *_, **__: objetos)
    monkeypatch.setattr(s3, "raiz", lambda: str(tmp_path))
    monkeypatch.setattr(consultas, "conectar", functools.partial(consultas.conectar, s3=False))
    return tmp_path


def test_app_monta_as_tres_abas_e_executa_sql(bucket_local):
    at = _rodar()
    assert not at.exception
    assert [t.label for t in at.tabs] == ["Arquivos", "Partições", "Consultas SQL"]
    at.text_area(key="sql").input("SELECT count(*) AS n FROM silver_eventos").run()
    at.button(key="executar").click().run()
    assert not at.exception
    assert at.dataframe[-1].value["n"].tolist() == [4]


def test_app_recusa_sql_de_escrita(bucket_local):
    at = _rodar()
    at.text_area(key="sql").input("DROP VIEW silver_eventos").run()
    at.button(key="executar").click().run()
    assert any("só faz consultas de leitura" in w.value for w in at.warning)


def test_app_mostra_aviso_quando_corta_o_resultado(bucket_local):
    at = _rodar()
    at.text_area(key="sql").input("SELECT * FROM range(20)").run()
    at.number_input(key="limite").set_value(5).run()
    at.button(key="executar").click().run()
    assert len(at.dataframe[-1].value) == 5
    assert any("cortado em 5 linhas" in w.value for w in at.warning)


def test_erro_inesperado_vira_mensagem_sem_stack_trace(monkeypatch):
    def negado(*_, **__):
        raise RuntimeError("AccessDenied")
    monkeypatch.setattr(s3, "listar", negado)
    at = _rodar()
    assert not at.exception
    assert "Algo deu errado" in at.error[0].value


def test_resultado_continua_na_tela_apos_outra_interacao(bucket_local):
    at = _rodar()
    at.text_area(key="sql").input("SELECT count(*) AS n FROM silver_eventos").run()
    at.button(key="executar").click().run()
    assert at.dataframe[-1].value["n"].tolist() == [4]
    at.number_input(key="limite").set_value(7).run()
    assert not at.exception
    assert at.dataframe[-1].value["n"].tolist() == [4]


def test_resultado_mostra_a_sql_que_o_gerou(bucket_local):
    at = _rodar()
    at.text_area(key="sql").input("SELECT count(*) AS n FROM silver_eventos").run()
    at.button(key="executar").click().run()
    assert at.code[-1].value == "SELECT count(*) AS n FROM silver_eventos"


def test_erro_de_sql_vem_explicado_em_portugues(bucket_local):
    at = _rodar()
    at.text_area(key="sql").input("SELECT * FROM view_que_nao_existe").run()
    at.button(key="executar").click().run()
    assert not at.exception
    assert any("não existe" in e.value for e in at.error)
    assert any("Mensagem original do DuckDB" in c.value for c in at.caption)


def test_navega_ate_o_arquivo_e_mostra_a_previa(bucket_local):
    at = _rodar()
    for pasta in ("silver", "eventos", "year=2026", "month=09", "day=01"):
        atual = at.session_state["pasta"] if "pasta" in at.session_state else ""
        at.button(key=f"pasta:{atual}{pasta}").click().run()
    assert at.session_state["pasta"] == "silver/eventos/year=2026/month=09/day=01/"
    at.selectbox(key="previa").select("part-0.parquet").run()
    assert not at.exception
    assert at.dataframe[1].value["event_id"].tolist() == ["e0", "e1", "e2", "e3"]
    at.button(key="voltar").click().run()
    assert at.session_state["pasta"] == "silver/eventos/year=2026/month=09/"


def test_aba_particoes_lista_os_dias_e_conta_linhas(bucket_local):
    at = _rodar()
    assert at.selectbox(key="dataset").value == "silver_eventos"
    at.button(key="contar").click().run()
    assert not at.exception
    contagens = [d.value for d in at.dataframe if "linhas" in d.value.columns]
    assert contagens[-1]["linhas"].tolist() == [4]
