from datetime import datetime

import pytest
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


@pytest.fixture(scope="module")
def silver(job):
    return job("02_silver_eventos")


@pytest.fixture(scope="module")
def schema_bronze(job):
    bronze = job("01_bronze_ingestao")
    return StructType(bronze.SCHEMA.fields + [
        StructField("_arquivo_origem", StringType()),
        StructField("_ingerido_em", TimestampType()),
        StructField("dt", StringType()),
    ])


def linha(event_id, event_name, ts="2026-09-01T10:00:00.000Z", pseudo="anon-1", user_id=None,
          params=None, items=None, ingerido="2026-10-01 00:00:00", arquivo="lote-1"):
    return (event_id, event_name, ts, pseudo, user_id, "s-1",
            ("https://loja.exemplo/", None), ("google", "cpc", None), ("mobile", "Android", "Chrome"),
            params or {}, items or [], arquivo, datetime.fromisoformat(ingerido), "2026-09-01")


@pytest.fixture
def bronze_df(spark, schema_bronze):
    def criar(*linhas):
        return spark.createDataFrame(list(linhas), schema_bronze)
    return criar


def test_deduplicar_mantem_a_primeira_ingestao(silver, bronze_df):
    df = bronze_df(
        linha("e1", "page_view", ingerido="2026-10-02 00:00:00", arquivo="b"),
        linha("e1", "page_view", ingerido="2026-10-01 00:00:00", arquivo="a"),
        linha("e2", "scroll", params={"percent_scrolled": "25"}),
    )
    linhas = silver.deduplicar(df).collect()
    assert len(linhas) == 2
    assert [r._arquivo_origem for r in linhas if r.event_id == "e1"] == ["a"]


def test_tipar_converte_params_timestamp_e_achata_structs(silver, bronze_df):
    df = silver.tipar(bronze_df(
        linha("e1", "purchase", params={"value": "1086.8", "transaction_id": "T-1", "currency": "BRL"}),
        linha("e2", "click", params={"outbound": "true", "link_domain": "instagram.com"}),
        linha("e3", "scroll", params={"percent_scrolled": "50"}),
    ))
    r = {x.event_id: x for x in df.collect()}
    assert r["e1"].value == 1086.8 and r["e1"].transaction_id == "T-1" and r["e1"].currency == "BRL"
    assert r["e1"].ts == datetime(2026, 9, 1, 10, 0)
    assert r["e1"].source == "google" and r["e1"].device_category == "mobile"
    assert r["e1"].page_location == "https://loja.exemplo/"
    assert r["e2"].outbound is True and r["e2"].link_domain == "instagram.com"
    assert r["e3"].percent_scrolled == 50 and r["e3"].value is None


def test_validar_aplica_os_seis_motivos_com_prioridade(silver, bronze_df):
    brl = {"currency": "BRL"}
    df = silver.tipar(bronze_df(
        linha("ok-compra", "purchase", params={"transaction_id": "T-1", "value": "597.0", **brl}),
        linha("ok-reembolso", "refund", params={"transaction_id": "T-1", "value": "597.0", **brl}),
        linha("ok-lead", "generate_lead", params={"value": "0", "lead_source": "ebook", **brl}),
        linha("catalogo", "add_to_wishlist", ts=None),
        linha("ts-nulo", "page_view", ts=None),
        linha("ts-futuro", "page_view", ts="2099-01-01T00:00:00.000Z"),
        linha("sem-id", "purchase", params={"value": "297.0", **brl}),
        linha("negativo", "add_to_cart", params={"value": "-297.0", **brl}),
        linha("usd", "add_to_cart", params={"value": "297.0", "currency": "USD"}),
        linha("marco", "scroll", params={"percent_scrolled": "33"}),
        linha("orfao", "refund", params={"transaction_id": "T-X", "value": "597.0", **brl}),
        linha("compra-invalida", "purchase", params={"transaction_id": "T-2", "value": "-1.0", **brl}),
        linha("cascata", "refund", params={"transaction_id": "T-2", "value": "1.0", **brl}),
    ))
    aprovados, rejeitados = silver.validar(df)
    assert {r.event_id for r in aprovados.collect()} == {"ok-compra", "ok-reembolso", "ok-lead"}
    assert "motivo_rejeicao" not in aprovados.columns
    assert {r.event_id: r.motivo_rejeicao for r in rejeitados.collect()} == {
        "catalogo": "evento fora do catalogo",
        "ts-nulo": "timestamp ausente ou futuro",
        "ts-futuro": "timestamp ausente ou futuro",
        "sem-id": "transacao sem id",
        "negativo": "valor monetario invalido",
        "usd": "valor monetario invalido",
        "marco": "marco fora do padrao",
        "orfao": "reembolso sem compra",
        "compra-invalida": "valor monetario invalido",
        "cascata": "reembolso sem compra",
    }


def _tipados(silver, bronze_df, *linhas):
    return silver.tipar(bronze_df(*linhas))


def test_sessionizar_abre_sessao_depois_de_30_minutos(silver, bronze_df):
    df = _tipados(silver, bronze_df,
        linha("a", "page_view", ts="2026-09-01T10:00:00.000Z"),
        linha("b", "scroll", ts="2026-09-01T10:20:00.000Z", params={"percent_scrolled": "25"}),
        linha("c", "page_view", ts="2026-09-01T10:50:01.000Z"),
        linha("d", "page_view", ts="2026-09-01T10:55:00.000Z"),
        linha("x", "page_view", ts="2026-09-01T10:00:00.000Z", pseudo="anon-2"),
    )
    r = {x.event_id: x.session_id for x in silver.sessionizar(df).collect()}
    assert r == {"a": "anon-1-1", "b": "anon-1-1", "c": "anon-1-2", "d": "anon-1-2", "x": "anon-2-1"}


def test_sessao_que_vira_a_meia_noite_continua_uma_so(silver, bronze_df):
    df = _tipados(silver, bronze_df,
        linha("a", "page_view", ts="2026-09-01T23:50:00.000Z"),
        linha("b", "scroll", ts="2026-09-02T00:10:00.000Z", params={"percent_scrolled": "25"}),
    )
    assert {x.session_id for x in silver.sessionizar(df).collect()} == {"anon-1-1"}


def test_resolver_identidade_preenche_eventos_anteriores_ao_cadastro(silver, bronze_df):
    df = _tipados(silver, bronze_df,
        linha("a", "page_view", ts="2026-09-01T10:00:00.000Z"),
        linha("b", "sign_up", ts="2026-09-01T10:05:00.000Z", user_id="u-1", params={"method": "email"}),
        linha("c", "refund", ts="2026-09-03T09:00:00.000Z", user_id=None),
        linha("x", "page_view", pseudo="anon-2"),
    )
    r = {x.event_id: x.user_id_resolvido for x in silver.resolver_identidade(df).collect()}
    assert r == {"a": "u-1", "b": "u-1", "c": "u-1", "x": None}


def test_explodir_itens_gera_uma_linha_por_item(silver, bronze_df):
    itens = [
        ("PLANO-PRO", "Plano Pro", "plano", 597.0, 1, None, None, None),
        ("BUMP-MENTORIA", "Mentoria em grupo", "order_bump", 147.0, 1, None, None, None),
    ]
    df = _tipados(silver, bronze_df,
        linha("p", "purchase", params={"transaction_id": "T-1", "value": "744.0", "currency": "BRL"}, items=itens),
        linha("v", "page_view"),
    )
    r = silver.explodir_itens(silver.resolver_identidade(silver.sessionizar(df))).collect()
    assert sorted(x.item_id for x in r) == ["BUMP-MENTORIA", "PLANO-PRO"]
    assert all(x.transaction_id == "T-1" and x.session_id == "anon-1-1" for x in r)
    assert sorted(x.posicao for x in r) == [0, 1]
