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
