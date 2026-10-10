import pytest
from pyspark.sql import functions as F

from professor.solucao.tests.dados import eventos, jornada

ATRIBUTOS_DDL = "session_id string, canal string, dispositivo string, plano string"


@pytest.fixture(scope="module")
def funil(job):
    return job("03_gold_funil")


def test_contar_etapas_calcula_conversao_anterior_e_do_topo(spark, funil):
    flags = spark.createDataFrame(
        [("s1", "x", "p", 1, 1, 1), ("s2", "x", "p", 1, 1, 0), ("s3", "x", "p", 1, 0, 0), ("s4", "x", "p", 1, 0, 0)],
        "session_id string, canal string, plano string, a int, b int, c int")
    r = {x.etapa: x for x in funil.contar_etapas(flags, ["canal", "plano"], ["a", "b", "c"]).collect()}
    assert [r[e].sessoes for e in "abc"] == [4, 2, 1]
    assert [r[e].ordem for e in "abc"] == [1, 2, 3]
    assert r["a"].conv_etapa_anterior is None
    assert r["b"].conv_etapa_anterior == 0.5 and r["c"].conv_topo == 0.25


def test_funil_compra_so_tem_frete_no_recorte_premium(spark, funil):
    premium = ["view_item_list", "select_item", "add_to_cart", "view_cart", "begin_checkout",
               "add_shipping_info", "add_payment_info", "purchase"]
    pro = ["view_item_list", "select_item", "view_item", "add_to_cart", "view_cart", "begin_checkout",
           "add_payment_info", "purchase"]
    ev = eventos(spark, *jornada("s1", premium), *jornada("s2", pro, inicio="2026-09-01 12:00:00"))
    atributos = spark.createDataFrame(
        [("s1", "google / cpc", "mobile", "PLANO-PREMIUM"), ("s2", "google / cpc", "mobile", "PLANO-PRO")],
        ATRIBUTOS_DDL)
    f = funil.funil_compra(ev, atributos)

    def etapas(plano):
        return [x.etapa for x in f.filter(F.col("plano") == plano).orderBy("ordem").collect()]

    assert "add_shipping_info" in etapas("PLANO-PREMIUM")
    assert "add_shipping_info" not in etapas("PLANO-PRO")
    assert etapas("(todos)") == funil.ETAPAS
    total = {x.etapa: x for x in f.filter("escopo = 'total'").collect()}
    assert total["purchase"].sessoes == 2 and total["purchase"].conv_topo == 1.0
    assert total["purchase"].taxa_view_item == 0.5
    assert f.filter("plano = 'PLANO-PRO'").first().taxa_view_item == 1.0


def test_funil_lead_conta_visita_rolagem_e_lead_por_canal(spark, funil):
    s1 = jornada("s1", ["page_view", "scroll", "generate_lead"])
    s1[1]["percent_scrolled"] = 50
    s2 = jornada("s2", ["page_view", "scroll"], inicio="2026-09-01 12:00:00")
    s2[1]["percent_scrolled"] = 25
    s3 = jornada("s3", ["page_view"], inicio="2026-09-01 13:00:00", page_location="https://loja.exemplo/checkout")
    ev = eventos(spark, *s1, *s2, *s3)
    atributos = spark.createDataFrame(
        [("s1", "google / cpc", "mobile", "(nenhum)"), ("s2", "direct / (none)", "mobile", "(nenhum)"),
         ("s3", "direct / (none)", "mobile", "(nenhum)")], ATRIBUTOS_DDL)
    r = {(x.canal, x.etapa): x.sessoes for x in funil.funil_lead(ev, atributos).collect()}
    assert r[("(todos)", "visita_landing")] == 2
    assert r[("(todos)", "scroll_50")] == 1 and r[("(todos)", "generate_lead")] == 1
    assert r[("google / cpc", "generate_lead")] == 1 and r[("direct / (none)", "generate_lead")] == 0
