import pytest

from professor.solucao.tests.dados import eventos, itens

ATRIBUTOS_DDL = "session_id string, canal string, dispositivo string, plano string"


@pytest.fixture(scope="module")
def rec(job):
    return job("05_gold_receita_aquisicao")


@pytest.fixture
def cenario(spark):
    """T-1 (Pro) reembolsada 3 dias depois; T-2 (Pro + bump) de quem virou lead 2 dias antes."""
    ev = eventos(spark,
        {"event_id": "c1", "event_name": "purchase", "ts": "2026-09-01 10:00:00", "session_id": "s1",
         "transaction_id": "T-1", "value": 597.0},
        {"event_id": "r1", "event_name": "refund", "ts": "2026-09-04 10:00:00", "session_id": "s9",
         "transaction_id": "T-1", "value": 597.0},
        {"event_id": "l2", "event_name": "generate_lead", "ts": "2026-08-30 12:00:00", "session_id": "s0",
         "user_pseudo_id": "anon-2", "value": 0.0},
        {"event_id": "c2", "event_name": "purchase", "ts": "2026-09-01 12:00:00", "session_id": "s2",
         "user_pseudo_id": "anon-2", "transaction_id": "T-2", "value": 744.0},
        {"event_id": "c3", "event_name": "purchase", "ts": "2026-09-01 08:00:00", "session_id": "s3",
         "user_pseudo_id": "anon-3", "transaction_id": "T-3", "value": 297.0},
        {"event_id": "l3", "event_name": "generate_lead", "ts": "2026-09-02 09:00:00", "session_id": "s4",
         "user_pseudo_id": "anon-3", "value": 0.0},
    )
    it = itens(spark,
        {"event_id": "c1", "event_name": "purchase", "transaction_id": "T-1", "item_id": "PLANO-PRO", "item_category": "plano"},
        {"event_id": "c2", "event_name": "purchase", "transaction_id": "T-2", "item_id": "PLANO-PRO", "item_category": "plano"},
        {"event_id": "c2", "event_name": "purchase", "transaction_id": "T-2", "item_id": "BUMP-MENTORIA", "item_category": "order_bump"},
        {"event_id": "c3", "event_name": "purchase", "transaction_id": "T-3", "item_id": "PLANO-BASICO", "item_category": "plano"},
    )
    atributos = spark.createDataFrame(
        [(s, c, "mobile", "(nenhum)") for s, c in [("s0", "newsletter / email"), ("s1", "google / cpc"),
                                                     ("s2", "google / cpc"), ("s3", "direct / (none)"),
                                                     ("s4", "instagram / paid_social"), ("s9", "direct / (none)")]],
        ATRIBUTOS_DDL)
    return ev, it, atributos


def test_reembolso_desconta_na_data_da_compra(rec, cenario):
    ev, it, atr = cenario
    compras = rec.compras(ev, it, atr)
    r = {(x.data_compra.isoformat(), x.plano, x.canal): x
         for x in rec.receita_diaria(compras, rec.reembolsos(ev)).collect()}
    pro = r[("2026-09-01", "PLANO-PRO", "google / cpc")]
    assert (pro.receita_bruta, pro.receita_reembolsada, pro.receita_liquida) == (1341.0, 597.0, 744.0)
    assert (pro.pedidos, pro.ticket_medio) == (2, 670.5)
    assert all(data == "2026-09-01" for data, _, _ in r)


def test_taxa_de_reembolso_e_adesao_ao_bump(rec, cenario):
    ev, it, atr = cenario
    compras = rec.compras(ev, it, atr)
    taxa = {(x.plano, x.canal): x for x in rec.taxa_reembolso(compras, rec.reembolsos(ev)).collect()}
    assert (taxa[("(todos)", "(todos)")].pedidos, taxa[("(todos)", "(todos)")].reembolsados) == (3, 1)
    assert taxa[("PLANO-PRO", "google / cpc")].taxa_reembolso == 0.5
    bump = {x.plano: x for x in rec.adesao_order_bump(compras).collect()}
    assert (bump["(todos)"].pedidos, bump["(todos)"].com_bump) == (3, 1)
    assert bump["PLANO-PRO"].adesao == 0.5


def test_lead_so_conta_compra_posterior(rec, cenario):
    ev, it, atr = cenario
    r = {x.canal: x for x in rec.lead_para_compra(ev, rec.compras(ev, it, atr), atr).collect()}
    todos = r["(todos)"]
    assert (todos.leads, todos.compraram_depois, todos.mediana_dias) == (2, 1, 2.0)
    assert todos.taxa_conversao == 0.5
    assert r["instagram / paid_social"].compraram_depois == 0
