import pytest

from professor.solucao.tests.dados import eventos, jornada

LANDING = "https://loja.exemplo/"
CHECKOUT = "https://loja.exemplo/checkout"


@pytest.fixture
def mapa(job):
    return job("07_gold_mapa_calor")


def test_pagina_do_evento_tira_dominio_e_ancora(spark, mapa):
    df = spark.createDataFrame([(LANDING,), ("https://loja.exemplo/checkout",), ("https://loja.exemplo/#planos",),
                                (None,)], "page_location string")
    assert [r.p for r in df.select(mapa.pagina_do_evento().alias("p")).collect()] == ["/", "/checkout", "/", None]


def test_mapa_cliques_conta_taxa_e_intensidade(spark, mapa):
    linhas = (
        jornada("s1", ["page_view", "click", "select_item"], page_location=LANDING)
        + jornada("s2", ["page_view", "select_item", "select_item"], page_location=LANDING)
        + jornada("s3", ["page_view"], page_location=LANDING)
        + jornada("s4", ["begin_checkout"], page_location=CHECKOUT)
    )
    elementos = {"s1-1": "cta-hero-ver-planos", "s1-2": "plano-pro", "s2-1": "plano-pro", "s2-2": "plano-pro",
                 "s4-0": "btn-continuar"}
    for l in linhas:
        l["element_id"] = elementos.get(l["event_id"])
    df = mapa.mapa_cliques(eventos(spark, *linhas))
    r = {(x.pagina, x.element_id): x for x in df.collect()}
    pro = r[("/", "plano-pro")]
    assert (pro.cliques, pro.sessoes, pro.sessoes_na_pagina, pro.taxa_clique, pro.intensidade) == (3, 2, 3, 0.6667, 1.0)
    cta = r[("/", "cta-hero-ver-planos")]
    assert (cta.cliques, cta.taxa_clique, cta.intensidade) == (1, 0.3333, 0.3333)
    sem_visita = r[("/checkout", "btn-continuar")]
    assert (sem_visita.sessoes_na_pagina, sem_visita.taxa_clique, sem_visita.intensidade) == (0, None, 1.0)
    assert ("/", None) not in r


def test_mapa_rolagem_comeca_em_1_e_nunca_cresce(spark, mapa):
    linhas = (
        jornada("a", ["page_view", "scroll", "scroll"], page_location=LANDING)
        + jornada("b", ["page_view", "scroll"], page_location=LANDING)
        + jornada("c", ["page_view"], page_location=LANDING)
        + jornada("d", ["page_view", "scroll"], page_location=CHECKOUT)
    )
    pct = {"a-1": 25, "a-2": 90, "b-1": 50, "d-1": 90}
    for l in linhas:
        l["percent_scrolled"] = pct.get(l["event_id"])
    df = mapa.mapa_rolagem(eventos(spark, *linhas))
    r = [(x.pagina, x.marco, x.sessoes_que_chegaram, x.pct_sessoes) for x in df.orderBy("marco").collect()]
    assert r == [("/", 0, 3, 1.0), ("/", 25, 2, 0.6667), ("/", 50, 2, 0.6667), ("/", 75, 1, 0.3333),
                 ("/", 90, 1, 0.3333)]


def test_mapa_cliques_descarta_eventos_sem_pagina(spark, mapa):
    linhas = jornada("s1", ["page_view", "click", "click"], page_location=LANDING)
    linhas[1]["element_id"] = linhas[2]["element_id"] = "cta-hero-ver-planos"
    linhas[2]["page_location"] = None
    df = mapa.mapa_cliques(eventos(spark, *linhas))
    assert [(r.pagina, r.element_id, r.cliques) for r in df.collect()] == [("/", "cta-hero-ver-planos", 1)]
