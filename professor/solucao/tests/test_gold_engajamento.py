import pytest

from professor.solucao.tests.dados import eventos, jornada


@pytest.fixture(scope="module")
def eng(job):
    return job("04_gold_engajamento")


def _video(sessao, percentuais, inicio):
    nomes = ["video_start"] + ["video_progress"] * (len(percentuais) - 1)
    if percentuais[-1] == 100:
        nomes[-1] = "video_complete"
    linhas = jornada(sessao, nomes, inicio=inicio)
    for linha, p in zip(linhas, percentuais):
        linha["video_percent"] = p
    return linhas


def test_retencao_video_mede_quem_iniciou_e_chegou_a_cada_marco(spark, eng):
    ev = eventos(spark,
                 *_video("s1", [0, 10, 25, 50, 75, 100], "2026-09-01 10:00:00"),
                 *_video("s2", [0, 10], "2026-09-01 11:00:00"),
                 *_video("s3", [0], "2026-09-01 12:00:00"),
                 *jornada("s4", ["page_view"], inicio="2026-09-01 13:00:00"))
    r = {x.marco: x for x in eng.retencao_video(ev).collect()}
    assert r[10].iniciaram == 3
    assert (r[10].sessoes, r[25].sessoes, r[100].sessoes) == (2, 1, 1)
    assert r[10].percentual == round(2 / 3, 4)


def test_buscas_faq_separa_as_sem_resultado(spark, eng):
    buscas = jornada("s1", ["site_search"] * 4)
    for linha, (termo, n) in zip(buscas, [("linux", 0), ("linux", 0), ("linux", 0), ("certificado", 1)]):
        linha.update(search_term=termo, search_results=n)
    r = {x.search_term: x for x in eng.buscas_faq(eventos(spark, *buscas)).collect()}
    assert (r["linux"].buscas, r["linux"].buscas_sem_resultado) == (3, 3)
    assert (r["certificado"].buscas, r["certificado"].buscas_sem_resultado) == (1, 0)


def test_saidas_contam_so_cliques_externos_e_downloads_por_arquivo(spark, eng):
    linhas = jornada("s1", ["click", "click", "click", "file_download"])
    linhas[0].update(outbound=True, link_domain="instagram.com")
    linhas[1].update(outbound=True, link_domain="instagram.com")
    linhas[2].update(outbound=False, link_domain=None)
    linhas[3].update(file_name="ementa-formacao-dados.pdf")
    r = {(x.tipo, x.alvo): x.quantidade for x in eng.saidas_downloads(eventos(spark, *linhas)).collect()}
    assert r == {("saida", "instagram.com"): 2, ("download", "ementa-formacao-dados.pdf"): 1}
