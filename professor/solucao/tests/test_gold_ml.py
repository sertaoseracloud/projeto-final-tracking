import pytest

from professor.solucao.tests.dados import eventos, jornada

ATRIBUTOS_DDL = "session_id string, canal string, dispositivo string, plano string"


@pytest.fixture(scope="module")
def ml(job):
    return job("06_gold_ml")


def test_features_ignoram_tudo_que_vem_depois_do_add_to_cart(spark, ml):
    linhas = jornada("s1", ["page_view", "scroll", "view_item_list", "select_item", "add_to_cart",
                            "view_cart", "scroll", "page_view", "purchase"])
    linhas[1]["percent_scrolled"] = 50
    linhas[6]["percent_scrolled"] = 90
    atributos = spark.createDataFrame([("s1", "google / cpc", "mobile", "PLANO-PRO")], ATRIBUTOS_DDL)
    r = ml.features_sessao(eventos(spark, *linhas), atributos).first()
    assert (r.scroll_max, r.page_views, r.viu_planos, r.selecionou_plano) == (50, 1, 1, 1)
    assert r.rotulo == 1.0 and r.plano == "PLANO-PRO"


def _dados_separaveis(spark, ml):
    linhas = []
    for i in range(200):
        comprou = i % 4 == 0
        base = {n: 0 for n in ml.NUMERICAS}
        base.update(scroll_max=90 if comprou else 25 + 25 * (i % 2), video_max=75 if comprou else i % 3 * 10,
                    viu_planos=1, selecionou_plano=1 if comprou else i % 5 == 0, page_views=1)
        linhas.append((f"s{i}", ["google / cpc", "instagram / paid_social"][i % 2], "mobile",
                       "PLANO-PRO" if comprou else "(nenhum)", *[int(base[n]) for n in ml.NUMERICAS],
                       1.0 if comprou else 0.0))
    ddl = ("session_id string, canal string, dispositivo string, plano string, "
           + ", ".join(f"{n} int" for n in ml.NUMERICAS) + ", rotulo double")
    return spark.createDataFrame(linhas, ddl)


def test_treinar_propensao_pontua_todas_as_sessoes_e_mede_no_teste(spark, ml):
    pontuado, metricas = ml.treinar_propensao(_dados_separaveis(spark, ml))
    assert metricas["auc"] > 0.9
    assert metricas["n_treino"] + metricas["n_teste"] == 200
    assert pontuado.count() == 200
    assert {"precisao", "recall"} <= set(metricas)
    assert pontuado.columns == ["session_id", "conjunto", "rotulo", "probabilidade"]
    assert {r.conjunto for r in pontuado.select("conjunto").distinct().collect()} == {"treino", "teste"}
    assert all(0 <= r.probabilidade <= 1 for r in pontuado.collect())


def test_pesos_de_classe_fazem_o_modelo_prever_a_classe_rara(spark, ml):
    linhas = []
    for i in range(400):
        comprou = i % 20 == 0
        base = {n: 0 for n in ml.NUMERICAS}
        base.update(scroll_max=(60 + i % 4 * 10) if comprou else (10 + i % 7 * 10),
                    video_max=(50 + i % 3 * 15) if comprou else (i % 6 * 10),
                    viu_planos=1, page_views=1)
        linhas.append((f"s{i}", ["google / cpc", "instagram / paid_social"][i % 2], "mobile", "(nenhum)",
                       *[int(base[n]) for n in ml.NUMERICAS], 1.0 if comprou else 0.0))
    ddl = ("session_id string, canal string, dispositivo string, plano string, "
           + ", ".join(f"{n} int" for n in ml.NUMERICAS) + ", rotulo double")
    _, metricas = ml.treinar_propensao(spark.createDataFrame(linhas, ddl))
    assert metricas["recall"] > 0.5


def test_segmentar_escolhe_k_pelo_silhouette_e_devolve_um_por_visitante(spark, ml):
    grupos = [(1.0, 5.0, 25.0, 0.0, 0.0, 0.0), (3.0, 20.0, 90.0, 1.0, 1.0, 0.0), (2.0, 40.0, 75.0, 1.0, 1.0, 1.0)]
    linhas = [(f"v{g}-{i}", *[x + (i % 3) * 0.01 for x in grupos[g]]) for g in range(3) for i in range(30)]
    ddl = "visitante string, " + ", ".join(f"{c} double" for c in ml.PERFIL)
    seg = ml.segmentar(spark.createDataFrame(linhas, ddl)).collect()
    assert len(seg) == 90 and len({r.visitante for r in seg}) == 90
    assert seg[0].k in (3, 4, 5) and seg[0].silhueta > 0.5
    assert sum(1 for r in seg if r.segmento == seg[0].segmento) == seg[0].visitantes_no_segmento
