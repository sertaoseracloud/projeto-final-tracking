def test_dados_monta_dataframe_com_padroes(spark):
    from professor.solucao.tests.dados import eventos, jornada

    df = eventos(spark, *jornada("s1", ["page_view", "scroll"], percent_scrolled=25))
    linhas = df.orderBy("ts").collect()
    assert [r.event_id for r in linhas] == ["s1-0", "s1-1"]
    assert linhas[0].source == "google" and linhas[1].percent_scrolled == 25


def test_solucao_usa_o_comum_entregue_ao_aluno(job):
    comum = job("comum")
    assert comum.__file__.replace("\\", "/").endswith("pipeline/comum.py")
