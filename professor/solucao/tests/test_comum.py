def test_caminho_aponta_para_o_bucket_por_s3a(job):
    comum = job("comum")
    assert comum.caminho("silver/eventos") == f"s3a://{comum.BUCKET}/silver/eventos"


def test_existe_usa_o_filesystem_do_hadoop(spark, job, tmp_path):
    comum = job("comum")
    (tmp_path / "camada").mkdir()
    assert comum.existe(spark, (tmp_path / "camada").as_uri()) is True
    assert comum.existe(spark, (tmp_path / "nada").as_uri()) is False


def test_dados_monta_dataframe_com_padroes(spark):
    from professor.solucao.tests.dados import eventos, jornada

    df = eventos(spark, *jornada("s1", ["page_view", "scroll"], percent_scrolled=25))
    linhas = df.orderBy("ts").collect()
    assert [r.event_id for r in linhas] == ["s1-0", "s1-1"]
    assert linhas[0].source == "google" and linhas[1].percent_scrolled == 25
