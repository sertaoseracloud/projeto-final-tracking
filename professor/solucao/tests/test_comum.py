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


def test_gravar_eventos_preserva_as_pastas_vizinhas_e_ler_eventos_as_ignora(spark, job, tmp_path, monkeypatch):
    comum = job("comum")
    monkeypatch.setattr(comum, "caminho", lambda camada: f"file://{tmp_path}/{camada}")
    itens = spark.createDataFrame([("i1",)], ["item_id"])
    itens.write.parquet(comum.caminho("silver/itens"))
    linhas = [("e1", "page_view", "2026", "09", "01"), ("e2", "scroll", "2026", "09", "01")]
    df = spark.createDataFrame(linhas, ["event_id", "event_name", "year", "month", "day"])

    comum.gravar_eventos(df, "silver", "overwrite")
    comum.gravar_eventos(df.filter("event_name = 'scroll'"), "silver", "overwrite")

    assert (tmp_path / "silver" / "itens").is_dir()
    assert (tmp_path / "silver" / "event_name=page_view" / "year=2026" / "month=09" / "day=01").is_dir()
    lidos = comum.ler_eventos(spark, "silver")
    assert sorted((r.event_id, r.event_name) for r in lidos.collect()) == [("e1", "page_view"), ("e2", "scroll")]
    assert comum.existe(spark, comum.caminho(comum.glob_eventos("silver"))) is True
    assert comum.existe(spark, comum.caminho(comum.glob_eventos("bronze"))) is False
