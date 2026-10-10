import duckdb
import pytest

from explorador import consultas, particoes


@pytest.fixture
def lago(tmp_path):
    """Monta um mini-bucket local: Silver particionada, landing JSONL e uma Gold."""
    con = duckdb.connect()
    for dia in ("01", "02"):
        pasta = tmp_path / "silver" / "eventos" / "year=2026" / "month=09" / f"day={dia}"
        pasta.mkdir(parents=True)
        con.sql(f"COPY (SELECT 'e{dia}-' || i AS event_id, 'page_view' AS event_name FROM range(3) t(i)) "
                f"TO '{pasta}/part-0.parquet' (FORMAT parquet)")
    gold = tmp_path / "gold" / "funil_compra"
    gold.mkdir(parents=True)
    con.sql(f"COPY (SELECT 'purchase' AS etapa, 7 AS sessoes) TO '{gold}/part-0.parquet' (FORMAT parquet)")
    (gold / "_SUCCESS").write_text("")
    landing = tmp_path / "landing" / "eventos" / "year=2026" / "month=09" / "day=01"
    landing.mkdir(parents=True)
    (landing / "coletor-s42-001.jsonl").write_text(
        '{"event_id":"a","event_name":"scroll"}\n{"event_id":"b","event_name":"click"}\n', encoding="utf-8")
    chaves = [p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*") if p.is_file()]
    conexao = consultas.conectar(s3=False)
    views = consultas.criar_views(conexao, str(tmp_path), particoes.datasets(chaves))
    consultas.travar(conexao, str(tmp_path))
    return conexao, views, tmp_path


def test_cria_uma_view_por_dataset(lago):
    _, views, _ = lago
    assert set(views) == {"gold_funil_compra", "landing_eventos", "silver_eventos"}


def test_particoes_viram_colunas_de_texto(lago):
    con, _, _ = lago
    r = consultas.executar(con, "SELECT DISTINCT month, day, typeof(month) AS tipo FROM silver_eventos ORDER BY day")
    assert r.tabela.to_dict("records") == [
        {"month": "09", "day": "01", "tipo": "VARCHAR"},
        {"month": "09", "day": "02", "tipo": "VARCHAR"},
    ]


def test_contagens_das_views(lago):
    con, _, _ = lago
    contar = lambda v: consultas.executar(con, f"SELECT count(*) AS n FROM {v}").tabela["n"][0]
    assert (contar("silver_eventos"), contar("landing_eventos"), contar("gold_funil_compra")) == (6, 2, 1)


def test_executar_corta_no_limite_e_avisa(lago):
    con, _, _ = lago
    r = consultas.executar(con, "SELECT * FROM range(10)", limite=3)
    assert len(r.tabela) == 3 and r.cortado is True and r.colunas == ["range"]
    r = consultas.executar(con, "SELECT * FROM range(10)", limite=10)
    assert len(r.tabela) == 10 and r.cortado is False


def test_executar_nunca_passa_do_limite_maximo(lago):
    con, _, _ = lago
    r = consultas.executar(con, f"SELECT * FROM range({consultas.LIMITE_MAXIMO + 5})", limite=10**9)
    assert len(r.tabela) == consultas.LIMITE_MAXIMO and r.cortado is True


def test_executar_recusa_sql_proibido_sem_executar(lago):
    con, _, tmp_path = lago
    destino = tmp_path / "copia.csv"
    with pytest.raises(consultas.SqlNaoPermitido):
        consultas.executar(con, f"COPY silver_eventos TO '{destino}'")
    assert not destino.exists()


def test_executar_recusa_explain_analyze_com_copy_escondido(lago):
    con, _, tmp_path = lago
    destino = tmp_path / "pwn1.csv"
    sql = f"EXPLAIN ANALYZE /* /* */ ' */ COPY (SELECT 42) TO '{destino}' --'"
    with pytest.raises(consultas.SqlNaoPermitido):
        consultas.executar(con, sql)
    assert not destino.exists()


def test_previa_le_um_arquivo(lago):
    con, _, tmp_path = lago
    chave = "landing/eventos/year=2026/month=09/day=01/coletor-s42-001.jsonl"
    assert consultas.previa(con, str(tmp_path), chave)["event_id"].tolist() == ["a", "b"]
    assert len(consultas.previa(con, str(tmp_path), "gold/funil_compra/part-0.parquet")) == 1
    with pytest.raises(ValueError, match="Prévia"):
        consultas.previa(con, str(tmp_path), "gold/funil_compra/_SUCCESS")


def test_conexao_travada_nao_le_fora_da_raiz(lago):
    con, _, _ = lago
    with pytest.raises(duckdb.Error):
        consultas.executar(con, "SELECT * FROM read_text('/etc/hostname')")
    with pytest.raises(duckdb.Error):
        con.sql("SET enable_external_access = true")
