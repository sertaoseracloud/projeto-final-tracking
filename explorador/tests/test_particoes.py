from explorador.particoes import (SEM_PARTICAO, Dataset, LinhaParticao, Objeto, datasets,
                                  formato_do_arquivo, navegar, particao_da_chave,
                                  tabela_particoes, tamanho_legivel)

CHAVES = [
    "landing/eventos/year=2026/month=09/day=01/coletor-s42-001.jsonl",
    "landing/eventos/year=2026/month=09/day=02/coletor-s42-001.jsonl",
    "silver/eventos/year=2026/month=09/day=01/part-0.snappy.parquet",
    "silver/eventos/_SUCCESS",
    "silver/eventos_rejeitados/part-0.snappy.parquet",
    "gold/funil_compra/part-0.snappy.parquet",
    "gold/funil_compra/_SUCCESS",
    "_verificacao/part-0.parquet",
    "bronze/eventos/_temporary/0/part-0.parquet",
    "solto.parquet",
    "silver/eventos/leia-me.txt",
]


def test_formato_do_arquivo():
    assert formato_do_arquivo("a/b.snappy.parquet") == "parquet"
    assert formato_do_arquivo("a/b.jsonl") == "jsonl"
    assert formato_do_arquivo("a/_SUCCESS") is None
    assert formato_do_arquivo("a/b.txt") is None


def test_particao_da_chave_mantem_o_mes_com_dois_digitos():
    assert particao_da_chave(CHAVES[0]) == {"year": "2026", "month": "09", "day": "01"}
    assert particao_da_chave("gold/funil_compra/part-0.parquet") is None
    assert particao_da_chave("x/year=2026/part-0.parquet") == {"year": "2026"}


def test_datasets_ignora_pastas_internas_arquivos_soltos_e_nao_dados():
    assert datasets(CHAVES) == [
        Dataset("gold/funil_compra", "parquet", "gold_funil_compra"),
        Dataset("landing/eventos", "jsonl", "landing_eventos"),
        Dataset("silver/eventos", "parquet", "silver_eventos"),
        Dataset("silver/eventos_rejeitados", "parquet", "silver_eventos_rejeitados"),
    ]


def test_tabela_particoes_soma_arquivos_e_bytes_por_dia():
    objetos = [
        Objeto(CHAVES[0], 100), Objeto(CHAVES[1], 50),
        Objeto("landing/eventos/year=2026/month=09/day=01/coletor-s42-002.jsonl", 10),
        Objeto("landing/eventos_outros/year=2026/month=09/day=01/x.jsonl", 999),
    ]
    assert tabela_particoes(objetos, "landing/eventos") == [
        LinhaParticao("2026", "09", "01", 2, 110),
        LinhaParticao("2026", "09", "02", 1, 50),
    ]


def test_tabela_particoes_de_dataset_sem_particao():
    objetos = [Objeto("gold/funil_compra/part-0.parquet", 7), Objeto("gold/funil_compra/_SUCCESS", 0)]
    assert tabela_particoes(objetos, "gold/funil_compra/") == [
        LinhaParticao(SEM_PARTICAO, SEM_PARTICAO, SEM_PARTICAO, 1, 7)]


def test_navegar_devolve_subpastas_e_arquivos_diretos():
    objetos = [Objeto(c, 1) for c in CHAVES]
    pastas, arquivos = navegar(objetos, "")
    assert pastas == ["_verificacao", "bronze", "gold", "landing", "silver"]
    assert [o.chave for o in arquivos] == ["solto.parquet"]
    pastas, arquivos = navegar(objetos, "gold/funil_compra/")
    assert pastas == []
    assert [o.chave for o in arquivos] == ["gold/funil_compra/_SUCCESS", "gold/funil_compra/part-0.snappy.parquet"]


def test_tamanho_legivel():
    assert tamanho_legivel(0) == "0 B"
    assert tamanho_legivel(512) == "512 B"
    assert tamanho_legivel(1536) == "1,5 KB"
    assert tamanho_legivel(5 * 1024 * 1024) == "5,0 MB"
