import json


def _gravar_landing(tmp_path):
    def pasta(nome):
        destino = tmp_path / "landing" / f"event_name={nome}" / "year=2026" / "month=09" / "day=01"
        destino.mkdir(parents=True)
        return destino

    compra = {
        "event_id": "e1", "event_name": "purchase", "event_ts": "2026-09-01T14:22:05.123Z",
        "user_pseudo_id": "anon-1", "user_id": None, "session_hint": "s-1",
        "page": {"location": "https://loja.exemplo/checkout", "referrer": None},
        "traffic": {"source": "google", "medium": "cpc", "campaign": "formacao_dados"},
        "device": {"category": "mobile", "os": "Android", "browser": "Chrome"},
        "params": {"transaction_id": "T-1", "value": 1086.8, "coupon": None, "currency": "BRL"},
        "items": [{"item_id": "PLANO-PREMIUM", "item_name": "Plano Premium",
                   "item_category": "plano", "price": 997.0, "quantity": 1}],
    }
    rolagem = dict(compra, event_id="e2", event_name="scroll",
                   params={"percent_scrolled": 50, "outbound": True}, items=[])
    (pasta("purchase") / "coletor-s42-001.jsonl").write_text(json.dumps(compra, ensure_ascii=False) + "\n",
                                                             encoding="utf-8")
    (pasta("scroll") / "coletor-s42-001.jsonl").write_text(json.dumps(rolagem) + "\n", encoding="utf-8")
    return tmp_path / "landing"


def test_le_a_landing_com_schema_fixo_e_params_como_texto(spark, job, tmp_path):
    bronze = job("01_bronze_ingestao")
    df = bronze.ler_landing(spark, _gravar_landing(tmp_path).as_uri())
    linhas = {r.event_name: r for r in df.collect()}
    assert linhas["purchase"].params["value"] == "1086.8"
    assert linhas["purchase"].params["coupon"] is None
    assert linhas["purchase"].items[0].price == 997.0
    assert linhas["purchase"].traffic.source == "google"
    assert linhas["scroll"].params["percent_scrolled"] == "50"
    assert linhas["scroll"].params["outbound"] == "true"


def test_proveniencia_traz_arquivo_instante_e_data_do_caminho(spark, job, tmp_path):
    bronze = job("01_bronze_ingestao")
    df = bronze.adicionar_proveniencia(bronze.ler_landing(spark, _gravar_landing(tmp_path).as_uri()))
    linhas = df.collect()
    assert {(r.year, r.month, r.day) for r in linhas} == {("2026", "09", "01")}
    assert all(r._arquivo_origem.endswith("coletor-s42-001.jsonl") for r in linhas)
    assert all(r._ingerido_em is not None for r in linhas)
    tipos = dict(df.dtypes)
    assert (tipos["year"], tipos["month"], tipos["day"]) == ("string", "string", "string")
    assert "dt" not in df.columns


def test_event_name_vem_da_pasta_da_particao(spark, job, tmp_path):
    bronze = job("01_bronze_ingestao")
    df = bronze.ler_landing(spark, _gravar_landing(tmp_path).as_uri())
    assert sorted(r.event_name for r in df.collect()) == ["purchase", "scroll"]
