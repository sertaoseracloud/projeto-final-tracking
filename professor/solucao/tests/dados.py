"""Fábricas de DataFrames no formato da Silver, para os testes das Golds."""

from __future__ import annotations

from datetime import datetime, timedelta

EVENTOS_DDL = (
    "event_id string, event_name string, ts timestamp, session_id string, user_pseudo_id string, "
    "user_id_resolvido string, page_location string, source string, medium string, "
    "device_category string, percent_scrolled int, video_percent int, search_term string, "
    "search_results int, outbound boolean, link_domain string, file_name string, "
    "transaction_id string, value double"
)
ITENS_DDL = (
    "event_id string, event_name string, ts timestamp, session_id string, "
    "transaction_id string, item_id string, item_category string"
)
PADRAO_EVENTO = {
    "user_pseudo_id": "anon-1",
    "page_location": "https://loja.exemplo/",
    "source": "google",
    "medium": "cpc",
    "device_category": "mobile",
}


def _criar(spark, ddl: str, linhas, padrao: dict):
    colunas = [c.strip().split()[0] for c in ddl.split(",")]

    def valor(coluna, linha):
        v = linha.get(coluna, padrao.get(coluna))
        return datetime.fromisoformat(v) if coluna == "ts" and isinstance(v, str) else v

    return spark.createDataFrame([tuple(valor(c, l) for c in colunas) for l in linhas], ddl)


def eventos(spark, *linhas: dict):
    return _criar(spark, EVENTOS_DDL, linhas, PADRAO_EVENTO)


def itens(spark, *linhas: dict):
    return _criar(spark, ITENS_DDL, linhas, {})


def jornada(sessao: str, nomes: list[str], inicio: str = "2026-09-01 10:00:00", **extra) -> list[dict]:
    """Uma sessão com um evento por minuto: event_id = '<sessao>-<i>'."""
    t0 = datetime.fromisoformat(inicio)
    return [
        {"event_id": f"{sessao}-{i}", "event_name": nome, "ts": t0 + timedelta(minutes=i),
         "session_id": sessao, **extra}
        for i, nome in enumerate(nomes)
    ]
