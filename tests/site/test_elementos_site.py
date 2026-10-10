import re
from pathlib import Path

from gerador.elementos import ELEMENTOS

HTML = (Path(__file__).resolve().parents[2] / "site" / "index.html").read_text(encoding="utf-8")


def _ids_no_site() -> set[str]:
    literais = set(re.findall(r'data-heat="([^"]+)"', HTML))
    planos = set(re.findall(r'heat: "([^"]+)"', HTML))      # cartões gerados por JS (PLANOS)
    return literais | planos


def test_todo_elemento_do_contrato_existe_no_site_e_vice_versa():
    assert _ids_no_site() == set(ELEMENTOS)


def test_eventos_de_clique_passam_por_comElemento():
    for nome in ("click", "file_download", "video_start", "select_item", "begin_checkout",
                 "add_shipping_info", "add_payment_info", "purchase", "refund", "generate_lead", "site_search"):
        trecho = re.search(r'track\("' + nome + r'", comElemento\(', HTML)
        assert trecho, f"track(\"{nome}\", ...) sem comElemento"
