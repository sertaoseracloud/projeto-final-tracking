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


def test_site_carrega_o_mapa_antes_do_script_principal_e_tem_o_botao():
    pos_mapa = HTML.index('<script src="mapa_calor.js"></script>')
    pos_principal = HTML.index("<script>\n(function () {")
    assert pos_mapa < pos_principal
    assert 'id="mapa-calor"' in HTML


def test_camadas_do_overlay_nao_recebem_clique():
    assert re.search(r"\.calor-camada\s*\{[^}]*pointer-events:\s*none", HTML)
    assert re.search(r"\.calor-faixa\s*\{[^}]*pointer-events:\s*none", HTML)
