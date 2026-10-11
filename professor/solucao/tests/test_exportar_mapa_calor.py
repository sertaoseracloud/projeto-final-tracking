import json

import pytest


@pytest.fixture
def exportar(job):
    return job("08_exportar_mapa_calor")


def test_montar_js_gera_atribuicao_com_json_valido(exportar):
    cliques = [{"pagina": "/", "element_id": "plano-pro", "cliques": 3, "sessoes": 2, "sessoes_na_pagina": 3,
                "taxa_clique": 0.6667, "intensidade": 1.0}]
    rolagem = [{"pagina": "/", "marco": 0, "sessoes_que_chegaram": 3, "pct_sessoes": 1.0}]
    texto = exportar.montar_js(cliques, rolagem, "2026-10-10T21:00:00Z")
    assert texto.startswith("window.MAPA_CALOR = ") and texto.endswith(";\n")
    dados = json.loads(texto[len("window.MAPA_CALOR = "):-2])
    assert dados == {"gerado_em": "2026-10-10T21:00:00Z", "cliques": cliques, "rolagem": rolagem}


def test_gravar_cria_a_pasta_e_substitui_o_arquivo(exportar, tmp_path):
    destino = tmp_path / "site" / "mapa_calor.js"
    exportar.gravar("window.MAPA_CALOR = {};\n", destino)
    exportar.gravar("window.MAPA_CALOR = {\"x\": 1};\n", destino)
    assert destino.read_text(encoding="utf-8") == "window.MAPA_CALOR = {\"x\": 1};\n"
    assert [p.name for p in destino.parent.iterdir()] == ["mapa_calor.js"]


def test_destino_e_a_pasta_site_do_repositorio(exportar):
    assert exportar.DESTINO.parts[-2:] == ("site", "mapa_calor.js")
    assert (exportar.DESTINO.parent / "index.html").exists()
