from explorador import tutorial

CONTEXTOS = ["s3-buckets", "s3-bucket", "catalogo-bancos", "catalogo-tabelas", "catalogo-tabela",
             "jobs-lista", "jobs-detalhe", "athena"]


def test_toda_tela_tem_um_roteiro_com_titulo_e_explicacao():
    assert set(tutorial.ROTEIROS) == set(CONTEXTOS)
    for contexto in CONTEXTOS:
        passos = tutorial.roteiro(contexto)
        assert passos, contexto
        for passo in passos:
            assert passo.titulo and len(passo.texto) > 40
            assert passo.seletor is None or passo.seletor.startswith(("[", "."))


def test_seletores_usam_a_classe_que_o_streamlit_gera():
    # o Streamlit troca ":" por "-" na classe st-key-…; seletor com ":" nunca encontra o elemento
    for passos in tutorial.ROTEIROS.values():
        for passo in passos:
            if passo.seletor and "st-key-" in passo.seletor:
                assert ":" not in passo.seletor, passo.seletor
    assert "data-baseweb" not in tutorial.ABAS                     # as abas do Streamlit 1.65 são role="tablist"


def test_roteiro_de_tela_desconhecida_e_vazio():
    assert tutorial.roteiro("nao-existe") == []


def test_preferencias_padrao_com_tutorial_ligado(tmp_path):
    assert tutorial.carregar(tmp_path / "nao_existe.json") == {"tutorial": True}


def test_salvar_e_carregar_preferencias(tmp_path):
    arquivo = tmp_path / "pasta" / "preferencias.json"
    tutorial.salvar(arquivo, {"tutorial": False})
    assert tutorial.carregar(arquivo) == {"tutorial": False}


def test_arquivo_estragado_volta_ao_padrao(tmp_path):
    arquivo = tmp_path / "preferencias.json"
    arquivo.write_text("{isto não é json", encoding="utf-8")
    assert tutorial.carregar(arquivo) == {"tutorial": True}
