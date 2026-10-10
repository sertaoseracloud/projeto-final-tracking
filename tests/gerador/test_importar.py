from datetime import datetime, timezone

import pytest

from gerador.importar_site import chave, main, validar

UM = '{"event_id":"a","event_ts":"2026-10-10T12:00:00.000Z"}'


def test_validar_aceita_o_jsonl_do_site_e_ignora_linhas_vazias():
    eventos = validar(UM + '\n\n{"event_id":"b","event_ts":"2026-10-10T12:00:01.000Z"}\n')
    assert [e["event_id"] for e in eventos] == ["a", "b"]


@pytest.mark.parametrize("texto, mensagem", [
    ("isto não é json\n", "linha 1: JSON inválido"),
    ('{"event_id":"a"}\n', "linha 1: falta event_id ou event_ts"),
    (UM + "\n[1, 2]\n", "linha 2: falta event_id ou event_ts"),
    ("\n\n", "arquivo sem eventos"),
])
def test_validar_rejeita_com_a_linha_do_problema(texto, mensagem):
    with pytest.raises(ValueError, match=mensagem):
        validar(texto)


def test_chave_usa_a_data_do_primeiro_evento_e_o_instante_do_envio():
    agora = datetime(2026, 10, 11, 8, 30, 0, tzinfo=timezone.utc)
    assert chave([{"event_ts": "2026-10-10T12:00:00.000Z"}], agora) == \
        "landing/eventos/dt=2026-10-10/site-20261011T083000.jsonl"


def test_main_grava_o_arquivo_na_landing(tmp_path):
    origem = tmp_path / "eventos.jsonl"
    origem.write_text(UM + "\n", encoding="utf-8")
    assert main([str(origem), "--saida-local", str(tmp_path / "s3")]) == 0
    gravados = list((tmp_path / "s3").glob("landing/eventos/dt=2026-10-10/site-*.jsonl"))
    assert len(gravados) == 1
    assert gravados[0].read_text(encoding="utf-8") == UM + "\n"


def test_main_devolve_1_e_explica_quando_o_arquivo_e_invalido(tmp_path, capsys):
    origem = tmp_path / "ruim.jsonl"
    origem.write_text("{quebrado\n", encoding="utf-8")
    assert main([str(origem), "--saida-local", str(tmp_path / "s3")]) == 1
    assert "linha 1" in capsys.readouterr().err
    assert not (tmp_path / "s3").exists()
