import importlib.util
from pathlib import Path

_ARQUIVO = Path(__file__).resolve().parents[2] / "usar_solucao.py"
_spec = importlib.util.spec_from_file_location("usar_solucao", _ARQUIVO)
usar_solucao = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(usar_solucao)


def _montar(tmp_path):
    solucao, pipeline = tmp_path / "solucao", tmp_path / "pipeline"
    (solucao / "workflows").mkdir(parents=True)
    pipeline.mkdir()
    (solucao / "01_bronze.py").write_text("print('bronze')\n")
    (solucao / "comum.py").write_text("# só carrega o pipeline/comum.py\n")
    (solucao / "__init__.py").write_text("")
    (solucao / "workflows" / "medalhao.json").write_text("{}\n")
    (pipeline / "comum.py").write_text("X = 1\n")
    return solucao, pipeline


def test_copia_jobs_e_workflows_mas_nao_o_comum(tmp_path):
    solucao, pipeline = _montar(tmp_path)
    copiados, pulados = usar_solucao.copiar(solucao, pipeline)
    assert sorted(p.relative_to(pipeline).as_posix() for p in copiados) == ["01_bronze.py", "workflows/medalhao.json"]
    assert pulados == [] and (pipeline / "comum.py").read_text() == "X = 1\n"


def test_nunca_sobrescreve_o_arquivo_do_aluno(tmp_path):
    solucao, pipeline = _montar(tmp_path)
    (pipeline / "01_bronze.py").write_text("print('do aluno')\n")
    _, pulados = usar_solucao.copiar(solucao, pipeline)
    assert pulados == [pipeline / "01_bronze.py"]
    assert (pipeline / "01_bronze.py").read_text() == "print('do aluno')\n"


def test_desfazer_apaga_so_as_copias_identicas(tmp_path):
    solucao, pipeline = _montar(tmp_path)
    usar_solucao.copiar(solucao, pipeline)
    (pipeline / "workflows" / "medalhao.json").write_text('{"mudado": true}\n')   # o aluno mexeu: fica
    removidos = usar_solucao.desfazer(solucao, pipeline)
    assert removidos == [pipeline / "01_bronze.py"]
    assert (pipeline / "workflows" / "medalhao.json").exists() and (pipeline / "comum.py").exists()


def test_a_solucao_real_tem_o_que_copiar():
    pares = usar_solucao._pares(usar_solucao.SOLUCAO, usar_solucao.PIPELINE)
    nomes = [origem.name for origem, _ in pares]
    assert "01_bronze_ingestao.py" in nomes and "medalhao.json" in nomes and "comum.py" not in nomes
