"""O workflow da solução (workflows/medalhao.json) tem que continuar valendo para os jobs desta pasta."""

from pathlib import Path

from explorador import glue, workflows

from executar_pipeline import JOBS as SEQUENCIA

SOLUCAO = Path(__file__).resolve().parents[1]


def test_workflow_da_solucao_e_valido_e_espelha_o_executar_pipeline():
    wf = workflows.carregar(SOLUCAO / "workflows" / "medalhao.json")
    workflows.validar(wf, {j.nome for j in glue.listar_jobs(SOLUCAO)})
    assert sorted(e.job for e in wf.etapas) == sorted(j.removesuffix(".py") for j in SEQUENCIA)   # os mesmos 8 jobs


def test_golds_dependem_so_da_silver_e_rodam_lado_a_lado():
    camadas = workflows.niveis(workflows.carregar(SOLUCAO / "workflows" / "medalhao.json"))
    assert camadas[0] == ["01_bronze_ingestao"] and camadas[1] == ["02_silver_eventos"]
    assert len(camadas[2]) == 5 and camadas[-1] == ["08_exportar_mapa_calor"]
