"""Copia a solução de referência para as pastas do aluno, para mostrá-la no explorador.

    docker compose run --rm app python professor/usar_solucao.py            # copia
    docker compose run --rm app python professor/usar_solucao.py --desfazer # tira as cópias

O explorador só trabalha com pipeline/ (o botão Sincronizar envia essa pasta para o floci).
Este script copia os jobs de professor/solucao/ para pipeline/ e os workflows de
professor/solucao/workflows/ para pipeline/workflows/. Não copia o comum.py: a solução já usa
o pipeline/comum.py, que é a fonte única.

As cópias ficam fora do git só enquanto ninguém as commitar: rode --desfazer antes de gerar o
pacote do aluno, senão a solução vai junto no zip.
"""

from __future__ import annotations

import filecmp
import shutil
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
SOLUCAO = RAIZ / "professor" / "solucao"
PIPELINE = RAIZ / "pipeline"
NAO_COPIAR = {"comum.py", "__init__.py"}


def _pares(solucao: Path, pipeline: Path) -> list[tuple[Path, Path]]:
    jobs = [(p, pipeline / p.name) for p in sorted(solucao.glob("*.py")) if p.name not in NAO_COPIAR]
    fluxos = [(p, pipeline / "workflows" / p.name) for p in sorted((solucao / "workflows").glob("*.json"))]
    return jobs + fluxos


def copiar(solucao: Path = SOLUCAO, pipeline: Path = PIPELINE) -> tuple[list[Path], list[Path]]:
    """(copiados, pulados): um arquivo do aluno com o mesmo nome e conteúdo diferente nunca é sobrescrito."""
    copiados, pulados = [], []
    for origem, destino in _pares(solucao, pipeline):
        if destino.exists() and not filecmp.cmp(origem, destino, shallow=False):
            pulados.append(destino)
            continue
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origem, destino)
        copiados.append(destino)
    return copiados, pulados


def desfazer(solucao: Path = SOLUCAO, pipeline: Path = PIPELINE) -> list[Path]:
    """Apaga só as cópias idênticas à solução; o que o aluno escreveu fica."""
    removidos = []
    for origem, destino in _pares(solucao, pipeline):
        if destino.exists() and filecmp.cmp(origem, destino, shallow=False):
            destino.unlink()
            removidos.append(destino)
    return removidos


def main(argumentos: list[str]) -> int:
    if argumentos == ["--desfazer"]:
        removidos = desfazer()
        print(f"{len(removidos)} cópia(s) da solução removida(s) de pipeline/.")
        print("No explorador, clique em Sincronizar para tirá-las também do floci.")
        return 0
    copiados, pulados = copiar()
    for destino in copiados:
        print(f"  copiado  {destino.relative_to(RAIZ)}")
    for destino in pulados:
        print(f"  pulado   {destino.relative_to(RAIZ)} (já existe com outro conteúdo)")
    print(f"\n{len(copiados)} arquivo(s) copiado(s). No explorador, clique em Sincronizar (Jobs ETL ou Workflows).")
    print("Antes de gerar o pacote do aluno: python professor/usar_solucao.py --desfazer")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
