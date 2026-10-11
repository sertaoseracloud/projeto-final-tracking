"""Lógica das telas "Data Catalog" e "Jobs ETL" (no estilo do Glue), sem Streamlit.

Catálogo: os datasets do bucket viram bancos (um por camada) e tabelas.
Jobs: os scripts de pipeline/ rodam como `python <script>` num processo separado;
cada execução deixa na pasta de execuções um .json (início), um .log (saída) e,
ao terminar, um .fim (código de saída).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from explorador.particoes import CAMPOS, SEM_PARTICAO, Dataset, Objeto, banco_e_tabela, tabela_particoes

ORDEM_CAMADAS = ["landing", "bronze", "silver", "gold"]
IGNORADOS = {"comum.py", "__init__.py"}
EM_EXECUCAO, SUCESSO, FALHOU = "Em execução", "Sucesso", "Falhou"


# ------------------------------------------------------------- catálogo -------------------------------------------------------------

def bancos(datasets: list[Dataset]) -> dict[str, list[Dataset]]:
    """{banco: [tabelas]}, com as camadas na ordem do medalhão e o resto em ordem alfabética."""
    por_banco: dict[str, list[Dataset]] = {}
    for d in sorted(datasets, key=lambda d: d.nome_view):
        por_banco.setdefault(banco_e_tabela(d.nome_view)[0], []).append(d)
    ordem = lambda b: (ORDEM_CAMADAS.index(b) if b in ORDEM_CAMADAS else len(ORDEM_CAMADAS), b)
    return {b: por_banco[b] for b in sorted(por_banco, key=ordem)}


def localizacao(dataset: Dataset, bucket: str) -> str:
    return f"s3://{bucket}/{dataset.prefixo}/"


def chaves_particao(dataset: Dataset, objetos: list[Objeto]) -> list[str]:
    linhas = tabela_particoes(objetos, dataset.prefixo, dataset.formato)
    return [c for c in CAMPOS if any(getattr(l, c) != SEM_PARTICAO for l in linhas)]


# --------------------------------------------------------------- jobs ---------------------------------------------------------------

@dataclass(frozen=True)
class Job:
    nome: str
    script: Path


@dataclass
class Execucao:
    id: str
    job: str
    inicio: float
    status: str
    codigo: int | None
    duracao: float


def listar_jobs(pasta: Path) -> list[Job]:
    """Os scripts .py da pasta, menos o comum.py e os que começam com "_"."""
    pasta = Path(pasta)
    if not pasta.is_dir():
        return []
    return [Job(p.stem, p) for p in sorted(pasta.glob("*.py"))
            if p.name not in IGNORADOS and not p.name.startswith("_")]


# Roda o script, grava stdout+stderr no .log e, no fim, o código de saída no .fim.
_EXECUTOR = """
import json, subprocess, sys, time
script, log, fim, raiz = sys.argv[1:5]
with open(log, "w", encoding="utf-8") as saida:
    codigo = subprocess.run([sys.executable, "-u", script], stdout=saida, stderr=subprocess.STDOUT, cwd=raiz).returncode
with open(fim + ".tmp", "w", encoding="utf-8") as f:
    json.dump({"codigo": codigo, "fim": time.time()}, f)
import os; os.replace(fim + ".tmp", fim)
"""


def iniciar(job: Job, pasta: Path, raiz: Path) -> str:
    """Começa uma execução em segundo plano e devolve o id dela."""
    pasta = Path(pasta)
    pasta.mkdir(parents=True, exist_ok=True)
    agora = time.time()
    id_execucao = f"{job.nome}-{time.strftime('%Y%m%d-%H%M%S', time.localtime(agora))}-{int(agora * 1000) % 1000:03d}"
    base = pasta / id_execucao
    processo = subprocess.Popen(
        [sys.executable, "-c", _EXECUTOR, str(job.script), f"{base}.log", f"{base}.fim", str(raiz)],
        cwd=str(raiz), start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    (pasta / f"{id_execucao}.json").write_text(
        json.dumps({"job": job.nome, "script": str(job.script), "inicio": agora, "pid": processo.pid}), encoding="utf-8")
    return id_execucao


def _vivo(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    try:                                            # processo filho já terminado vira zumbi até ser recolhido
        return os.waitpid(pid, os.WNOHANG) == (0, 0)
    except ChildProcessError:
        return True


def execucao(pasta: Path, id_execucao: str) -> Execucao:
    pasta = Path(pasta)
    dados = json.loads((pasta / f"{id_execucao}.json").read_text(encoding="utf-8"))
    fim = pasta / f"{id_execucao}.fim"
    if fim.exists():
        final = json.loads(fim.read_text(encoding="utf-8"))
        status = SUCESSO if final["codigo"] == 0 else FALHOU
        return Execucao(id_execucao, dados["job"], dados["inicio"], status, final["codigo"], final["fim"] - dados["inicio"])
    if not _vivo(dados["pid"]):                     # o processo morreu sem gravar o .fim (contêiner reiniciado, kill…)
        return Execucao(id_execucao, dados["job"], dados["inicio"], FALHOU, None, time.time() - dados["inicio"])
    return Execucao(id_execucao, dados["job"], dados["inicio"], EM_EXECUCAO, None, time.time() - dados["inicio"])


def execucoes(pasta: Path, job: str | None = None) -> list[Execucao]:
    """Execuções da pasta (de um job, se pedido), a mais recente primeiro."""
    pasta = Path(pasta)
    if not pasta.is_dir():
        return []
    todas = [execucao(pasta, p.stem) for p in pasta.glob("*.json")]
    return sorted((e for e in todas if job is None or e.job == job), key=lambda e: e.inicio, reverse=True)


def em_execucao(pasta: Path, job: str) -> bool:
    return any(e.status == EM_EXECUCAO for e in execucoes(pasta, job))


def log(pasta: Path, id_execucao: str, ultimas: int | None = None) -> str:
    arquivo = Path(pasta) / f"{id_execucao}.log"
    if not arquivo.exists():
        return ""
    texto = arquivo.read_text(encoding="utf-8", errors="replace").replace("\r", "\n")
    linhas = [l for l in texto.split("\n") if l.strip() and "Stage " not in l]   # tira a barra de progresso do Spark
    return "\n".join(linhas[-ultimas:] if ultimas else linhas)
