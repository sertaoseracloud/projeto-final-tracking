"""Funções puras sobre as chaves do bucket: datasets, partições e navegação.

Uma chave é o "caminho" de um arquivo no S3, por exemplo
landing/eventos/year=2026/month=09/day=01/coletor-s42-001.jsonl.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

CAMPOS = ("year", "month", "day")
SEM_PARTICAO = "—"
EXTENSOES = {".parquet": "parquet", ".jsonl": "jsonl"}


@dataclass(frozen=True)
class Objeto:
    chave: str
    tamanho: int
    modificado: datetime | None = None


@dataclass(frozen=True)
class Dataset:
    prefixo: str
    formato: str
    nome_view: str


@dataclass(frozen=True)
class LinhaParticao:
    year: str
    month: str
    day: str
    arquivos: int
    bytes: int


def _interno(segmento: str) -> bool:
    return segmento.startswith(("_", "."))


def formato_do_arquivo(chave: str) -> str | None:
    nome = chave.rsplit("/", 1)[-1]
    if _interno(nome):
        return None
    return next((f for ext, f in EXTENSOES.items() if nome.endswith(ext)), None)


def particao_da_chave(chave: str) -> dict[str, str] | None:
    valores = {}
    for segmento in chave.split("/")[:-1]:
        campo, igual, valor = segmento.partition("=")
        if igual and campo in CAMPOS:
            valores[campo] = valor
    return valores or None


def _prefixo_do_dataset(chave: str) -> str:
    base = []
    for segmento in chave.split("/")[:-1]:
        if "=" in segmento:
            break
        base.append(segmento)
    return "/".join(base)


def _nome_view(prefixo: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", prefixo).strip("_").lower()


def datasets(chaves: Iterable[str]) -> list[Dataset]:
    """Uma pasta com arquivos .parquet ou .jsonl vira um dataset (e uma view).

    Se a mesma pasta tiver os dois formatos, vira um dataset por formato, com o
    formato no fim do nome da view (ex.: landing_eventos_jsonl)."""
    formatos: dict[str, set[str]] = defaultdict(set)
    for chave in chaves:
        formato = formato_do_arquivo(chave)
        if formato is None or any(_interno(s) for s in chave.split("/")[:-1]):
            continue
        prefixo = _prefixo_do_dataset(chave)
        if prefixo:
            formatos[prefixo].add(formato)
    resultado = []
    for prefixo, achados in sorted(formatos.items()):
        for formato in sorted(achados):
            nome = _nome_view(prefixo) + (f"_{formato}" if len(achados) > 1 else "")
            resultado.append(Dataset(prefixo, formato, nome))
    return resultado


def _pertence_ao_dataset(resto: str) -> bool:
    """`resto` é a chave sem o prefixo do dataset. O arquivo conta se estiver direto na
    pasta do dataset ou dentro de pastas de partição (k=v), sem pastas internas (_x, .x)."""
    pastas = resto.split("/")[:-1]
    if any(_interno(p) for p in pastas):
        return False
    return not pastas or "=" in pastas[0]


def tabela_particoes(objetos: Iterable[Objeto], prefixo: str, formato: str | None = None) -> list[LinhaParticao]:
    """Uma linha por partição year/month/day; com `formato`, conta só arquivos daquele formato."""
    base = prefixo.rstrip("/") + "/"
    grupos: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0])
    for o in objetos:
        formato_do_objeto = formato_do_arquivo(o.chave)
        if not o.chave.startswith(base) or formato_do_objeto is None:
            continue
        if formato is not None and formato_do_objeto != formato:
            continue
        if not _pertence_ao_dataset(o.chave[len(base):]):
            continue
        particao = particao_da_chave(o.chave) or {}
        chave = tuple(particao.get(c, SEM_PARTICAO) for c in CAMPOS)
        grupos[chave][0] += 1
        grupos[chave][1] += o.tamanho
    return [LinhaParticao(*k, arquivos=a, bytes=b) for k, (a, b) in sorted(grupos.items())]


def navegar(objetos: Iterable[Objeto], pasta: str) -> tuple[list[str], list[Objeto]]:
    subpastas: set[str] = set()
    arquivos: list[Objeto] = []
    for o in objetos:
        if not o.chave.startswith(pasta):
            continue
        resto = o.chave[len(pasta):]
        if "/" in resto:
            subpastas.add(resto.split("/", 1)[0])
        elif resto:
            arquivos.append(o)
    return sorted(subpastas), sorted(arquivos, key=lambda o: o.chave)


def tamanho_legivel(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    valor = float(n)
    for unidade in ("KB", "MB", "GB", "TB"):
        valor /= 1024
        # Compara já arredondado: 1048575 B é "1,0 MB", não "1024,0 KB".
        if round(valor, 1) < 1024 or unidade == "TB":
            return f"{valor:.1f} {unidade}".replace(".", ",")
    return f"{n} B"
