"""Sobe para a landing do datalake o JSONL exportado pela página site/index.html.

    python gerador/importar_site.py eventos_2026-10-10.jsonl
    python gerador/importar_site.py eventos.jsonl --saida-local saida
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gerador.destinos import DestinoLocal, DestinoS3
from gerador.lotes import PREFIXO


def validar(texto: str) -> list[dict]:
    eventos = []
    for n, linha in enumerate(texto.splitlines(), start=1):
        if not linha.strip():
            continue
        try:
            evento = json.loads(linha)
        except json.JSONDecodeError as erro:
            raise ValueError(f"linha {n}: JSON inválido ({erro.msg})") from None
        if not isinstance(evento, dict) or not isinstance(evento.get("event_id"), str) \
                or not isinstance(evento.get("event_ts"), str):
            raise ValueError(f"linha {n}: falta event_id ou event_ts")
        eventos.append(evento)
    if not eventos:
        raise ValueError("arquivo sem eventos")
    return eventos


def chave(eventos: list[dict], agora: datetime) -> str:
    return f"{PREFIXO}/dt={eventos[0]['event_ts'][:10]}/site-{agora:%Y%m%dT%H%M%S}.jsonl"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("arquivo", type=Path, help="JSONL exportado pelo botão 'Exportar JSONL' do site")
    ap.add_argument("--saida-local", type=Path, help="grava nesta pasta em vez do S3")
    a = ap.parse_args(argv)

    texto = a.arquivo.read_text(encoding="utf-8")
    try:
        eventos = validar(texto)
    except ValueError as erro:
        print(f"erro: {erro}", file=sys.stderr)
        return 1

    destino = DestinoLocal(a.saida_local) if a.saida_local else DestinoS3()
    k = chave(eventos, datetime.now(timezone.utc))
    destino.gravar(k, texto if texto.endswith("\n") else texto + "\n")
    print(f"{len(eventos)} eventos enviados para {destino.descricao()} em {k}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
