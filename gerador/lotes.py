"""Divide os eventos em lotes por dia, como um coletor que descarrega em intervalos.

Cada dia vira uma partição `year=AAAA/month=MM/day=DD`. O dia do lote
(`_dt`) é o dia de coleta, gravado antes da sujeira, por isso
um evento com timestamp nulo ou no futuro ainda tem uma partição. Cerca de
1% dos eventos é reenviado no lote vizinho com o mesmo event_id; é a
duplicata que a Silver precisa remover.
"""

from __future__ import annotations

import json
import random
from collections import defaultdict

PREFIXO = "landing/eventos"


def particao(data_iso: str) -> str:
    """'2026-09-01' -> 'year=2026/month=09/day=01'."""
    ano, mes, dia = data_iso[:10].split("-")
    return f"year={ano}/month={mes}/day={dia}"


def serializar(eventos: list[dict]) -> str:
    return "".join(
        json.dumps({k: v for k, v in e.items() if not k.startswith("_")},
                   ensure_ascii=False, separators=(",", ":")) + "\n"
        for e in eventos
    )


def montar_lotes(rng: random.Random, eventos: list[dict], origem: str, seed: int,
                 fracao_reenvio: float = 0.01) -> tuple[dict[str, str], int]:
    """Devolve ({chave S3: conteúdo JSONL}, número de duplicatas por reenvio)."""
    por_dia: dict[str, list[dict]] = defaultdict(list)
    for e in eventos:
        por_dia[e["_dt"]].append(e)

    arquivos: dict[str, str] = {}
    duplicatas = 0
    for dt in sorted(por_dia):
        do_dia = sorted(por_dia[dt], key=lambda e: e["event_ts"] or "")
        n = min(rng.randint(3, 6), len(do_dia))
        lotes = [do_dia[i * len(do_dia) // n:(i + 1) * len(do_dia) // n] for i in range(n)]
        reenvios: list[list[dict]] = [[] for _ in lotes]
        if len(lotes) > 1:
            for i, lote in enumerate(lotes):
                destino = i + 1 if i + 1 < len(lotes) else i - 1
                for e in lote:
                    if rng.random() < fracao_reenvio:
                        reenvios[destino].append(e)
                        duplicatas += 1
        for i, (lote, extra) in enumerate(zip(lotes, reenvios), start=1):
            arquivos[f"{PREFIXO}/{particao(dt)}/{origem}-s{seed}-{i:03d}.jsonl"] = serializar(lote + extra)
    return arquivos, duplicatas
