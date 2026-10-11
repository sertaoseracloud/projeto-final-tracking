"""Roda os oito jobs da solução de referência em sequência. Não gera dados.

    docker compose run --rm app python professor/solucao/executar_pipeline.py
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

AQUI = Path(__file__).resolve().parent
JOBS = [
    "01_bronze_ingestao.py",
    "02_silver_eventos.py",
    "03_gold_funil.py",
    "04_gold_engajamento.py",
    "05_gold_receita_aquisicao.py",
    "06_gold_ml.py",
    "07_gold_mapa_calor.py",
    "08_exportar_mapa_calor.py",
]


def main() -> int:
    for job in JOBS:
        inicio = time.time()
        print(f"\n>>> {job}", flush=True)
        resultado = subprocess.run([sys.executable, str(AQUI / job)])
        if resultado.returncode != 0:
            print(f"!!! {job} falhou (código {resultado.returncode})", flush=True)
            return resultado.returncode
        print(f"<<< {job} ok em {time.time() - inicio:.0f} s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
