"""Apaga bronze/, silver/ e gold/ do bucket (mantém a landing), pelo FileSystem do Hadoop.

    docker compose run --rm app python professor/solucao/limpar_camadas.py
"""

from __future__ import annotations

from comum import apagar_camada, criar_sessao

CAMADAS = ["bronze", "silver", "gold"]


def main() -> None:
    spark = criar_sessao("limpar-camadas")
    for camada in CAMADAS:
        print(f"{camada:<7} {'apagada' if apagar_camada(spark, camada) else 'não existia'}")
    spark.stop()


if __name__ == "__main__":
    main()
