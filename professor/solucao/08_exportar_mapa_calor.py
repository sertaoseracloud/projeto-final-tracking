"""Exporta o mapa de calor da Gold para o site de exemplo (site/mapa_calor.js).

    docker compose run --rm app python professor/solucao/08_exportar_mapa_calor.py

Lê as duas tabelas pelo Spark (s3a). São poucas dezenas de linhas, então dá
para trazer tudo para o Python com collect() e gravar um arquivo .js que o
site carrega com <script src="mapa_calor.js">.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from comum import arquivo_do_projeto, caminho, criar_sessao, exigir_camada, titulo

DESTINO = arquivo_do_projeto("site/mapa_calor.js")       # e não Path(__file__): o script pode vir do floci
PREFIXO = "window.MAPA_CALOR = "


def montar_js(cliques: list[dict], rolagem: list[dict], gerado_em: str) -> str:
    dados = {"gerado_em": gerado_em, "cliques": cliques, "rolagem": rolagem}
    return PREFIXO + json.dumps(dados, ensure_ascii=False) + ";\n"


def gravar(texto: str, destino: Path) -> None:
    """Grava num temporário e renomeia: o site nunca lê um arquivo pela metade."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".tmp")
    temporario.write_text(texto, encoding="utf-8")
    os.replace(temporario, destino)


def avisos(cliques: list[dict], rolagem: list[dict]) -> list[str]:
    """Explica um mapa vazio em vez de deixar o site sem cor e sem motivo."""
    if cliques:
        return []
    return ["Nenhum clique com element_id na Silver: a landing provavelmente foi gerada antes do element_id. "
            "Rode de novo `python gerador/gerar_eventos.py` e o pipeline inteiro."]


def _linhas(spark, tabela: str, ordem: list[str]) -> list[dict]:
    return [linha.asDict() for linha in spark.read.parquet(caminho(f"gold/{tabela}")).orderBy(*ordem).collect()]


def main() -> None:
    spark = criar_sessao("exportar-mapa-calor")
    for tabela in ("mapa_calor_cliques", "mapa_calor_rolagem"):
        exigir_camada(spark, f"gold/{tabela}", "python professor/solucao/07_gold_mapa_calor.py")
    cliques = _linhas(spark, "mapa_calor_cliques", ["pagina", "element_id"])
    rolagem = _linhas(spark, "mapa_calor_rolagem", ["pagina", "marco"])
    spark.stop()

    gerado_em = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    gravar(montar_js(cliques, rolagem, gerado_em), DESTINO)
    titulo("Mapa de calor exportado")
    print(f"{len(cliques)} elementos e {len(rolagem)} marcos de rolagem em {DESTINO}")
    for aviso in avisos(cliques, rolagem):
        print(f"AVISO: {aviso}")
    print("Abra site/index.html e clique em \"Mapa de calor\" no painel.")


if __name__ == "__main__":
    main()
