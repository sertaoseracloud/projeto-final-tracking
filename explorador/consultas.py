"""Consultas SQL no bucket com DuckDB — somente leitura."""

from __future__ import annotations

import re

MENSAGEM_SO_LEITURA = "O explorador só faz consultas de leitura (SELECT, WITH, DESCRIBE…)."
INICIOS_PERMITIDOS = ("select", "with", "describe", "show", "summarize", "from", "explain", "pragma")
PROIBIDAS = ("copy", "attach", "detach", "install", "load", "create", "drop", "insert", "update",
             "delete", "alter", "export", "import", "set", "call", "checkpoint", "vacuum", "use")


class SqlNaoPermitido(ValueError):
    """SQL que o explorador não executa; a mensagem vai direto para o usuário."""


def _somente_codigo(sql: str) -> str:
    """Troca o conteúdo de strings por vazio e remove comentários, para analisar só o código."""
    saida, i, n = [], 0, len(sql)
    while i < n:
        if sql.startswith("--", i):
            fim = sql.find("\n", i)
            i = n if fim < 0 else fim
            continue
        if sql.startswith("/*", i):
            fim = sql.find("*/", i + 2)
            i = n if fim < 0 else fim + 2
            saida.append(" ")
            continue
        aspa = sql[i]
        if aspa in "'\"":
            j = i + 1
            while j < n:
                if sql[j] == aspa:
                    if j + 1 < n and sql[j + 1] == aspa:
                        j += 2
                        continue
                    break
                j += 1
            saida.append(aspa * 2)
            i = j + 1
            continue
        saida.append(aspa)
        i += 1
    return "".join(saida)


def validar_sql(sql: str) -> None:
    codigo = _somente_codigo(sql).strip().rstrip(";").strip()
    if not codigo:
        raise SqlNaoPermitido("Escreva uma consulta SQL.")
    if ";" in codigo:
        raise SqlNaoPermitido("Rode um comando por vez (sem `;` no meio).")
    palavras = re.findall(r"[a-z_][a-z0-9_]*", codigo.lower())
    primeira = palavras[0] if palavras else ""
    if primeira not in INICIOS_PERMITIDOS:
        raise SqlNaoPermitido(MENSAGEM_SO_LEITURA)
    if primeira == "pragma" and not re.match(r"pragma\s+table_info\b", codigo.lower()):
        raise SqlNaoPermitido(MENSAGEM_SO_LEITURA)
    proibida = next((p for p in palavras if p in PROIBIDAS), None)
    if proibida:
        raise SqlNaoPermitido(f"{MENSAGEM_SO_LEITURA} O comando `{proibida.upper()}` não é permitido.")
