"""Consultas SQL no bucket com DuckDB — somente leitura.

A validação final usa o parser do próprio DuckDB (1 comando, só SELECT/EXPLAIN)."""

from __future__ import annotations

import re
from dataclasses import dataclass

import duckdb
import pandas as pd

from explorador.particoes import Dataset, formato_do_arquivo

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
    """Recusa o que não é uma única consulta de leitura; o último passo confirma com o parser do DuckDB."""
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
    # Confirmação autoritativa com o parser do DuckDB (strings $$...$$, E'...' etc.).
    try:
        comandos = duckdb.extract_statements(sql)
    except duckdb.Error:
        return  # erro de sintaxe: deixa o DuckDB explicar na execução
    if len(comandos) != 1:
        raise SqlNaoPermitido("Rode um comando por vez (sem `;` no meio).")
    tipo = comandos[0].type.name
    if tipo not in ("SELECT", "EXPLAIN"):
        raise SqlNaoPermitido(f"{MENSAGEM_SO_LEITURA} O comando `{tipo}` não é permitido.")


LIMITE_PADRAO = 1000
LIMITE_MAXIMO = 50_000
LINHAS_PREVIA = 100
OPCOES_LEITURA = "hive_partitioning=true, hive_types_autocast=false, union_by_name=true"


@dataclass
class Resultado:
    tabela: pd.DataFrame
    cortado: bool
    colunas: list[str]


def _texto_sql(valor: str) -> str:
    return "'" + valor.replace("'", "''") + "'"


def conectar(s3: bool = True, endpoint: str = "http://floci:4566", chave: str = "test",
             segredo: str = "test", regiao: str = "us-east-1") -> duckdb.DuckDBPyConnection:
    """Conexão DuckDB em memória. Com s3=True, aponta o S3 do DuckDB para o floci."""
    con = duckdb.connect()
    if s3:
        con.sql("LOAD httpfs")
        ssl = endpoint.startswith("https://")
        host = endpoint.removeprefix("https://").removeprefix("http://").rstrip("/")
        con.sql(
            "CREATE OR REPLACE SECRET floci (TYPE s3, "
            f"KEY_ID {_texto_sql(chave)}, SECRET {_texto_sql(segredo)}, REGION {_texto_sql(regiao)}, "
            f"ENDPOINT {_texto_sql(host)}, URL_STYLE 'path', USE_SSL {'true' if ssl else 'false'})"
        )
    return con


def _origem(raiz: str, dataset: Dataset) -> str:
    extensao = "parquet" if dataset.formato == "parquet" else "jsonl"
    funcao = "read_parquet" if dataset.formato == "parquet" else "read_json_auto"
    padrao = f"{raiz.rstrip('/')}/{dataset.prefixo}/**/*.{extensao}"
    return f"{funcao}({_texto_sql(padrao)}, {OPCOES_LEITURA})"


def criar_views(con: duckdb.DuckDBPyConnection, raiz: str, datasets: list[Dataset]) -> dict[str, str]:
    views = {}
    for dataset in datasets:
        origem = _origem(raiz, dataset)
        con.sql(f'CREATE OR REPLACE VIEW "{dataset.nome_view}" AS SELECT * FROM {origem}')
        views[dataset.nome_view] = origem
    return views


def travar(con: duckdb.DuckDBPyConnection, raiz: str) -> None:
    """Depois das views: só deixa ler dentro da raiz do bucket e congela a configuração."""
    pasta = raiz.rstrip("/") + "/"
    con.sql(f"SET allowed_directories = [{_texto_sql(pasta)}]")
    con.sql("SET enable_external_access = false")
    con.sql("SET lock_configuration = true")


def executar(con: duckdb.DuckDBPyConnection, sql: str, limite: int = LIMITE_PADRAO) -> Resultado:
    validar_sql(sql)
    limite = max(1, min(int(limite), LIMITE_MAXIMO))
    relacao = con.sql(sql)
    if relacao is None:
        return Resultado(pd.DataFrame(), False, [])
    tabela = relacao.limit(limite + 1).df()
    cortado = len(tabela) > limite
    tabela = tabela.head(limite)
    return Resultado(tabela, cortado, list(tabela.columns))


def previa(con: duckdb.DuckDBPyConnection, raiz: str, chave: str, linhas: int = LINHAS_PREVIA) -> pd.DataFrame:
    formato = formato_do_arquivo(chave)
    if formato is None:
        raise ValueError("Prévia só para arquivos .parquet e .jsonl.")
    funcao = "read_parquet" if formato == "parquet" else "read_json_auto"
    caminho = f"{raiz.rstrip('/')}/{chave}"
    return con.sql(f"SELECT * FROM {funcao}({_texto_sql(caminho)}) LIMIT {int(linhas)}").df()
