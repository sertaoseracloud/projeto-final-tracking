"""Lista o bucket do floci com boto3. O explorador só lê: nada aqui grava ou apaga."""

from __future__ import annotations

import os

from explorador.particoes import Objeto

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL") or "http://floci:4566"
BUCKET = os.environ.get("AWS_S3_BUCKET") or "datalake-tracking"
CHAVE = os.environ.get("AWS_ACCESS_KEY_ID") or "test"
SEGREDO = os.environ.get("AWS_SECRET_ACCESS_KEY") or "test"
REGIAO = os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
ERROS_DE_CONEXAO = {"EndpointConnectionError", "ConnectTimeoutError", "ConnectionClosedError",
                    "ReadTimeoutError", "ConnectionError"}


class S3Indisponivel(RuntimeError):
    """O floci não respondeu; a mensagem vai direto para o usuário."""


def raiz() -> str:
    return f"s3://{BUCKET}"


def _cliente():
    import boto3
    from botocore.config import Config

    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=CHAVE,
                        aws_secret_access_key=SEGREDO, region_name=REGIAO,
                        config=Config(connect_timeout=3, read_timeout=10, retries={"max_attempts": 1}))


def listar(prefixo: str = "", cliente=None) -> list[Objeto]:
    s3 = cliente if cliente is not None else _cliente()
    try:
        objetos = []
        for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefixo):
            for item in pagina.get("Contents", []):
                objetos.append(Objeto(item["Key"], item["Size"], item.get("LastModified")))
        return objetos
    except Exception as erro:
        codigo = getattr(erro, "response", {}).get("Error", {}).get("Code")
        if codigo in ("NoSuchBucket", "404"):
            return []
        if type(erro).__name__ in ERROS_DE_CONEXAO:
            raise S3Indisponivel(
                f"Não consegui falar com o S3 em {ENDPOINT}. Rode `docker compose up -d floci`."
            ) from None
        raise
