from datetime import datetime, timezone

import pytest

from explorador import s3
from explorador.particoes import Objeto

AGORA = datetime(2026, 10, 10, tzinfo=timezone.utc)


class Paginador:
    def __init__(self, paginas):
        self.paginas = paginas
        self.argumentos = None

    def paginate(self, **argumentos):
        self.argumentos = argumentos
        return iter(self.paginas)


class S3Falso:
    def __init__(self, paginas=(), erro=None):
        self.paginador = Paginador(list(paginas))
        self.erro = erro

    def get_paginator(self, nome):
        assert nome == "list_objects_v2"
        if self.erro:
            raise self.erro
        return self.paginador


class EndpointConnectionError(Exception):
    pass


class ErroS3(Exception):
    def __init__(self, codigo):
        super().__init__(codigo)
        self.response = {"Error": {"Code": codigo}}


def test_listar_junta_todas_as_paginas():
    falso = S3Falso([
        {"Contents": [{"Key": "a/1.parquet", "Size": 10, "LastModified": AGORA}]},
        {"Contents": [{"Key": "a/2.parquet", "Size": 20, "LastModified": AGORA}]},
        {},
    ])
    assert s3.listar("a/", cliente=falso) == [Objeto("a/1.parquet", 10, AGORA), Objeto("a/2.parquet", 20, AGORA)]
    assert falso.paginador.argumentos == {"Bucket": s3.BUCKET, "Prefix": "a/"}


def test_bucket_inexistente_vira_lista_vazia():
    assert s3.listar(cliente=S3Falso(erro=ErroS3("NoSuchBucket"))) == []


def test_floci_desligado_vira_mensagem_amigavel():
    with pytest.raises(s3.S3Indisponivel, match="docker compose up -d floci"):
        s3.listar(cliente=S3Falso(erro=EndpointConnectionError("sem rede")))


def test_outros_erros_sobem_como_estao():
    with pytest.raises(ErroS3):
        s3.listar(cliente=S3Falso(erro=ErroS3("AccessDenied")))


def test_raiz_usa_o_bucket():
    assert s3.raiz() == f"s3://{s3.BUCKET}"
