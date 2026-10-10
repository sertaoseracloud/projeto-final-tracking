"""Configuração compartilhada pelos jobs da solução de referência.

O pipeline fala com o S3 (floci) só pelo Spark, via s3a://. Nenhum SDK de
nuvem: para saber se uma camada existe, ou para apagá-la, usa o FileSystem
do Hadoop pela JVM do próprio Spark.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL") or "http://floci:4566"
BUCKET = os.environ.get("AWS_S3_BUCKET", "datalake-tracking")


def criar_sessao(nome: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(nome)
        .master("local[4]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        # year=2026/month=09/day=01: sem inferência, month continua "09" (e não 9) ao regravar
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def caminho(camada: str) -> str:
    return f"s3a://{BUCKET}/{camada}"


def _sistema_de_arquivos(spark, uri: str):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()), path


def existe(spark, uri: str) -> bool:
    fs, path = _sistema_de_arquivos(spark, uri)
    return bool(fs.exists(path))


def camada_existe(spark, camada: str) -> bool:
    return existe(spark, caminho(camada))


def exigir_camada(spark, camada: str, comando: str) -> None:
    if not camada_existe(spark, camada):
        spark.stop()
        raise SystemExit(f"{caminho(camada)} não existe. Rode antes: {comando}")


def apagar_camada(spark, camada: str) -> bool:
    fs, path = _sistema_de_arquivos(spark, caminho(camada))
    return bool(fs.exists(path) and fs.delete(path, True))


def titulo(texto: str) -> None:
    print(f"\n{'=' * 70}\n{texto}\n{'=' * 70}")
