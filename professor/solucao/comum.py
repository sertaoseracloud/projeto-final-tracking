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


# Os eventos de cada camada ficam em <camada>/event_name=<nome>/year=…/month=…/day=…
# (sem pasta "eventos"). Na Silver, itens/ e eventos_rejeitados/ ficam ao lado das partições.
PARTICAO_EVENTOS = "event_name=*"
COLUNAS_PARTICAO = ["event_name", "year", "month", "day"]


def glob_eventos(camada: str) -> str:
    """'silver' -> 'silver/event_name=*': o glob que pega só as partições de eventos."""
    return f"{camada}/{PARTICAO_EVENTOS}"


def ler_eventos(spark, camada: str, formato: str = "parquet"):
    """Lê só as pastas event_name=… da camada. O basePath faz o Spark devolver event_name
    como coluna e ignorar as pastas vizinhas (silver/itens, silver/eventos_rejeitados)."""
    leitor = spark.read.option("basePath", caminho(camada))
    return leitor.format(formato).load(caminho(glob_eventos(camada)))


def gravar_eventos(df, camada: str, modo: str) -> None:
    """Grava particionado por event_name/year/month/day direto na raiz da camada.

    Com "overwrite", o modo dinâmico troca só as partições presentes no DataFrame:
    sem ele, o Spark apagaria a pasta inteira da camada (e, na Silver, itens/ e
    eventos_rejeitados/ junto)."""
    (df.write.mode(modo).option("partitionOverwriteMode", "dynamic")
       .partitionBy(*COLUNAS_PARTICAO).parquet(caminho(camada)))


def _sistema_de_arquivos(spark, uri: str):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()), path


def existe(spark, uri: str) -> bool:
    """Aceita glob (ex.: .../silver/event_name=*): existe se alguma pasta casar."""
    fs, path = _sistema_de_arquivos(spark, uri)
    if "*" in uri:
        return bool(fs.globStatus(path))
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
