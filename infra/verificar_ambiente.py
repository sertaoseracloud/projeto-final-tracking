"""Confere que o Spark lê e grava no floci por s3a:// (rode dentro do contêiner app).

    docker compose run --rm app python infra/verificar_ambiente.py
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://floci:4566")
BUCKET = os.environ.get("AWS_S3_BUCKET", "datalake-tracking")


def criar_sessao() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("verificar-ambiente")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # partições year=AAAA/month=MM/day=DD: sem inferência, month continua "09" (e não 9)
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


def main() -> int:
    spark = criar_sessao()
    landing = f"s3a://{BUCKET}/landing/"
    teste = f"s3a://{BUCKET}/_verificacao/"
    try:
        eventos = spark.read.json(landing)
        print(f"[ok] landing lida por s3a: {eventos.count()} linhas em {landing}")
        eventos.groupBy("event_name").count().orderBy(F.desc("count")).show(25, truncate=False)

        spark.range(5).write.mode("overwrite").parquet(teste)
        lidos = spark.read.parquet(teste).count()
        print(f"[ok] escrita e leitura de Parquet por s3a: {lidos} linhas em {teste}")

        caminho = spark._jvm.org.apache.hadoop.fs.Path(teste)
        caminho.getFileSystem(spark._jsc.hadoopConfiguration()).delete(caminho, True)
        print("[ok] pasta de verificação removida")
    except Exception as erro:  # diagnóstico para o aluno
        print(f"[erro] {type(erro).__name__}: {str(erro).splitlines()[0]}", file=sys.stderr)
        print("Confira: docker compose up -d floci, e rode o gerador antes.", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
