"""Bronze — lê a landing (JSON Lines) com schema fixo e grava Parquet, sem limpar nada.

    docker compose run --rm app python professor/solucao/01_bronze_ingestao.py

Append-only: rodar duas vezes duplica a Bronze, de propósito. Quem
deduplica é a Silver, por event_id.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, LongType, MapType, StringType,
                               StructField, StructType)

from comum import caminho, criar_sessao, exigir_camada, titulo


def _texto(*nomes: str) -> list[StructField]:
    return [StructField(n, StringType()) for n in nomes]


ITEM = StructType(_texto("item_id", "item_name", "item_category") + [
    StructField("price", DoubleType()),
    StructField("quantity", LongType()),
    StructField("index", LongType()),
] + _texto("item_list_id", "item_list_name"))

SCHEMA = StructType(_texto("event_id", "event_name", "event_ts", "user_pseudo_id", "user_id", "session_hint") + [
    StructField("page", StructType(_texto("location", "referrer"))),
    StructField("traffic", StructType(_texto("source", "medium", "campaign"))),
    StructField("device", StructType(_texto("category", "os", "browser"))),
    StructField("params", MapType(StringType(), StringType())),   # números e booleanos chegam como texto
    StructField("items", ArrayType(ITEM)),
])


PARTICAO = r"year=(\d{4})/month=(\d{2})/day=(\d{2})"   # dia de coleta, tirado do caminho
DATA = ["year", "month", "day"]


def ler_landing(spark, uri: str) -> DataFrame:
    return spark.read.schema(SCHEMA).json(uri)


def adicionar_proveniencia(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_arquivo_origem", F.input_file_name())
          .withColumn("_ingerido_em", F.current_timestamp())
          .withColumn("year", F.regexp_extract("_arquivo_origem", PARTICAO, 1))
          .withColumn("month", F.regexp_extract("_arquivo_origem", PARTICAO, 2))
          .withColumn("day", F.regexp_extract("_arquivo_origem", PARTICAO, 3))
    )


def main() -> None:
    spark = criar_sessao("bronze")
    exigir_camada(spark, "landing/eventos", "python gerador/gerar_eventos.py")
    bronze = adicionar_proveniencia(ler_landing(spark, caminho("landing/eventos"))).cache()
    bronze.write.mode("append").partitionBy(*DATA).parquet(caminho("bronze/eventos"))

    titulo("Bronze")
    print(f"linhas lidas da landing e gravadas: {bronze.count()}")
    print(f"arquivos de origem:                 {bronze.select('_arquivo_origem').distinct().count()}")
    print(f"total acumulado na Bronze:          {spark.read.parquet(caminho('bronze/eventos')).count()}")
    spark.stop()


if __name__ == "__main__":
    main()
