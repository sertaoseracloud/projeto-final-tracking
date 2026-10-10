"""Gold — sessões, retenção do vídeo, buscas no FAQ, saídas e downloads.

    docker compose run --rm app python professor/solucao/04_gold_engajamento.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from sessoes import atributos_sessao, tabela_sessoes

MARCOS = [10, 25, 50, 75, 100]


def retencao_video(eventos: DataFrame) -> DataFrame:
    por_sessao = (
        eventos.filter(F.col("event_name").startswith("video_")).groupBy("session_id")
        .agg(F.max((F.col("event_name") == "video_start").cast("int")).alias("iniciou"),
             F.max("video_percent").alias("maximo"))
        .filter("iniciou = 1")
    )
    somas = por_sessao.agg(F.count("*").alias("iniciaram"),
                           *[F.sum((F.col("maximo") >= m).cast("int")).alias(f"m{m}") for m in MARCOS])
    pilha = ", ".join(f"{m}, m{m}" for m in MARCOS)
    return (somas.select("iniciaram", F.expr(f"stack({len(MARCOS)}, {pilha}) as (marco, sessoes)"))
            .withColumn("percentual", F.round(F.col("sessoes") / F.col("iniciaram"), 4)))


def buscas_faq(eventos: DataFrame) -> DataFrame:
    return (eventos.filter(F.col("event_name") == "site_search").groupBy("search_term")
            .agg(F.count("*").alias("buscas"),
                 F.sum((F.col("search_results") == 0).cast("int")).alias("buscas_sem_resultado"))
            .orderBy(F.desc("buscas_sem_resultado"), F.desc("buscas"), "search_term"))


def saidas_downloads(eventos: DataFrame) -> DataFrame:
    e = F.col("event_name")
    saidas = (eventos.filter((e == "click") & F.coalesce(F.col("outbound"), F.lit(False)))
              .groupBy(F.col("link_domain").alias("alvo")).count().withColumn("tipo", F.lit("saida")))
    downloads = (eventos.filter(e == "file_download")
                 .groupBy(F.col("file_name").alias("alvo")).count().withColumn("tipo", F.lit("download")))
    return saidas.unionByName(downloads).select("tipo", "alvo", F.col("count").alias("quantidade"))


def main() -> None:
    spark = criar_sessao("gold-engajamento")
    exigir_camada(spark, glob_eventos("silver"), "python professor/solucao/02_silver_eventos.py")
    eventos = ler_eventos(spark, "silver").cache()
    atributos = atributos_sessao(eventos, spark.read.parquet(caminho("silver/itens")))

    tabelas = {
        "sessoes": tabela_sessoes(eventos, atributos),
        "retencao_video": retencao_video(eventos),
        "buscas_faq": buscas_faq(eventos),
        "saidas_downloads": saidas_downloads(eventos),
    }
    for nome, df in tabelas.items():
        df.coalesce(1).write.mode("overwrite").parquet(caminho(f"gold/{nome}"))

    titulo("Engajamento")
    print(f"sessões: {spark.read.parquet(caminho('gold/sessoes')).count()}")
    tabelas["retencao_video"].orderBy("marco").show(truncate=False)
    tabelas["buscas_faq"].show(20, truncate=False)
    tabelas["saidas_downloads"].orderBy("tipo", F.desc("quantidade")).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
