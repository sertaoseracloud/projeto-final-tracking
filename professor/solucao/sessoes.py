"""Transformações de sessão usadas por mais de um job Gold."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SEM_PLANO = "(nenhum)"


def _primeiro_por_sessao(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("session_id").orderBy("ts", "event_id")
    return df.withColumn("_ordem", F.row_number().over(w)).filter("_ordem = 1").drop("_ordem")


def atributos_sessao(eventos: DataFrame, itens: DataFrame) -> DataFrame:
    """Canal e dispositivo do primeiro evento da sessão; plano do primeiro select_item."""
    primeiro = _primeiro_por_sessao(eventos).select(
        "session_id",
        F.concat_ws(" / ", "source", "medium").alias("canal"),
        F.col("device_category").alias("dispositivo"),
    )
    plano = _primeiro_por_sessao(itens.filter(F.col("event_name") == "select_item")) \
        .select("session_id", F.col("item_id").alias("plano"))
    return primeiro.join(plano, "session_id", "left").fillna({"plano": SEM_PLANO})


def tabela_sessoes(eventos: DataFrame, atributos: DataFrame) -> DataFrame:
    e = F.col("event_name")
    resumo = eventos.groupBy("session_id").agg(
        F.first("user_pseudo_id").alias("user_pseudo_id"),
        F.first("user_id_resolvido", ignorenulls=True).alias("user_id_resolvido"),
        F.min("ts").alias("inicio"),
        F.max("ts").alias("fim"),
        F.count("*").alias("eventos"),
        F.coalesce(F.max("percent_scrolled"), F.lit(0)).alias("scroll_max"),
        F.max("video_percent").alias("video_max"),
        F.max(e == "video_start").alias("viu_video"),
        F.max(e == "generate_lead").alias("fez_lead"),
        F.max(e == "purchase").alias("comprou"),
    )
    return (resumo.withColumn("duracao_s", F.col("fim").cast("long") - F.col("inicio").cast("long"))
            .join(atributos, "session_id"))
