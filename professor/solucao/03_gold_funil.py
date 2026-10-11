"""Gold — funil de compra (por plano × canal) e funil de lead (por canal).

    docker compose run --rm app python professor/solucao/03_gold_funil.py

Cada etapa conta as sessões que tiveram aquele evento, sem exigir as
anteriores. view_item é opcional na jornada: fica fora da sequência e vira
taxa_view_item. add_shipping_info só existe no Premium, então só entra no
recorte desse plano.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from sessoes import atributos_sessao

PREMIUM = "PLANO-PREMIUM"
LANDING = "https://loja.exemplo/"
TODOS = "(todos)"
ETAPAS = ["view_item_list", "select_item", "add_to_cart", "view_cart", "begin_checkout",
          "add_payment_info", "purchase"]
ETAPAS_PREMIUM = ETAPAS[:5] + ["add_shipping_info"] + ETAPAS[5:]
ETAPAS_LEAD = ["visita_landing", "scroll_50", "generate_lead"]
DIMS = ["canal", "plano"]


def flags_por_sessao(eventos: DataFrame, etapas: list[str]) -> DataFrame:
    return eventos.groupBy("session_id").agg(
        *[F.max((F.col("event_name") == e).cast("int")).alias(e) for e in etapas])


def contar_etapas(flags: DataFrame, dims: list[str], etapas: list[str]) -> DataFrame:
    somas = flags.groupBy(*dims).agg(*[F.sum(e).alias(e) for e in etapas])
    pilha = ", ".join(f"'{e}', {i}, `{e}`" for i, e in enumerate(etapas, start=1))
    linhas = somas.select(*dims, F.expr(f"stack({len(etapas)}, {pilha}) as (etapa, ordem, sessoes)"))
    w = Window.partitionBy(*dims).orderBy("ordem")
    return (linhas
            .withColumn("conv_etapa_anterior", F.round(F.try_divide(F.col("sessoes"), F.lag("sessoes").over(w)), 4))
            .withColumn("conv_topo", F.round(F.try_divide(F.col("sessoes"), F.first("sessoes").over(w)), 4)))


def _todos(df: DataFrame, dims: list[str]) -> DataFrame:
    for d in dims:
        df = df.withColumn(d, F.lit(TODOS))
    return df


def funil_compra(eventos: DataFrame, atributos: DataFrame) -> DataFrame:
    flags = flags_por_sessao(eventos, ETAPAS_PREMIUM + ["view_item"]).join(atributos, "session_id")
    total = _todos(flags, DIMS)
    funil = (
        contar_etapas(total, DIMS, ETAPAS).withColumn("escopo", F.lit("total"))
        .unionByName(contar_etapas(flags.filter(F.col("plano") != PREMIUM), DIMS, ETAPAS)
                     .withColumn("escopo", F.lit("canal_plano")))
        .unionByName(contar_etapas(flags.filter(F.col("plano") == PREMIUM), DIMS, ETAPAS_PREMIUM)
                     .withColumn("escopo", F.lit("canal_plano")))
    )
    taxa = total.unionByName(flags).groupBy(*DIMS).agg(
        F.round(F.try_divide(F.sum("view_item"), F.sum("select_item")), 4).alias("taxa_view_item"))
    return funil.join(taxa, DIMS, "left")


def funil_lead(eventos: DataFrame, atributos: DataFrame) -> DataFrame:
    e = F.col("event_name")
    flags = eventos.groupBy("session_id").agg(
        F.max(((e == "page_view") & (F.col("page_location") == LANDING)).cast("int")).alias("visita_landing"),
        F.max(((e == "scroll") & (F.col("percent_scrolled") >= 50)).cast("int")).alias("scroll_50"),
        F.max((e == "generate_lead").cast("int")).alias("generate_lead"),
    ).join(atributos.select("session_id", "canal"), "session_id")
    return contar_etapas(_todos(flags, ["canal"]).unionByName(flags), ["canal"], ETAPAS_LEAD)


def main() -> None:
    spark = criar_sessao("gold-funil")
    exigir_camada(spark, glob_eventos("silver"), "python professor/solucao/02_silver_eventos.py")
    eventos = ler_eventos(spark, "silver")
    atributos = atributos_sessao(eventos, spark.read.parquet(caminho("silver/itens"))).cache()

    compra = funil_compra(eventos, atributos).cache()
    lead = funil_lead(eventos, atributos).cache()
    compra.coalesce(1).write.mode("overwrite").parquet(caminho("gold/funil_compra"))
    lead.coalesce(1).write.mode("overwrite").parquet(caminho("gold/funil_lead"))

    titulo("Funil de compra — total")
    compra.filter("escopo = 'total'").orderBy("ordem") \
        .select("etapa", "sessoes", "conv_etapa_anterior", "conv_topo", "taxa_view_item").show(truncate=False)
    titulo("Funil de lead por canal")
    lead.orderBy("canal", "ordem").show(30, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
