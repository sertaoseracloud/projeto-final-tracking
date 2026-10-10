"""Gold — receita líquida, reembolso, order bump e conversão de leads.

    docker compose run --rm app python professor/solucao/05_gold_receita_aquisicao.py

O refund desconta na data da COMPRA original (join por transaction_id),
nunca na data do próprio refund.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from comum import caminho, criar_sessao, exigir_camada, titulo
from sessoes import atributos_sessao

BUMP = "BUMP-MENTORIA"
TODOS = "(todos)"


def compras(eventos: DataFrame, itens: DataFrame, atributos: DataFrame) -> DataFrame:
    base = eventos.filter(F.col("event_name") == "purchase").select(
        "transaction_id", "session_id", "ts", F.to_date("ts").alias("data_compra"), "value",
        "user_id_resolvido", "user_pseudo_id")
    composicao = (itens.filter(F.col("event_name") == "purchase").groupBy("transaction_id")
                  .agg(F.max(F.when(F.col("item_category") == "plano", F.col("item_id"))).alias("plano"),
                       F.max((F.col("item_id") == BUMP).cast("int")).alias("com_bump")))
    return (base.join(composicao, "transaction_id", "left")
            .join(atributos.select("session_id", "canal"), "session_id", "left"))


def reembolsos(eventos: DataFrame) -> DataFrame:
    return (eventos.filter(F.col("event_name") == "refund").groupBy("transaction_id")
            .agg(F.sum("value").alias("valor_reembolsado")))


def _com_reembolso(compras: DataFrame, reembolsos: DataFrame) -> DataFrame:
    return compras.join(reembolsos, "transaction_id", "left").fillna({"valor_reembolsado": 0.0})


def _com_total(df: DataFrame, dims: list[str]) -> DataFrame:
    total = df
    for d in dims:
        total = total.withColumn(d, F.lit(TODOS))
    return df.unionByName(total)


def receita_diaria(compras: DataFrame, reembolsos: DataFrame) -> DataFrame:
    return (_com_reembolso(compras, reembolsos).groupBy("data_compra", "plano", "canal")
            .agg(F.round(F.sum("value"), 2).alias("receita_bruta"),
                 F.round(F.sum("valor_reembolsado"), 2).alias("receita_reembolsada"),
                 F.count("*").alias("pedidos"))
            .withColumn("receita_liquida", F.round(F.col("receita_bruta") - F.col("receita_reembolsada"), 2))
            .withColumn("ticket_medio", F.round(F.col("receita_bruta") / F.col("pedidos"), 2)))


def taxa_reembolso(compras: DataFrame, reembolsos: DataFrame) -> DataFrame:
    base = _com_total(_com_reembolso(compras, reembolsos), ["plano", "canal"])
    return (base.groupBy("plano", "canal")
            .agg(F.count("*").alias("pedidos"),
                 F.sum((F.col("valor_reembolsado") > 0).cast("int")).alias("reembolsados"))
            .withColumn("taxa_reembolso", F.round(F.col("reembolsados") / F.col("pedidos"), 4)))


def adesao_order_bump(compras: DataFrame) -> DataFrame:
    return (_com_total(compras, ["plano"]).groupBy("plano")
            .agg(F.count("*").alias("pedidos"), F.sum("com_bump").alias("com_bump"))
            .withColumn("adesao", F.round(F.col("com_bump") / F.col("pedidos"), 4)))


def lead_para_compra(eventos: DataFrame, compras: DataFrame, atributos: DataFrame) -> DataFrame:
    """Primeiro lead de cada pessoa e a primeira compra DEPOIS dele."""
    pessoa = F.coalesce("user_id_resolvido", "user_pseudo_id")
    w = Window.partitionBy("pessoa").orderBy("ts", "event_id")
    leads = (eventos.filter(F.col("event_name") == "generate_lead").withColumn("pessoa", pessoa)
             .withColumn("_ordem", F.row_number().over(w)).filter("_ordem = 1")
             .join(atributos.select("session_id", "canal"), "session_id", "left")
             .select("pessoa", F.col("ts").alias("lead_em"), "canal"))
    posteriores = (compras.withColumn("pessoa", pessoa).join(leads.select("pessoa", "lead_em"), "pessoa")
                   .filter(F.col("ts") > F.col("lead_em"))
                   .groupBy("pessoa").agg(F.min("ts").alias("compra_em")))
    base = (leads.join(posteriores, "pessoa", "left")
            .withColumn("dias", (F.col("compra_em").cast("long") - F.col("lead_em").cast("long")) / 86400.0))
    return (_com_total(base, ["canal"]).groupBy("canal")
            .agg(F.count("*").alias("leads"), F.count("compra_em").alias("compraram_depois"),
                 F.round(F.percentile_approx("dias", 0.5), 2).alias("mediana_dias"))
            .withColumn("taxa_conversao", F.round(F.col("compraram_depois") / F.col("leads"), 4)))


def main() -> None:
    spark = criar_sessao("gold-receita")
    exigir_camada(spark, "silver/eventos", "python professor/solucao/02_silver_eventos.py")
    eventos = spark.read.parquet(caminho("silver/eventos")).cache()
    itens = spark.read.parquet(caminho("silver/itens"))
    atributos = atributos_sessao(eventos, itens).cache()
    pedidos = compras(eventos, itens, atributos).cache()
    devolvidos = reembolsos(eventos)

    tabelas = {
        "receita_diaria": receita_diaria(pedidos, devolvidos),
        "taxa_reembolso": taxa_reembolso(pedidos, devolvidos),
        "adesao_order_bump": adesao_order_bump(pedidos),
        "lead_para_compra": lead_para_compra(eventos, pedidos, atributos),
    }
    for nome, df in tabelas.items():
        df.coalesce(1).write.mode("overwrite").parquet(caminho(f"gold/{nome}"))

    titulo("Receita (R$)")
    tabelas["receita_diaria"].agg(F.round(F.sum("receita_bruta"), 2).alias("bruta"),
                                  F.round(F.sum("receita_reembolsada"), 2).alias("reembolsada"),
                                  F.round(F.sum("receita_liquida"), 2).alias("liquida"),
                                  F.sum("pedidos").alias("pedidos")).show(truncate=False)
    tabelas["taxa_reembolso"].orderBy("plano", "canal").show(30, truncate=False)
    tabelas["adesao_order_bump"].orderBy("plano").show(truncate=False)
    tabelas["lead_para_compra"].orderBy("canal").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
