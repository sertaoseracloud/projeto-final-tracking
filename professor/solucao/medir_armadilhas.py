"""Mede, nos dados gravados, como cada armadilha da seção 9 do spec aparece.

    docker compose run --rm app python professor/solucao/medir_armadilhas.py

Uso do professor, para o GABARITO.md. Precisa de Bronze e Silver gravadas.
"""

from __future__ import annotations

import importlib

from pyspark.sql import functions as F

from comum import caminho, criar_sessao, exigir_camada, titulo
from sessoes import atributos_sessao

funil = importlib.import_module("03_gold_funil")
receita = importlib.import_module("05_gold_receita_aquisicao")
ml = importlib.import_module("06_gold_ml")


def metricas_em_texto(m: dict) -> str:
    return f"AUC {m['auc']}   F1 {m['f1']}   precisão {m['precisao']}   recall {m['recall']}"


def main() -> None:
    spark = criar_sessao("armadilhas")
    exigir_camada(spark, "silver/eventos", "python professor/solucao/executar_pipeline.py")
    bronze = spark.read.parquet(caminho("bronze/eventos"))
    eventos = spark.read.parquet(caminho("silver/eventos")).cache()
    itens = spark.read.parquet(caminho("silver/itens"))
    atributos = atributos_sessao(eventos, itens).cache()

    titulo("1. Frete como etapa obrigatória para todos os planos")
    flags = funil.flags_por_sessao(eventos, funil.ETAPAS_PREMIUM).join(atributos, "session_id")
    errado = funil.contar_etapas(flags.withColumn("canal", F.lit("(todos)")), ["canal", "plano"], funil.ETAPAS_PREMIUM)
    errado.filter(F.col("etapa").isin("begin_checkout", "add_shipping_info", "add_payment_info", "purchase")) \
        .orderBy("plano", "ordem").select("plano", "etapa", "sessoes", "conv_etapa_anterior").show(40, truncate=False)

    titulo("2. Reembolso descontado na data do próprio refund")
    compras = receita.compras(eventos, itens, atributos)
    refund = (eventos.filter(F.col("event_name") == "refund")
              .select("transaction_id", F.to_date("ts").alias("data"), F.col("value").alias("reembolso")))
    por_data_errada = (
        compras.select(F.col("data_compra").alias("data"), "plano", "canal", F.col("value").alias("bruta"))
        .unionByName(refund.join(compras.select("transaction_id", "plano", "canal"), "transaction_id")
                     .select("data", "plano", "canal", (-F.col("reembolso")).alias("bruta")))
        .groupBy("data", "plano", "canal").agg(F.round(F.sum("bruta"), 2).alias("liquida"))
    )
    total = por_data_errada.count()
    negativas = por_data_errada.filter("liquida < 0").count()
    print(f"células (data × plano × canal) com receita líquida negativa: {negativas} de {total}")
    por_data_errada.filter("liquida < 0").orderBy("data").show(truncate=False)

    titulo("3. Vazamento: features do checkout no modelo")
    honesto = ml.features_sessao(eventos, atributos)
    checkout = funil.flags_por_sessao(eventos, ["view_cart", "begin_checkout", "add_payment_info"])
    _, sem_vazar = ml.treinar_propensao(honesto)
    _, vazando = ml.treinar_propensao(honesto.join(checkout, "session_id"),
                                      ml.NUMERICAS + ["view_cart", "begin_checkout", "add_payment_info"])
    print(f"Só com a landing antes do carrinho: {metricas_em_texto(sem_vazar)}")
    print(f"Com eventos do checkout:            {metricas_em_texto(vazando)}")

    titulo("4. Sem deduplicar por event_id")
    compras_bronze = bronze.filter(F.col("event_name") == "purchase")
    print(f"purchase na Bronze (com reenvios): {compras_bronze.count()}   "
          f"event_id distintos: {compras_bronze.select('event_id').distinct().count()}")
    print(f"linhas na Bronze: {bronze.count()}   event_id distintos: {bronze.select('event_id').distinct().count()}")

    titulo("5. Sessões contadas pelo session_hint do navegador")
    print(f"session_hint distintos: {eventos.select('session_hint').distinct().count()}   "
          f"sessões (30 min): {eventos.select('session_id').distinct().count()}")
    spark.stop()


if __name__ == "__main__":
    main()
