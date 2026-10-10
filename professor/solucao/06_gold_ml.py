"""Gold — propensão de compra por sessão e segmentação de visitantes.

    docker compose run --rm app python professor/solucao/06_gold_ml.py

As features só olham a landing ANTES do primeiro add_to_cart. Qualquer
evento do checkout (view_cart em diante) praticamente entrega a resposta,
e o modelo "acertaria" olhando a própria compra.

As classes são ponderadas no treino porque compradores são raros (~6%): sem
pesos o modelo quase nunca prevê compra e o F1 da classe positiva fica ~0.
"""

from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (BinaryClassificationEvaluator, ClusteringEvaluator,
                                   MulticlassClassificationEvaluator)
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from comum import caminho, criar_sessao, exigir_camada, titulo
from sessoes import atributos_sessao

SEED = 42
CATEGORICAS = ["canal", "dispositivo", "plano"]
NUMERICAS = ["scroll_max", "video_max", "viu_planos", "selecionou_plano", "abriu_detalhe",
             "buscas", "downloads", "saidas", "page_views"]
PERFIL = ["sessoes", "eventos_medios", "scroll_medio", "taxa_video", "fez_lead", "comprou"]

# pyspark.ml.functions.vector_to_array exige pandas, que a imagem não tem.
prob_compra = F.udf(lambda v: float(v[1]), DoubleType())


def features_sessao(eventos: DataFrame, atributos: DataFrame) -> DataFrame:
    e = F.col("event_name")
    corte = eventos.filter(e == "add_to_cart").groupBy("session_id").agg(F.min("ts").alias("corte"))
    antes = eventos.join(corte, "session_id", "left").filter(F.col("corte").isNull() | (F.col("ts") < F.col("corte")))

    def houve(cond):
        return F.max(cond.cast("int"))

    def conta(cond):
        return F.sum(cond.cast("int"))

    features = antes.groupBy("session_id").agg(
        F.coalesce(F.max("percent_scrolled"), F.lit(0)).alias("scroll_max"),
        F.coalesce(F.max("video_percent"), F.lit(0)).alias("video_max"),
        houve(e == "view_item_list").alias("viu_planos"),
        houve(e == "select_item").alias("selecionou_plano"),
        houve(e == "view_item").alias("abriu_detalhe"),
        conta(e == "site_search").alias("buscas"),
        conta(e == "file_download").alias("downloads"),
        conta((e == "click") & F.coalesce(F.col("outbound"), F.lit(False))).alias("saidas"),
        conta(e == "page_view").alias("page_views"),
    )
    rotulo = eventos.groupBy("session_id").agg(F.max((e == "purchase").cast("double")).alias("rotulo"))
    return features.join(atributos, "session_id").join(rotulo, "session_id")


def treinar_propensao(dados: DataFrame, numericas: list[str] = NUMERICAS) -> tuple[DataFrame, dict]:
    dados = dados.cache()
    treino, teste = dados.randomSplit([0.8, 0.2], seed=SEED)
    # Compradores são raros: cada classe pesa 50% do total, senão o modelo prevê "não compra" sempre.
    p = treino.filter(F.col("rotulo") == 1.0).count() / max(treino.count(), 1)
    peso_pos, peso_neg = (0.5 / p, 0.5 / (1 - p)) if 0 < p < 1 else (1.0, 1.0)
    treino_pesado = treino.withColumn("peso", F.when(F.col("rotulo") == 1.0, peso_pos).otherwise(peso_neg))
    indices = [f"{c}_idx" for c in CATEGORICAS]
    vetores = [f"{c}_vec" for c in CATEGORICAS]
    pipeline = Pipeline(stages=[
        *[StringIndexer(inputCol=c, outputCol=i, handleInvalid="keep") for c, i in zip(CATEGORICAS, indices)],
        OneHotEncoder(inputCols=indices, outputCols=vetores, handleInvalid="keep"),
        VectorAssembler(inputCols=numericas + vetores, outputCol="features"),
        LogisticRegression(labelCol="rotulo", featuresCol="features", weightCol="peso", maxIter=100),
    ])
    modelo = pipeline.fit(treino_pesado)
    previsto = modelo.transform(teste)
    auc = BinaryClassificationEvaluator(labelCol="rotulo", metricName="areaUnderROC").evaluate(previsto)
    f1 = MulticlassClassificationEvaluator(labelCol="rotulo", metricName="fMeasureByLabel",
                                           metricLabel=1.0).evaluate(previsto)
    avalia = lambda nome: MulticlassClassificationEvaluator(  # noqa: E731
        labelCol="rotulo", metricName=nome, metricLabel=1.0).evaluate(previsto)
    precisao, recall = avalia("precisionByLabel"), avalia("recallByLabel")
    pontuado = modelo.transform(
        treino.withColumn("conjunto", F.lit("treino")).unionByName(teste.withColumn("conjunto", F.lit("teste")))
    ).select("session_id", "conjunto", "rotulo",
             F.round(prob_compra("probability"), 4).alias("probabilidade"))
    metricas = {"auc": round(auc, 4), "f1": round(f1, 4),
                "precisao": round(precisao, 4), "recall": round(recall, 4), "n_treino": treino.count(),
                "n_teste": teste.count(), "features": ", ".join(numericas + CATEGORICAS)}
    return pontuado, metricas


def perfil_visitantes(eventos: DataFrame) -> DataFrame:
    e = F.col("event_name")
    por_sessao = eventos.groupBy("user_pseudo_id", "session_id").agg(
        F.count("*").alias("eventos"),
        F.coalesce(F.max("percent_scrolled"), F.lit(0)).alias("scroll_max"),
        F.max((e == "video_start").cast("int")).alias("viu_video"),
        F.max((e == "generate_lead").cast("int")).alias("fez_lead"),
        F.max((e == "purchase").cast("int")).alias("comprou"),
    )
    return por_sessao.groupBy(F.col("user_pseudo_id").alias("visitante")).agg(
        F.count("*").cast("double").alias("sessoes"),
        F.avg("eventos").alias("eventos_medios"),
        F.avg("scroll_max").alias("scroll_medio"),
        F.avg("viu_video").alias("taxa_video"),
        F.max("fez_lead").cast("double").alias("fez_lead"),
        F.max("comprou").cast("double").alias("comprou"),
    )


def segmentar(perfis: DataFrame, ks=(3, 4, 5)) -> DataFrame:
    vetor = VectorAssembler(inputCols=PERFIL, outputCol="bruto").transform(perfis)
    escalado = StandardScaler(inputCol="bruto", outputCol="features").fit(vetor).transform(vetor).cache()
    avaliador = ClusteringEvaluator(featuresCol="features", predictionCol="segmento")
    melhor = None
    for k in ks:
        modelo = KMeans(k=k, seed=SEED, featuresCol="features", predictionCol="segmento").fit(escalado)
        silhueta = avaliador.evaluate(modelo.transform(escalado))
        if melhor is None or silhueta > melhor[1]:
            melhor = (k, silhueta, modelo)
    k, silhueta, modelo = melhor
    segmentado = modelo.transform(escalado).select("visitante", "segmento", *PERFIL)
    perfil = segmentado.groupBy("segmento").agg(
        F.count("*").alias("visitantes_no_segmento"),
        *[F.round(F.avg(c), 3).alias(f"media_{c}") for c in PERFIL])
    return (segmentado.select("visitante", "segmento").join(perfil, "segmento")
            .withColumn("k", F.lit(k)).withColumn("silhueta", F.lit(round(silhueta, 4))))


def main() -> None:
    spark = criar_sessao("gold-ml")
    exigir_camada(spark, "silver/eventos", "python professor/solucao/02_silver_eventos.py")
    eventos = spark.read.parquet(caminho("silver/eventos")).cache()
    atributos = atributos_sessao(eventos, spark.read.parquet(caminho("silver/itens")))

    pontuado, metricas = treinar_propensao(features_sessao(eventos, atributos))
    pontuado.coalesce(1).write.mode("overwrite").parquet(caminho("gold/propensao_sessao"))
    spark.createDataFrame([metricas]).coalesce(1).write.mode("overwrite").parquet(caminho("gold/metricas_modelo"))
    segmentos = segmentar(perfil_visitantes(eventos)).cache()
    segmentos.coalesce(1).write.mode("overwrite").parquet(caminho("gold/segmentos_visitantes"))

    titulo("Propensão de compra")
    for chave, valor in metricas.items():
        print(f"{chave:<10} {valor}")
    titulo("Segmentos de visitantes")
    linha = segmentos.first()
    print(f"k escolhido: {linha.k}   silhueta: {linha.silhueta}")
    segmentos.drop("visitante").distinct().orderBy("segmento").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
