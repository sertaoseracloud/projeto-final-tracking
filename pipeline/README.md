# pipeline/ — o seu datalake

Seu código vai aqui: um job por camada, como pede o [enunciado](../docs/ENUNCIADO.md) (R1 a R10).

## Como rodar um job

Todo código Python roda no contêiner `app`, que já tem PySpark 3.5.3 e o conector `s3a`:

```bash
docker compose run --rm app python pipeline/<seu_job>.py
```

## Falando com o S3

O pipeline acessa o bucket **só pelo Spark**, com caminhos `s3a://datalake-tracking/...`. Nada de `boto3` aqui. A configuração da `SparkSession` que funciona com o floci está pronta em [`infra/verificar_ambiente.py`](../infra/verificar_ambiente.py): copie a função `criar_sessao()` para o seu código.

Para saber se uma camada existe antes de ler, use o `FileSystem` do Hadoop pela JVM do Spark:

```python
caminho = spark._jvm.org.apache.hadoop.fs.Path("s3a://datalake-tracking/silver/eventos")
existe = caminho.getFileSystem(spark._jsc.hadoopConfiguration()).exists(caminho)
```

## Partições por dia

A landing, a Bronze e a Silver usam pastas `year=AAAA/month=MM/day=DD`. Ao ler uma pasta assim, o Spark descobre `year`, `month` e `day` como colunas e, por padrão, converte os valores em número: `month=09` vira `9`, e ao regravar a pasta sai `month=9`. Para manter os dois dígitos, desligue essa inferência na `SparkSession`:

```python
.config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
```

Na gravação, use `.partitionBy("year", "month", "day")` com as três colunas como texto.

## Antes de começar

1. `docker compose up -d floci`
2. `docker compose run --rm app python gerador/gerar_eventos.py`
3. `docker compose run --rm app python infra/verificar_ambiente.py`, que precisa mostrar três linhas `[ok]`
