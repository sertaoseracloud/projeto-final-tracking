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

## Antes de começar

1. `docker compose up -d floci`
2. `docker compose run --rm app python gerador/gerar_eventos.py`
3. `docker compose run --rm app python infra/verificar_ambiente.py`, que precisa mostrar três linhas `[ok]`
