# pipeline/ — o seu datalake

Seu código vai aqui: um job por camada, como pede o [enunciado](../docs/ENUNCIADO.md) (R1 a R10).

Um **job** é um script Python que faz uma etapa do trabalho do começo ao fim: lê uma camada, transforma os dados e grava a camada seguinte. Por exemplo, o job Bronze lê a `landing/` e grava a `bronze/`.

## Como rodar um job

Todo código Python roda no contêiner `app`, que já tem PySpark 3.5.3 e o conector `s3a`:

```bash
docker compose run --rm app python pipeline/<seu_job>.py
```

Troque `<seu_job>` pelo nome do seu arquivo. Se você nunca usou Docker, o [SETUP.md](../SETUP.md) explica o que cada parte desse comando faz.

## Falando com o S3

O pipeline acessa o bucket **só pelo Spark**, com caminhos `s3a://datalake-tracking/...`. Nada de `boto3` aqui.

- `s3a://` é o prefixo que diz ao Spark "este caminho está num S3". É o conector `s3a` (biblioteca `hadoop-aws`) que faz a conversa com o floci.
- `boto3` é o SDK da AWS para Python. O gerador usa `boto3`, mas o seu pipeline não pode usar (requisito R1).

A configuração da `SparkSession` que funciona com o floci está pronta em [`infra/verificar_ambiente.py`](../infra/verificar_ambiente.py): copie a função `criar_sessao()` para o seu código. A `SparkSession` é o objeto de entrada do Spark: todo `spark.read` e todo `df.write` passam por ela.

Para saber se uma camada existe antes de ler, use o `FileSystem` do Hadoop pela JVM do Spark:

```python
caminho = spark._jvm.org.apache.hadoop.fs.Path("s3a://datalake-tracking/silver/eventos")
existe = caminho.getFileSystem(spark._jsc.hadoopConfiguration()).exists(caminho)
```

Por que isso importa: o requisito R2 pede que cada job confira se a camada anterior existe. O trecho acima devolve `True` ou `False`; se for `False`, o job deve parar com uma mensagem dizendo qual job rodar antes.

## Partições por dia

Uma **partição** é uma subpasta que agrupa os dados por um valor. Aqui o valor é o dia: todos os eventos de 3 de setembro de 2026 ficam em `year=2026/month=09/day=03/`. Assim, quem quer ler só um dia não precisa abrir os outros.

A landing, a Bronze e a Silver usam pastas `year=AAAA/month=MM/day=DD`. Ao ler uma pasta assim, o Spark descobre `year`, `month` e `day` como colunas e, por padrão, converte os valores em número: `month=09` vira `9`, e ao regravar a pasta sai `month=9`. Para manter os dois dígitos, desligue essa inferência na `SparkSession`:

```python
.config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
```

Na gravação, use `.partitionBy("year", "month", "day")` com as três colunas como texto. O `partitionBy` diz ao Spark para criar uma subpasta por combinação de valores dessas colunas.

## Antes de começar

Rode estes três comandos, nesta ordem:

1. `docker compose up -d floci` (liga o S3 local)
2. `docker compose run --rm app python gerador/gerar_eventos.py` (grava os eventos na landing)
3. `docker compose run --rm app python infra/verificar_ambiente.py`, que precisa mostrar três linhas `[ok]`
