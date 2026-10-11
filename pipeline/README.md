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
caminho = spark._jvm.org.apache.hadoop.fs.Path("s3a://datalake-tracking/silver/event_name=*")
existe = bool(caminho.getFileSystem(spark._jsc.hadoopConfiguration()).globStatus(caminho))
```

Por que isso importa: o requisito R2 pede que cada job confira se a camada anterior existe. O trecho acima devolve `True` ou `False`; se for `False`, o job deve parar com uma mensagem dizendo qual job rodar antes. O `*` é um **glob**, um "curinga": `event_name=*` casa com qualquer pasta que comece com `event_name=`. Por isso o trecho usa `globStatus` (que entende curingas) em vez de `exists`.

## Partições por nome do evento e por dia

Uma **partição** é uma subpasta que agrupa os dados por um valor. Aqui há dois níveis: primeiro o nome do evento, depois o dia. Todas as rolagens de 3 de setembro de 2026 ficam em `event_name=scroll/year=2026/month=09/day=03/`. Assim, quem quer ler só as compras, ou só um dia, não precisa abrir o resto.

A landing, a Bronze e a Silver guardam os eventos assim, **direto na raiz da camada** (não existe pasta `eventos`):

```text
landing/event_name=scroll/year=2026/month=09/day=03/coletor-s42-001.jsonl
bronze/event_name=scroll/year=2026/month=09/day=03/part-….parquet
silver/event_name=scroll/year=2026/month=09/day=03/part-….parquet
silver/itens/year=2026/month=09/day=03/part-….parquet      ← vizinha das pastas event_name=
silver/eventos_rejeitados/part-….parquet                   ← vizinha também
```

### Os dois dígitos do mês

Ao ler pastas `k=v`, o Spark descobre `event_name`, `year`, `month` e `day` como colunas e, por padrão, converte os valores que parecem número: `month=09` vira `9`, e ao regravar sai `month=9`. Para manter tudo como texto, desligue essa inferência na `SparkSession`:

```python
.config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
```

### Ler só os eventos

Se você ler a raiz `s3a://datalake-tracking/silver/` direto, o Spark encontra `event_name=…` ao lado de `itens/` e `eventos_rejeitados/` e para com o erro *Conflicting directory structures*. Leia só as pastas `event_name=` com um glob e diga ao Spark onde fica a raiz das partições (`basePath`), para ele devolver `event_name` como coluna:

```python
raiz = "s3a://datalake-tracking/silver"
eventos = spark.read.option("basePath", raiz).parquet(f"{raiz}/event_name=*")
```

A landing é igual, com `.json(...)` no lugar de `.parquet(...)`. O `event_name` que vem do caminho é o mesmo que está dentro de cada evento.

### Gravar sem apagar as vizinhas

Na gravação, use `.partitionBy("event_name", "year", "month", "day")` com as quatro colunas como texto. O `partitionBy` cria uma subpasta por combinação de valores dessas colunas.

Cuidado com o `mode("overwrite")` na Silver: por padrão ele **apaga a pasta inteira** antes de gravar, e `silver/` inteira inclui `itens/` e `eventos_rejeitados/`. Use o modo **dinâmico**, que troca só as partições que estão no DataFrame:

```python
(silver.write.mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("event_name", "year", "month", "day")
    .parquet("s3a://datalake-tracking/silver"))
```

Na Bronze, que é append-only (R4), use `mode("append")` com o mesmo `partitionBy`.

## Antes de começar

Rode estes três comandos, nesta ordem:

1. `docker compose up -d floci` (liga o S3 local)
2. `docker compose run --rm app python gerador/gerar_eventos.py` (grava os eventos na landing)
3. `docker compose run --rm app python infra/verificar_ambiente.py`, que precisa mostrar três linhas `[ok]`

## Exportar um resultado pequeno para o site

O requisito R11 pede um arquivo `site/mapa_calor.js` com os números do mapa de calor. Esse arquivo não fica no S3: o site lê do disco. Então o job lê a Gold pelo Spark e, no fim, grava um arquivo comum com Python.

- **Trazer o resultado para o Python:** `collect()` traz as linhas de um DataFrame do Spark para uma lista do Python. Use só com tabelas pequenas, como as duas do mapa de calor, que têm poucas dezenas de linhas. **Nunca** faça isso com a Bronze ou a Silver: são dezenas de milhares de linhas e o job fica lento ou estoura a memória.
- **Onde gravar:** o contêiner enxerga o repositório em `/projeto`, então `site/` é `/projeto/site/`. A partir de um arquivo em `pipeline/`, o caminho é `Path(__file__).resolve().parents[1] / "site" / "mapa_calor.js"`.

```python
import json
from datetime import datetime, timezone
from pathlib import Path

cliques = [linha.asDict() for linha in spark.read.parquet("s3a://datalake-tracking/gold/mapa_calor_cliques").collect()]
rolagem = [linha.asDict() for linha in spark.read.parquet("s3a://datalake-tracking/gold/mapa_calor_rolagem").collect()]
agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
dados = {"gerado_em": agora, "cliques": cliques, "rolagem": rolagem}

destino = Path(__file__).resolve().parents[1] / "site" / "mapa_calor.js"
texto = json.dumps(dados, ensure_ascii=False, default=float)
destino.write_text("window.MAPA_CALOR = " + texto + ";\n", encoding="utf-8")
```

- `default=float` cuida das colunas do tipo `Decimal` (por exemplo, quando você usa `cast("decimal(…)")`): o `json.dumps` não sabe escrever `Decimal` e daria `TypeError`. Prefira gravar as taxas da Gold como `double`.
- Se o mapa sair sem cliques, a sua landing provavelmente foi gerada antes do campo `element_id` existir. Rode de novo `python gerador/gerar_eventos.py` e o pipeline inteiro.

Depois, abra `site/index.html` no navegador e clique em **Mapa de calor**, no painel lateral. Se você rodar o job de novo, recarregue a página para ver os números novos.
