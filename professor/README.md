# Área do professor

Esta pasta **não vai para o aluno**. O `.gitattributes` marca `professor/`, `docs/specs/` e `docs/superpowers/` como `export-ignore`.

## Gerar o pacote do aluno

```bash
git archive --format=zip -o projeto-final-aluno.zip HEAD
```

Distribua o zip, nunca o repositório nem um fork dele. Antes de distribuir, confira:

```bash
# 1. nada da solução dentro do zip (a saída tem que ficar vazia)
unzip -l projeto-final-aluno.zip | grep -E "professor/|docs/specs/|docs/superpowers/"

# 2. o pacote roda sozinho: descompacte numa pasta nova e rode os testes dela
unzip -q projeto-final-aluno.zip -d /tmp/pacote-aluno
docker run --rm -v /tmp/pacote-aluno:/projeto -w /projeto datalake-tracking-app python -m pytest -q
```

O pacote leva o `pipeline/comum.py` pronto, o explorador inteiro (S3, Data Catalog, Jobs ETL, Workflows e Athena) e nenhum job: o aluno escreve todos.

## Rodar a solução de referência

```bash
docker compose up -d floci
docker compose run --rm app python gerador/gerar_eventos.py
docker compose run --rm app python professor/solucao/limpar_camadas.py
docker compose run --rm app python professor/solucao/executar_pipeline.py
docker compose run --rm app python professor/solucao/medir_armadilhas.py
```

O mesmo pipeline como workflow (um grafo: as cinco Golds rodam lado a lado depois da Silver, até 2 ao mesmo tempo):

```bash
docker compose run --rm app python -m explorador.workflows professor/solucao/workflows/medalhao.json
```

Medido com os dados padrão: cerca de 11 minutos, todos os 8 jobs com sucesso.

Testes da solução:

```bash
docker compose run --rm app python -m pytest professor/solucao/tests
```

## Mostrar a solução no explorador

O explorador lê os jobs da pasta indicada em `EXPLORADOR_JOBS` (o padrão é `pipeline`). Para a aula, aponte para a solução:

```bash
EXPLORADOR_JOBS=professor/solucao docker compose up -d --force-recreate explorador
```

Em **Glue · Jobs ETL** aparecem os jobs da solução, e em **Glue · Workflows** o workflow `medalhao`. No PowerShell, defina a variável antes: `$env:EXPLORADOR_JOBS = "professor/solucao"`. Para voltar à pasta do aluno, recrie o contêiner sem a variável.

## O que há aqui

| Arquivo | Para quê |
| --- | --- |
| [`GABARITO.md`](GABARITO.md) | números medidos com os dados padrão, tolerâncias e roteiro de correção |
| `solucao/01_bronze_ingestao.py` … `07_gold_mapa_calor.py` | um job por camada, lendo e gravando no S3 só pelo Spark (`s3a://`) |
| `solucao/08_exportar_mapa_calor.py` | exporta as Golds do mapa de calor para `site/mapa_calor.js` |
| `solucao/comum.py`, `solucao/sessoes.py` | `comum.py` só carrega o `pipeline/comum.py` do aluno (fonte única); `sessoes.py` tem as transformações de sessão compartilhadas |
| `solucao/executar_pipeline.py`, `solucao/limpar_camadas.py` | rodar os 8 jobs em sequência (o R3); zerar Bronze, Silver e Gold sem tocar na landing |
| `solucao/workflows/medalhao.json` | os mesmos 8 jobs como workflow do explorador, com as dependências reais entre eles |
| `solucao/medir_armadilhas.py` | como cada armadilha aparece nos números |
| `solucao/tests/` | testes das transformações, com Spark local e sem S3, e do workflow `medalhao` |

A Bronze é append-only: rodar o `executar_pipeline.py` (ou o workflow) duas vezes sem limpar dobra a Bronze. Silver e Gold não mudam, porque a Silver deduplica por `event_id`. Para os números do gabarito, rode sempre a partir do `limpar_camadas.py`.
