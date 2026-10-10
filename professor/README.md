# Área do professor

Esta pasta **não vai para o aluno**. O `.gitattributes` marca `professor/`, `docs/specs/` e `docs/superpowers/` como `export-ignore`.

## Gerar o pacote do aluno

```bash
git archive --format=zip -o projeto-final-aluno.zip HEAD
```

Distribua o zip, nunca o repositório nem um fork dele.

## Rodar a solução de referência

```bash
docker compose up -d floci
docker compose run --rm app python gerador/gerar_eventos.py
docker compose run --rm app python professor/solucao/limpar_camadas.py
docker compose run --rm app python professor/solucao/executar_pipeline.py
docker compose run --rm app python professor/solucao/medir_armadilhas.py
```

Testes da solução:

```bash
docker compose run --rm app python -m pytest professor/solucao/tests
```

## O que há aqui

| Arquivo | Para quê |
| --- | --- |
| [`GABARITO.md`](GABARITO.md) | números medidos com os dados padrão, tolerâncias e roteiro de correção |
| `solucao/01_bronze_ingestao.py` … `06_gold_ml.py` | um job por camada, lendo e gravando no S3 só pelo Spark (`s3a://`) |
| `solucao/comum.py`, `solucao/sessoes.py` | sessão Spark com `s3a`, camadas, transformações de sessão compartilhadas |
| `solucao/executar_pipeline.py`, `solucao/limpar_camadas.py` | rodar tudo em sequência; zerar Bronze, Silver e Gold sem tocar na landing |
| `solucao/medir_armadilhas.py` | como cada armadilha aparece nos números |
| `solucao/tests/` | testes das transformações, com Spark local e sem S3 |

A Bronze é append-only: rodar o `executar_pipeline.py` duas vezes sem limpar dobra a Bronze. Silver e Gold não mudam, porque a Silver deduplica por `event_id`. Para os números do gabarito, rode sempre a partir do `limpar_camadas.py`.
