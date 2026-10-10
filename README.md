# DadosPro — projeto final de engenharia de dados

Uma empresa vende um curso online por uma landing page de conversão, e cada interação do visitante vira um evento de tracking gravado num bucket S3. O projeto é construir o **datalake em arquitetura medalhão** que transforma esses eventos em respostas de negócio: funil de compra, engajamento, receita líquida e previsão de compra.

## O que há aqui

| Pasta | O que é |
| --- | --- |
| [`site/`](site/index.html) | a landing page de exemplo. Abra no navegador, navegue e veja cada evento nascer no painel lateral |
| [`gerador/`](gerador/README.md) | o coletor simulado: popula o bucket com 14 dias de eventos, inclusive com defeitos |
| `pipeline/` | onde o datalake é construído |
| `docs/` | enunciado e contrato do evento |

## Começar

1. Prepare o ambiente: [SETUP.md](SETUP.md).
2. Popule a landing: `docker compose run --rm app python gerador/gerar_eventos.py`.
3. Leia o enunciado em `docs/ENUNCIADO.md` e construa o pipeline em `pipeline/`.
