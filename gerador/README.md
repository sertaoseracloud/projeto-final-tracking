# Gerador de eventos — o coletor de tracking simulado

Este diretório faz o papel do **mundo lá fora**: a landing page DadosPro e o coletor de tracking que grava os eventos dela no S3. O pipeline do datalake não conhece este código. A única coisa que os dois compartilham é o **formato do evento** (`docs/CONTRATO_EVENTO.md`) e o prefixo `landing/eventos/` do bucket.

Não usa Spark: é Python puro mais `boto3`.

## Uso

```powershell
docker compose up -d floci                                      # S3 local
docker compose run --rm app python gerador/gerar_eventos.py     # 3000 visitantes, 14 dias, seed 42
```

| Opção | Padrão | O que faz |
| --- | --- | --- |
| `--dias` | 14 | dias simulados a partir de `--data-inicial` |
| `--visitantes` | 3000 | visitantes únicos (cada um faz de 1 a 4 sessões) |
| `--seed` | 42 | mesma seed, mesmos arquivos, byte a byte |
| `--sujeira` | 0.03 | fração de eventos com defeito (0 a 0.5) |
| `--data-inicial` | 2026-09-01 | primeiro dia, em UTC |
| `--saida-local PASTA` | — | grava numa pasta, com a mesma árvore de chaves, em vez do S3 |

Rodar de novo com a **mesma** seed sobrescreve os mesmos arquivos. Uma seed **diferente** acrescenta arquivos novos (`coletor-s<seed>-*.jsonl`), como um segundo coletor gravando no mesmo bucket.

### Importar os eventos da página de exemplo

Navegue por `site/index.html`, clique em **Exportar JSONL** no painel lateral e copie o arquivo exportado para dentro da pasta do repositório (por exemplo, `saida/`), porque o comando roda no contêiner e só enxerga o repositório:

```powershell
docker compose run --rm app python gerador/importar_site.py saida/eventos_2026-10-10.jsonl
```

O arquivo vai para `landing/eventos/dt=<data do primeiro evento>/site-<instante>.jsonl`. Arquivos com BOM e quebras de linha CRLF são aceitos.

## O que é gravado

`s3://datalake-tracking/landing/eventos/dt=AAAA-MM-DD/coletor-s<seed>-<lote>.jsonl` — um evento JSON por linha, de 3 a 6 lotes por dia; o dia seguinte ao último (ex.: `dt=2026-09-15`) pode ter 1 ou 2 lotes, com sessões que viram a meia-noite.

## Como a jornada é simulada

Cada visitante recebe um canal de origem e um dispositivo, que mudam as chances de rolar a página, ver o vídeo, escolher um plano e comprar. Na ordem da página: hero → vídeo → módulos → tabela de planos → checkout → obrigado → área de membros.

- **Quem vira lead** volta outro dia, em geral pela newsletter, com mais intenção de compra.
- **Quem compra** pode criar conta (`sign_up`) e, em visitas seguintes, faz `login`.
- **Reembolsos:** cerca de 8% das compras recebem `refund` de 1 a 7 dias depois (compras da última semana podem ficar sem reembolso, porque ele cairia fora da janela).
- **Frete:** só o plano Premium tem `add_shipping_info`, porque inclui apostila impressa.
- **`session_hint`:** muda em cerca de 25% das trocas de página, como uma recarga do navegador. Por isso não serve para contar sessões.

## Defeitos injetados

| Tipo | O que acontece |
| --- | --- |
| `purchase_sem_id` | `purchase` sem `transaction_id` (no máximo 5% das compras) |
| `valor_negativo` | `value` negativo |
| `moeda_usd` | `currency = "USD"` |
| `scroll_33` | `percent_scrolled = 33`, fora dos marcos |
| `ts_nulo` | `event_ts` nulo |
| `ts_futuro` | `event_ts` dez anos à frente |
| `refund_orfao` | `refund` de uma transação que não existe (no máximo 5% das compras) |
| `evento_fora_catalogo` | `event_name = "add_to_wishlist"` |
| reenvio de lote | cerca de 1% dos eventos aparece de novo, com o mesmo `event_id`, no lote vizinho |

Os tipos que alteram um evento existente atingem no máximo 5% dos candidatos; a cota que sobra vai para `ts_futuro`.

Um `purchase` que perde o `transaction_id` ou o valor deixa o `refund` dele sem compra válida. Esse efeito em cascata é proposital.

## Saída com os parâmetros padrão

```text
Destino:            s3://datalake-tracking (http://floci:4566)
Período:            2026-09-01 a 2026-09-14   seed: 42
visitantes              3000
sessões simuladas       4472
eventos limpos         31107
duplicatas (lote)        299
linhas gravadas        31553   em 63 arquivos

Eventos por nome (antes da sujeira):
  page_view               5557
  scroll                 10446
  click                   1449
  site_search              196
  video_start             1473
  video_progress          3713
  video_complete           436
  file_download            191
  view_item_list          2750
  select_item              886
  view_item                547
  add_to_cart              930
  remove_from_cart         129
  view_cart                666
  begin_checkout           477
  add_shipping_info         86
  add_payment_info         339
  purchase                 267
  refund                    21
  generate_lead            274
  sign_up                  190
  login                     84

Defeitos injetados:
  purchase_sem_id           14
  valor_negativo           134
  moeda_usd                134
  scroll_33                133
  ts_nulo                  133
  ts_futuro                238
  refund_orfao              14
  evento_fora_catalogo     133
```

## Testes

```powershell
docker compose run --rm app python -m pytest tests/gerador
```
