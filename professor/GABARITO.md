# Gabarito do projeto final

Todos os números abaixo foram medidos rodando a solução de referência sobre os dados padrão do gerador (seed 42). Eles não são estimativas.

## 1. Como reproduzir

A landing precisa estar com os dados padrão. Para regenerá-la do zero, apague a landing e rode o gerador:

```bash
docker compose up -d floci
docker compose run --rm app python -c "import sys; sys.path.insert(0,'professor/solucao'); from comum import criar_sessao, apagar_camada; s=criar_sessao('x'); print(apagar_camada(s,'landing'))"
docker compose run --rm app python gerador/gerar_eventos.py
```

Depois, zere Bronze, Silver e Gold e rode o pipeline inteiro:

```bash
docker compose run --rm app python professor/solucao/limpar_camadas.py
docker compose run --rm app python professor/solucao/executar_pipeline.py
```

Para medir as armadilhas:

```bash
docker compose run --rm app python professor/solucao/medir_armadilhas.py
```

## 2. Dados de entrada

Parâmetros: seed 42, 6000 visitantes, 14 dias (2026-09-01 a 2026-09-14), 3% de sujeira.

| Item | Valor |
| --- | --- |
| visitantes | 6000 |
| sessões simuladas | 8805 |
| eventos limpos | 63414 |
| duplicatas (lote) | 656 |
| linhas gravadas | 64372, em 66 arquivos |

Defeitos injetados: `purchase_sem_id` 31, `valor_negativo` 272, `moeda_usd` 77, `scroll_33` 272, `ts_nulo` 272, `ts_futuro` 676, `refund_orfao` 31, `evento_fora_catalogo` 271.

## 3. Silver

| Item | Valor |
| --- | --- |
| linhas na Bronze | 64372 |
| duplicatas removidas | 656 |
| aprovados | 61813 |
| sessões | 8793 |

Rejeitados por motivo (total 1903):

| Motivo | Linhas |
| --- | --- |
| timestamp ausente ou futuro | 948 |
| valor monetario invalido | 349 |
| marco fora do padrao | 272 |
| evento fora do catalogo | 271 |
| reembolso sem compra | 32 |
| transacao sem id | 31 |

As sessões são calculadas somente com os eventos aprovados pela Silver: um evento rejeitado não estende a sessão. Por isso são 8793 sessões, contra as 8805 simuladas pelo gerador.

## 4. Gold: funil

Funil de compra, total:

| Etapa | Sessões | Conv. sobre a anterior | Conv. sobre o topo | `taxa_view_item` |
| --- | --- | --- | --- | --- |
| view_item_list | 5258 | — | 1,0 | 0,573 |
| select_item | 1719 | 0,3269 | 0,3269 | 0,573 |
| add_to_cart | 1291 | 0,751 | 0,2455 | 0,573 |
| view_cart | 1255 | 0,9721 | 0,2387 | 0,573 |
| begin_checkout | 912 | 0,7267 | 0,1734 | 0,573 |
| add_payment_info | 684 | 0,75 | 0,1301 | 0,573 |
| purchase | 573 | 0,8377 | 0,109 | 0,573 |

A maior perda está na passagem de `view_item_list` para `select_item`: só 0,3269 das sessões que viram a lista escolhem um plano. Dentro do checkout, a maior perda é `view_cart` para `begin_checkout` (0,7267).

Funil de lead, total:

| Etapa | Sessões | Conv. sobre a anterior | Conv. sobre o topo |
| --- | --- | --- | --- |
| visita_landing | 8382 | — | 1,0 |
| scroll_50 | 5892 | 0,7029 | 0,7029 |
| generate_lead | 522 | 0,0886 | 0,0623 |

Por canal, o `generate_lead` sobre o topo é: direct / (none) 0,0565; google / cpc 0,068; google / organic 0,0685; instagram / paid_social 0,0642; newsletter / email 0,0263.

## 5. Gold: engajamento

Retenção do vídeo (2890 sessões iniciaram o vídeo):

| Marco | Sessões | Percentual |
| --- | --- | --- |
| 10 | 2626 | 0,9087 |
| 25 | 2138 | 0,7398 |
| 50 | 1474 | 0,51 |
| 75 | 1105 | 0,3824 |
| 100 | 877 | 0,3035 |

Termos do FAQ sem resultado:

| Termo | Buscas | Sem resultado |
| --- | --- | --- |
| estagio | 44 | 44 |
| presencial | 33 | 33 |
| bolsa | 32 | 32 |
| power bi | 28 | 28 |
| linux | 24 | 24 |

Os termos acesso, parcelar, certificado, reembolso e apostila têm resultado (0 buscas sem resultado).

Saídas e downloads:

| Tipo | Alvo | Quantidade |
| --- | --- | --- |
| download | ementa-formacao-dados.pdf | 335 |
| saída | instagram.com | 102 |
| saída | linkedin.com | 92 |
| saída | wa.me | 87 |
| saída | youtube.com | 85 |

Total de downloads: 335. Total de saídas: 366 (102 + 92 + 87 + 85).

## 6. Gold: receita e aquisição

Receita total (R$):

| Bruta | Reembolsada | Líquida | Pedidos | Ticket médio |
| --- | --- | --- | --- | --- |
| 338523,10 | 28608,50 | 309914,60 | 573 | 590,79 |

O ticket médio é a receita bruta dividida pelos pedidos, lido da tabela `gold/receita_diaria`.

Taxa de reembolso: total 0,0838 (48 reembolsos em 573 pedidos). Por canal, somando os planos da tabela `taxa_reembolso`:

| Canal | Pedidos | Reembolsados | Taxa |
| --- | --- | --- | --- |
| direct / (none) | 112 | 2 | 0,0179 |
| google / cpc | 221 | 20 | 0,0905 |
| google / organic | 101 | 9 | 0,0891 |
| instagram / paid_social | 65 | 15 | 0,2308 |
| newsletter / email | 74 | 2 | 0,027 |

O canal instagram / paid_social se destaca (0,2308). A combinação PLANO-PREMIUM com instagram / paid_social chega a 0,4 (6 de 15 pedidos).

Adesão ao order bump:

| Plano | Pedidos | Com bump | Adesão |
| --- | --- | --- | --- |
| (todos) | 573 | 90 | 0,1571 |
| PLANO-BASICO | 193 | 29 | 0,1503 |
| PLANO-PREMIUM | 120 | 18 | 0,15 |
| PLANO-PRO | 260 | 43 | 0,1654 |

Lead para compra:

| Canal | Leads | Compraram depois | Mediana de dias | Taxa de conversão |
| --- | --- | --- | --- | --- |
| (todos) | 522 | 40 | 4,64 | 0,0766 |
| direct / (none) | 94 | 8 | 7,48 | 0,0851 |
| google / cpc | 165 | 16 | 3,54 | 0,097 |
| google / organic | 118 | 8 | 1,89 | 0,0678 |
| instagram / paid_social | 131 | 7 | 6,05 | 0,0534 |
| newsletter / email | 14 | 1 | 10,09 | 0,0714 |

As medianas usam `percentile_approx`, portanto são aproximadas. Uma mediana exata pode diferir um pouco.

## 7. Gold: ML

Modelo de propensão de compra, com pesos de classe porque os compradores são cerca de 6,5% das sessões. Conjuntos: 7071 sessões de treino e 1722 de teste. Features: scroll_max, video_max, viu_planos, selecionou_plano, abriu_detalhe, buscas, downloads, saidas, page_views, canal, dispositivo, plano.

| Modelo | AUC | F1 | Precisão | Recall |
| --- | --- | --- | --- | --- |
| Honesto (só landing, antes do carrinho) | 0,9243 | 0,5065 | 0,3421 | 0,975 |
| Com vazamento (eventos do checkout) | 0,9886 | 0,7852 | 0,6573 | 0,975 |

O vazamento aparece mais claramente no F1 e na precisão (o F1 sobe de 0,5065 para 0,7852 e a precisão de 0,3421 para 0,6573). No AUC o salto também existe, de 0,9243 para 0,9886.

Segmentação de visitantes: k escolhido 4, silhueta 0,4767.

| Segmento | Visitantes | Média de sessões | Média de eventos médios | Média de scroll médio | Média da taxa de vídeo | Média fez lead | Média comprou |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 3074 | 1,39 | 8,349 | 70,542 | 0,501 | 0,0 | 0,0 |
| 1 | 535 | 1,914 | 12,438 | 52,393 | 0,304 | 0,004 | 1,0 |
| 2 | 520 | 1,796 | 8,059 | 68,803 | 0,348 | 1,0 | 0,073 |
| 3 | 1865 | 1,374 | 3,146 | 25,296 | 0,069 | 0,0 | 0,0 |

Leitura: o segmento 0 engaja sem converter, o 1 reúne os compradores, o 2 os leads e o 3 os visitantes de baixo engajamento.

## 8. Tolerâncias para corrigir o trabalho do aluno

| Critério | Tolerância |
| --- | --- |
| Contagens da Silver (duplicatas, aprovados, rejeitados por motivo) | ±1% (desempate diferente na deduplicação, regras extras justificadas) |
| Sessões | ±2% (bordas do gap de 30 min) |
| Funil e receita | ±2% |
| AUC honesto | dentro de ±0,03 de 0,9243. Um AUC ≥ 0,98 indica vazamento |
| Medianas (lead → compra, em dias) | ±0,5 dia (`percentile_approx` contra mediana exata) |
| Silhueta e k | livres (dependem das features de perfil escolhidas). Exigir só que k ∈ {3, 4, 5} e que a escolha seja justificada |

## 9. Como reconhecer cada armadilha no trabalho do aluno

1. **Frete no funil.** Com `add_shipping_info` como etapa obrigatória, a conversão em sequência por plano (entre as sessões com `begin_checkout`) cai de 0,5858 para 0,0356 no Básico e de 0,6133 para 0,0172 no Pro. O Premium cai só de 0,5682 para 0,4886. O frete praticamente só existe no Premium (136 sessões com `add_shipping_info`, contra 15 no Básico e 20 no Pro). Os poucos eventos de frete em Básico e Pro vêm de visitantes que trocaram de plano depois do `add_to_cart` (o gerador troca o plano em 10% dos carrinhos), porque o plano da sessão é o primeiro `select_item`. **Sintoma do aluno que seguiu o 5.4 (contagem sem exigir a sequência) e mesmo assim pôs o frete como etapa obrigatória:** no Básico a linha de `add_shipping_info` tem 15 sessões, cerca de 0,049 de `begin_checkout` (15 ÷ 309), e a etapa seguinte, `add_payment_info`, mostra conversão de cerca de 15,3 (230 ÷ 15), acima de 100%. No Pro são 20 sessões, cerca de 0,049 (20 ÷ 406), e a etapa seguinte mostra cerca de 16,1 (322 ÷ 20).
2. **Reembolso na data errada.** Atribuindo o `refund` à data do próprio evento, 7 de 189 células (data × plano × canal) ficam com receita líquida negativa, por exemplo -1144,0 em 2026-09-09 (PLANO-PREMIUM, google / organic) e -947,15 em 2026-09-04 (PLANO-PREMIUM, instagram / paid_social).
3. **Vazamento no modelo.** AUC 0,9886, F1 0,7852 e precisão 0,6573 com eventos do checkout, contra AUC 0,9243, F1 0,5065 e precisão 0,3421 só com a landing.
4. **Duplicata de lote.** Sem deduplicar por `event_id`, a Bronze tem 64372 linhas para 63716 `event_id` distintos, e há 609 `purchase` contra 604 distintos. A receita bruta de `purchase` na Bronze fica em 359250,85, contra 356240,4 deduplicada por `event_id`: diferença de 3010,45.
5. **`session_hint` do cliente.** Contando por `session_hint` há 11440 sessões distintas, contra 8793 com o gap de 30 minutos.

## 10. Roteiro de correção

| Critério | Pontos | O que olhar |
| --- | --- | --- |
| Arquitetura (R1–R4) | 15 | Um job por camada, verificação de camada antes de ler, Bronze append-only com proveniência (arquivo de origem). Rodar o script de sequência do zero e conferir que termina sem erro. |
| Silver (R5–R10) | 25 | Linhas na Bronze 64372, duplicatas 656, aprovados 61813 e rejeitados por motivo (seção 3), dentro de ±1%. Sessões perto de 8793 (±2%) e calculadas sem `session_hint`. Stitching de visitante presente. |
| Gold (seção 5.3 do enunciado) | 30 | As 13 tabelas existem. Funil, receita e lead batem com as seções 4 a 6 (±2%). Modelo sem vazamento: AUC honesto em 0,9243 ±0,03 e nunca ≥ 0,98. |
| Relatório | 20 | Cada resposta cita tabela e número da Gold. A recomendação é coerente com os dados (por exemplo, a maior perda do funil em `select_item`, o reembolso alto em instagram / paid_social, os termos de FAQ sem resultado). |
| Qualidade do dado | 10 | Identifica ao menos três das cinco armadilhas da seção 9 com o sintoma em número, além de duplicatas e rejeições. |

O formato (individual ou em dupla) e o prazo ficam a critério do professor.
