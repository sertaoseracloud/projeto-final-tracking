# Gabarito do projeto final

Todos os números abaixo foram medidos rodando a solução de referência sobre os dados padrão do gerador (seed 42). Eles não são estimativas.

## 1. Como reproduzir

A landing precisa estar com os dados padrão. Para regenerá-la do zero, apague a landing e rode o gerador:

```bash
docker compose up -d floci
docker compose run --rm app python -c "import sys; sys.path.insert(0,'pipeline'); from comum import criar_sessao, apagar_camada; s=criar_sessao('x'); print(apagar_camada(s,'landing'))"
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

Para rodar os jobs da solução pelo explorador (telas **Glue · Jobs ETL** e **Glue · Workflows**), copie a solução para `pipeline/` e clique em **⟳ Sincronizar**. O passo a passo está no [README do professor](README.md#mostrar-a-solução-no-explorador):

```bash
docker compose run --rm app python professor/usar_solucao.py              # antes da aula
docker compose run --rm app python professor/usar_solucao.py --desfazer   # depois, antes de gerar o pacote
```

## 2. Dados de entrada

Parâmetros: seed 42, 6000 visitantes, 14 dias (2026-09-01 a 2026-09-14), 3% de sujeira.

| Item | Valor |
| --- | --- |
| visitantes | 6000 |
| sessões simuladas | 8805 |
| eventos limpos | 63414 |
| duplicatas (lote) | 656 |
| linhas gravadas | 64372, em 1381 arquivos (um por lote e por event_name) |

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

Segmentação de visitantes: k escolhido 5, silhueta 0,5122. (O job ordena os visitantes antes do KMeans; sem isso, o resultado muda com a ordem em que as linhas chegam da Silver.)

| Segmento | Visitantes | Média de sessões | Média de eventos médios | Média de scroll médio | Média da taxa de vídeo | Média fez lead | Média comprou |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 1337 | 1,403 | 2,579 | 16,56 | 0,043 | 0,0 | 0,0 |
| 1 | 1686 | 1,219 | 6,562 | 72,667 | 0,0 | 0,0 | 0,0 |
| 2 | 1916 | 1,515 | 8,884 | 62,3 | 0,841 | 0,0 | 0,0 |
| 3 | 482 | 1,722 | 7,834 | 69,183 | 0,347 | 1,0 | 0,0 |
| 4 | 573 | 1,969 | 12,337 | 53,162 | 0,308 | 0,07 | 1,0 |

Leitura: o segmento 0 são os visitantes de baixo engajamento, o 1 lê a página sem ver o vídeo, o 2 vê o vídeo sem converter, o 3 reúne os leads e o 4 os compradores.

## 8. Gold: mapa de calor

O job 07 grava as duas tabelas e o job 08 exporta `site/mapa_calor.js` (24 elementos, 5 marcos). Com o arquivo gerado, o botão **Mapa de calor** do site acende.

`mapa_calor_cliques` (página, elemento, cliques, sessões que clicaram, sessões com `page_view` na página, taxa de clique, intensidade):

| Página | Elemento | Cliques | Sessões | Sessões na página | Taxa de clique | Intensidade |
| --- | --- | --- | --- | --- | --- | --- |
| / | btn-play | 2890 | 2890 | 8382 | 0,3448 | 1,0 |
| / | cta-hero-ver-planos | 2543 | 2543 | 8382 | 0,3034 | 0,8799 |
| / | plano-pro | 1086 | 836 | 8382 | 0,0997 | 0,3758 |
| / | plano-basico | 879 | 647 | 8382 | 0,0772 | 0,3042 |
| / | modal-comprar | 749 | 749 | 8382 | 0,0894 | 0,2592 |
| / | plano-premium | 530 | 383 | 8382 | 0,0457 | 0,1834 |
| / | form-lead | 522 | 522 | 8382 | 0,0623 | 0,1806 |
| / | dl-ementa | 335 | 335 | 8382 | 0,04 | 0,1159 |
| / | faq-busca | 303 | 303 | 8382 | 0,0361 | 0,1048 |
| / | social-instagram | 102 | 102 | 8382 | 0,0122 | 0,0353 |
| / | social-linkedin | 92 | 92 | 8382 | 0,011 | 0,0318 |
| / | social-whatsapp | 87 | 87 | 8382 | 0,0104 | 0,0301 |
| / | social-youtube | 85 | 85 | 8382 | 0,0101 | 0,0294 |
| /checkout | btn-continuar | 912 | 912 | 1291 | 0,7064 | 1,0 |
| /checkout | btn-comprar | 573 | 573 | 1291 | 0,4438 | 0,6283 |
| /checkout | bump | 412 | 314 | 1291 | 0,2432 | 0,4518 |
| /checkout | pagto-pix | 275 | 275 | 1291 | 0,213 | 0,3015 |
| /checkout | pagto-cartao-parcelado | 269 | 269 | 1291 | 0,2084 | 0,295 |
| /checkout | pagto-cartao-avista | 140 | 140 | 1291 | 0,1084 | 0,1535 |
| /checkout | frete-pac | 105 | 105 | 1291 | 0,0813 | 0,1151 |
| /checkout | frete-sedex | 73 | 73 | 1291 | 0,0565 | 0,08 |
| /membros | btn-signup | 445 | 445 | 626 | 0,7109 | 1,0 |
| /membros | btn-login | 181 | 181 | 626 | 0,2891 | 0,4067 |
| /obrigado | btn-refund | 48 | 48 | 644 | 0,0745 | 1,0 |

`mapa_calor_rolagem` (landing):

| Marco | Sessões que chegaram | % das sessões |
| --- | --- | --- |
| 0 | 8382 | 1,0 |
| 25 | 7362 | 0,8783 |
| 50 | 5822 | 0,6946 |
| 75 | 4219 | 0,5033 |
| 90 | 2177 | 0,2597 |

Leitura: na landing, o ponto mais quente é o play do vídeo, seguido de perto pelo CTA "Ver planos"; os cartões de plano recebem bem menos cliques, coerente com a perda do funil em `select_item`. A rolagem perde gente de forma contínua e cai mais forte entre 75% e 90% (de 50% para 26% das sessões): o FAQ e o rodapé são vistos por cerca de um quarto das visitas. As somas batem com outras Golds: `form-lead` 522 (leads) e `btn-comprar` 573 (pedidos). `plano-*` soma 2495 = 1719 `select_item` (o topo do funil) + 776 eventos de carrinho feitos pelo botão do cartão; os outros 749 vieram do botão do detalhe (`modal-comprar`, `add_to_cart` logo depois de `view_item`).

`modal-comprar` está na landing, mas o modal fica fechado na abertura da página: no site, a mancha dele só aparece com o detalhe do plano aberto, por cima do modal. Eventos sem `page_location` ficam fora do mapa (não há onde desenhar).

## 9. Tolerâncias para corrigir o trabalho do aluno

| Critério | Tolerância |
| --- | --- |
| Contagens da Silver (duplicatas, aprovados, rejeitados por motivo) | ±1% (desempate diferente na deduplicação, regras extras justificadas) |
| Sessões | ±2% (bordas do gap de 30 min) |
| Funil e receita | ±2% |
| AUC honesto | dentro de ±0,03 de 0,9243. Um AUC ≥ 0,98 indica vazamento |
| Medianas (lead → compra, em dias) | ±0,5 dia (`percentile_approx` contra mediana exata) |
| Mapa de calor | contagens (`cliques`, `sessoes`, `sessoes_na_pagina`, `sessoes_que_chegaram`) exatas; `taxa_clique`, `intensidade` e `pct_sessoes` com ±0,001. Nomes de coluna exatos (R11), senão o site não acende |
| Silhueta e k | livres (dependem das features de perfil escolhidas). Exigir só que k ∈ {3, 4, 5} e que a escolha seja justificada |

## 10. Como reconhecer cada armadilha no trabalho do aluno

1. **Frete no funil.** Com `add_shipping_info` como etapa obrigatória, a conversão em sequência por plano (entre as sessões com `begin_checkout`) cai de 0,5858 para 0,0356 no Básico e de 0,6133 para 0,0172 no Pro. O Premium cai só de 0,5682 para 0,4886. O frete praticamente só existe no Premium (136 sessões com `add_shipping_info`, contra 15 no Básico e 20 no Pro). Os poucos eventos de frete em Básico e Pro vêm de visitantes que trocaram de plano depois do `add_to_cart` (o gerador troca o plano em 10% dos carrinhos), porque o plano da sessão é o primeiro `select_item`. **Sintoma do aluno que seguiu o 5.4 (contagem sem exigir a sequência) e mesmo assim pôs o frete como etapa obrigatória:** no Básico a linha de `add_shipping_info` tem 15 sessões, cerca de 0,049 de `begin_checkout` (15 ÷ 309), e a etapa seguinte, `add_payment_info`, mostra conversão de cerca de 15,3 (230 ÷ 15), acima de 100%. No Pro são 20 sessões, cerca de 0,049 (20 ÷ 406), e a etapa seguinte mostra cerca de 16,1 (322 ÷ 20).
2. **Reembolso na data errada.** Atribuindo o `refund` à data do próprio evento, 7 de 189 células (data × plano × canal) ficam com receita líquida negativa, por exemplo -1144,0 em 2026-09-09 (PLANO-PREMIUM, google / organic) e -947,15 em 2026-09-04 (PLANO-PREMIUM, instagram / paid_social).
3. **Vazamento no modelo.** AUC 0,9886, F1 0,7852 e precisão 0,6573 com eventos do checkout, contra AUC 0,9243, F1 0,5065 e precisão 0,3421 só com a landing.
4. **Duplicata de lote.** Sem deduplicar por `event_id`, a Bronze tem 64372 linhas para 63716 `event_id` distintos, e há 609 `purchase` contra 604 distintos. A receita bruta de `purchase` na Bronze fica em 359250,85, contra 356240,4 deduplicada por `event_id`: diferença de 3010,45.
5. **`session_hint` do cliente.** Contando por `session_hint` há 11440 sessões distintas, contra 8793 com o gap de 30 minutos.

## 11. Roteiro de correção

| Critério | Pontos | O que olhar |
| --- | --- | --- |
| Arquitetura (R1–R4) | 15 | Um job por camada, verificação de camada antes de ler, Bronze append-only com proveniência (arquivo de origem). Rodar o script de sequência do zero e conferir que termina sem erro. O `pipeline/comum.py` (sessão, caminhos, `exigir_camada`, `ler_eventos`/`gravar_eventos`) vem pronto: usá-lo não vale ponto nem desconta; o que conta é o resto de cada job. |
| Silver (R5–R10) | 25 | Linhas na Bronze 64372, duplicatas 656, aprovados 61813 e rejeitados por motivo (seção 3), dentro de ±1%. Sessões perto de 8793 (±2%) e calculadas sem `session_hint`. Stitching de visitante presente. |
| Gold (seção 5.3 do enunciado) | 30 | As 15 tabelas existem. Funil, receita e lead batem com as seções 4 a 6 (±2%). Modelo sem vazamento: AUC honesto em 0,9243 ±0,03 e nunca ≥ 0,98. Mapa de calor bate com a seção 8 e acende no site: rodar o job de exportação, abrir `site/index.html` e clicar em **Mapa de calor** (R11). |
| Relatório | 20 | Cada resposta cita tabela e número da Gold. A recomendação é coerente com os dados (por exemplo, a maior perda do funil em `select_item`, o reembolso alto em instagram / paid_social, os termos de FAQ sem resultado). |
| Qualidade do dado | 10 | Identifica ao menos três das cinco armadilhas da seção 10 com o sintoma em número, além de duplicatas e rejeições. |

O formato (individual ou em dupla) e o prazo ficam a critério do professor.
