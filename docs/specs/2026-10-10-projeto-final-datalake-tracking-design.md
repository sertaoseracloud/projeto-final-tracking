# Projeto final — datalake de tracking de uma landing page de conversão

**Data:** 2026-10-10
**Repositório:** `projeto-final-tracking` (isolado, independente do `aula-pratica-senai`)
**Status:** design aprovado em conversa; aguardando revisão deste documento antes do plano de implementação

---

## 1. Propósito

Este é o **projeto final** da trilha de laboratórios do `aula-pratica-senai`. Ele não introduz nenhuma técnica que o aluno não tenha visto: pede que ele **junte tudo num único sistema**, a partir de um problema de negócio, em vez de completar um laboratório isolado.

O problema: uma empresa vende um curso online por meio de uma landing page de conversão e instrumentou a página com 20 eventos de tracking. Os eventos chegam a um bucket S3 como JSON Lines. O aluno constrói o datalake medalhão que transforma esses eventos em respostas para quatro perguntas de negócio:

1. **Funil** — em que etapa a jornada de compra perde mais gente, por plano e por canal?
2. **Engajamento** — o vídeo de apresentação é visto até o fim? O que o visitante procura no FAQ e não encontra?
3. **Receita e aquisição** — quanto entrou de fato, descontados os reembolsos, e qual canal traz cliente que não desiste?
4. **Previsão** — quais sessões estão perto de comprar, e que perfis de visitante existem?

### O que o projeto consolida

| Aula | Conceito retomado no projeto final |
| --- | --- |
| AULA01–02 | o dado chega com defeitos de entrega: reenvio de lote (duplicata), escrita "confirmada" que não é o que se esperava |
| AULA03 | DataFrame, schema fixo, validação com motivo de rejeição, Parquet particionado |
| AULA04 | Spark local + SDK de nuvem (`boto3`) contra S3 emulado (floci); baixar → processar → subir |
| AULA05 | um job por camada (Bronze/Silver/Gold), cada um checando que a camada anterior existe |
| AULA06 | Spark ML na Gold: classificação e clusterização, com cuidado com vazamento de informação |
| **novo** | dado semiestruturado (JSON aninhado, `map`, `array`), window functions para sessionização, identity stitching |

As únicas técnicas novas (JSON aninhado, `explode`, `Window` com `lag`) são as que dado de tracking exige e nenhum laboratório anterior pediu.

---

## 2. O produto hipotético

**DadosPro — Formação em Análise de Dados**, curso online vendido por uma landing page de página única.

**Seções da landing:** hero com CTA → vídeo de apresentação (VSL) → módulos → depoimentos → tabela de planos → e-book gratuito → garantia de 7 dias → FAQ com busca → rodapé com redes sociais. Fora da landing: checkout, página de obrigado e área de membros.

**Planos e preços:**

| Item | `item_id` | Preço | Observação |
| --- | --- | --- | --- |
| Plano Básico | `PLANO-BASICO` | R$ 297,00 | acesso por 1 ano |
| Plano Pro | `PLANO-PRO` | R$ 597,00 | acesso vitalício + mentoria |
| Plano Premium | `PLANO-PREMIUM` | R$ 997,00 | inclui **apostila impressa** enviada pelos Correios |
| Mentoria em grupo | `BUMP-MENTORIA` | R$ 147,00 | *order bump* oferecido no checkout |

- Frete só existe no Premium: PAC R$ 24,90 ou SEDEX R$ 49,90.
- Pix tem 5% de desconto (cupom `PIX5`).
- `tax` = 5% de ISS sobre `value`.
- Moeda única: BRL.

### Os 20 eventos nesta landing

| Família | Evento | Quando dispara |
| --- | --- | --- |
| Engajamento | `page_view` | entrada em landing, checkout, obrigado ou membros |
| | `scroll` | 25, 50, 75 e 90% da página, uma vez por `page_view` |
| | `click` | saída para rede social (`outbound=true`) ou CTA interno (`outbound=false`) |
| | `site_search` | busca no FAQ, com `search_term` e `search_results` |
| | `video_start` / `video_progress` / `video_complete` | VSL; progresso em 10, 25, 50 e 75% |
| | `file_download` | ementa em PDF |
| E-commerce | `view_item_list` | tabela de planos entra na tela |
| | `select_item` | clique em um plano |
| | `view_item` | modal de detalhe do plano |
| | `add_to_cart` / `remove_from_cart` | escolha ou troca de plano; marcar ou desmarcar o order bump |
| | `view_cart` | resumo do pedido no checkout |
| | `begin_checkout` | "Continuar" no formulário de dados |
| | `add_shipping_info` | escolha de PAC ou SEDEX — **só no Premium** |
| | `add_payment_info` | escolha de Pix, cartão à vista ou cartão parcelado |
| | `purchase` | compra concluída |
| | `refund` | pedido de garantia em até 7 dias, referenciando o `transaction_id` da compra |
| Aquisição | `generate_lead` | e-mail em troca do e-book (o e-mail **não** entra no evento) |
| | `sign_up` / `login` | conta na área de membros; passa a preencher `user_id` |

---

## 3. Entregáveis do aluno

O aluno recebe o repositório com o gerador pronto, a página de exemplo pronta e os seis jobs do pipeline com **oito funções incompletas** (`# TODO`). Ele entrega:

1. **O pipeline completo** — os oito TODOs implementados; `python pipeline/executar_pipeline.py` roda do início ao fim contra o floci sem erro.
2. **`RELATORIO.md`** — respostas às quatro perguntas de negócio da seção 1, cada uma com os números tirados das tabelas Gold que as sustentam e uma recomendação para a empresa.
3. **Uma reflexão sobre a sujeira** — no mesmo relatório: quantos eventos a Silver rejeitou, por qual motivo, e se algum limiar de validação rejeitou dado legítimo.

**Desafio opcional:** navegar pela página de `site/`, exportar o JSONL, subi-lo com `gerador/importar_site.py`, reprocessar e localizar a própria sessão nas tabelas Gold.

### Os oito TODOs

| # | Job | Função a completar | Técnica |
| --- | --- | --- | --- |
| 1 | Bronze | acrescentar proveniência (`_arquivo_origem`, `_ingerido_em`, `dt`) | `input_file_name`, `current_timestamp` |
| 2 | Silver | extrair `params` para colunas tipadas | acesso a `map`, `cast` |
| 3 | Silver | validar com motivo de rejeição (seção 6.3) | cadeia `F.when` + `F.coalesce(..., F.lit(False))`, um `left_anti` join |
| 4 | Silver | sessionizar (gap de 30 min) | `Window`, `lag`, soma cumulativa |
| 5 | Gold funil | conversão entre etapas consecutivas | agregação + `lag` sobre a ordem das etapas |
| 6 | Gold engajamento | curva de retenção do vídeo | pivot por `video_percent` |
| 7 | Gold receita | receita líquida por data **da compra** | join `refund` → `purchase` por `transaction_id` |
| 8 | Gold ML | montar o `Pipeline` de features e treinar | `StringIndexer`, `VectorAssembler`, `LogisticRegression` |

O resto de cada job (leitura, escrita, verificação de camada, impressão de resumo) vem pronto, como nos laboratórios.

### Avaliação (rubrica sugerida, 100 pontos)

| Critério | Pontos | Como se verifica |
| --- | --- | --- |
| Bronze e Silver corretas (TODOs 1–4) | 30 | contagens de aprovados e rejeitados por motivo batem com o esperado em `docs/professor/GABARITO.md` |
| Golds corretas (TODOs 5–8) | 30 | tabelas Gold batem com o esperado; AUC do modelo dentro da faixa sem vazamento |
| Relatório: respostas sustentadas por números | 25 | cada resposta cita a tabela Gold e o número |
| Reflexão sobre sujeira e limiares | 15 | identifica ao menos as três armadilhas da seção 8 |

O formato (individual ou em dupla) e o prazo ficam a critério do professor.

### Gabarito separado do enunciado

Diferente dos laboratórios, em que gabarito e enunciado ficam no mesmo `EXERCICIOS_*.md`, aqui o gabarito vai para `docs/professor/GABARITO.md`, com os números esperados para a seed padrão. O professor decide se remove essa pasta antes de distribuir o repositório.

---

## 4. Arquitetura

```
 ┌────────────── fora do pipeline ──────────────┐        ┌──────────────── pipeline (projeto do aluno) ────────────────┐
 │                                              │        │                                                              │
 │  site/index.html ──export JSONL──┐           │        │  01_bronze ─► 02_silver ─┬─► 03_gold_funil                   │
 │                                  ▼           │        │                          ├─► 04_gold_engajamento             │
 │  gerador/gerar_eventos.py ─► s3://.../landing/eventos/dt=…/*.jsonl ─────────────► ├─► 05_gold_receita_aquisicao       │
 │  gerador/importar_site.py ───────┘           │        │                          └─► 06_gold_ml                      │
 └──────────────────────────────────────────────┘        └──────────────────────────────────────────────────────────────┘
                                       contrato: formato do evento (seção 5) + prefixo landing/
```

### Estrutura do repositório

```
projeto-final-tracking/
├── site/index.html               landing de exemplo; dispara os 20 eventos; exporta JSONL
├── gerador/                      simulador do coletor — NÃO importa nada de pipeline/
│   ├── gerar_eventos.py          CLI: --dias --visitantes --seed --sujeira --data-inicial
│   ├── jornadas.py               máquina de estados da jornada na landing
│   ├── sujeira.py                defeitos injetados
│   ├── importar_site.py          sobe um JSONL exportado do site/ para a landing do S3
│   ├── s3.py                     cliente boto3 próprio do gerador
│   └── README.md
├── pipeline/
│   ├── comum.py                  sessão Spark + cliente S3 + subir_pasta / baixar_pasta / prefixo_existe
│   ├── 01_bronze_ingestao.py
│   ├── 02_silver_eventos.py
│   ├── 03_gold_funil.py
│   ├── 04_gold_engajamento.py
│   ├── 05_gold_receita_aquisicao.py
│   ├── 06_gold_ml.py
│   ├── executar_pipeline.py      roda os seis jobs em sequência (não gera dados)
│   └── verificar_ambiente.py
├── docs/
│   ├── ENUNCIADO.md              problema, entregáveis, TODOs, rubrica (seções 1–3 deste spec, para o aluno)
│   ├── CONTRATO_EVENTO.md        formato do evento e layout da landing no S3
│   ├── EXERCICIOS.md             enunciado e resposta esperada de cada TODO, sem gabarito
│   ├── professor/GABARITO.md     código de cada TODO + números esperados para a seed padrão
│   └── specs/                    este documento
├── docker-compose.yml            floci
├── requirements.txt              pyspark==3.5.3, boto3
├── SETUP.md                      Python 3.11, Java 17, winutils, floci — autossuficiente
└── README.md
```

### Ambiente

- PySpark 3.5.3 local, Python 3.11, Java 17 e, no Windows, `winutils.exe`.
- floci em Docker emulando S3, com o mesmo padrão de variáveis de ambiente do Lab 13: `AWS_ENDPOINT_URL`, `AWS_S3_BUCKET` e credenciais fictícias.
- `SETUP.md` e `README.md` não dependem do `aula-pratica-senai`: tudo o que é necessário para rodar está explicado no próprio repositório.

### Layout do bucket

| Prefixo | Escrito por | Formato |
| --- | --- | --- |
| `landing/eventos/dt=YYYY-MM-DD/coletor-<lote>.jsonl` | gerador / importar_site | JSON Lines, vários lotes por dia |
| `bronze/eventos/dt=…/` | 01 | Parquet, append-only |
| `silver/eventos/dt=…/` | 02 | Parquet, aprovados |
| `silver/itens/dt=…/` | 02 | Parquet, `items[]` explodido |
| `silver/eventos_rejeitados/` | 02 | Parquet, com `motivo_rejeicao` |
| `gold/funil_compra/`, `gold/funil_lead/` | 03 | Parquet |
| `gold/sessoes/`, `gold/retencao_video/`, `gold/buscas_faq/`, `gold/saidas_downloads/` | 04 | Parquet |
| `gold/receita_diaria/`, `gold/taxa_reembolso/`, `gold/adesao_order_bump/`, `gold/lead_para_compra/` | 05 | Parquet |
| `gold/propensao_sessao/`, `gold/metricas_modelo/`, `gold/segmentos_visitantes/` | 06 | Parquet |

Cada job verifica com `prefixo_existe()` que a camada anterior existe e, se não existir, para com uma mensagem que diz qual comando rodar. A Bronze, se não houver nada em `landing/eventos/`, orienta: "nenhum evento na landing: rode `python gerador/gerar_eventos.py` primeiro".

---

## 5. Contrato do evento

Um evento por linha de JSON. É o mesmo formato que `site/index.html` produz e que o gerador grava.

```json
{
  "event_id": "69e0446a-5000-4c95-9a90-0195c8f66be1",
  "event_name": "purchase",
  "event_ts": "2026-10-03T14:22:05.123Z",
  "user_pseudo_id": "anon-9a46b593",
  "user_id": null,
  "session_hint": "s-289b523a",
  "page": { "location": "https://loja.exemplo/checkout", "referrer": "https://loja.exemplo/" },
  "traffic": { "source": "google", "medium": "cpc", "campaign": "formacao_dados" },
  "device": { "category": "mobile", "os": "Android", "browser": "Chrome" },
  "params": { "transaction_id": "T-5BA58C26", "value": 1086.8, "tax": 54.34, "shipping": 49.9,
              "currency": "BRL", "payment_type": "pix", "coupon": "PIX5" },
  "items": [ { "item_id": "PLANO-PREMIUM", "item_name": "Plano Premium", "item_category": "plano",
               "price": 997.0, "quantity": 1 } ]
}
```

| Campo | Regra |
| --- | --- |
| `event_id` | UUID; único por evento **emitido**, mas pode chegar repetido (reenvio de lote) |
| `event_ts` | ISO-8601 UTC, horário do navegador |
| `user_pseudo_id` | cookie anônimo; estável por navegador |
| `user_id` | `u-<hash>` após `sign_up`/`login`; nulo antes |
| `session_hint` | id de sessão do cliente — **não confiável**, a Silver o ignora |
| `params` | varia por evento (tabela abaixo); valores numéricos em número JSON |
| `items` | presente nos eventos de e-commerce; `[]` nos demais |
| dado pessoal | **nunca** há e-mail, nome ou documento no evento |

**`params` por evento:**

| Evento | Chaves |
| --- | --- |
| `page_view` | `page_title` |
| `scroll` | `percent_scrolled` |
| `click` | `link_url`, `link_domain`, `link_text`, `link_id`, `outbound` |
| `site_search` | `search_term`, `search_results`, `search_area` |
| `video_*` | `video_title`, `video_provider`, `video_duration`, `video_current_time`, `video_percent` |
| `file_download` | `file_name`, `file_extension`, `link_url` |
| `view_item_list`, `select_item` | `item_list_id`, `item_list_name` |
| `view_item`, `add_to_cart`, `remove_from_cart`, `view_cart`, `begin_checkout` | `currency`, `value` |
| `add_shipping_info` | `currency`, `value`, `shipping_tier`, `shipping` |
| `add_payment_info` | `currency`, `value`, `payment_type` |
| `purchase` | `transaction_id`, `value`, `tax`, `shipping`, `currency`, `payment_type`, `coupon` |
| `refund` | `transaction_id`, `value`, `tax`, `shipping`, `currency`, `reason` |
| `generate_lead` | `lead_source`, `currency`, `value` |
| `sign_up`, `login` | `method` |

`value` é sempre a receita dos itens **sem frete**, já com o desconto do Pix.

---

## 6. Pipeline, job a job

### 6.1 `01_bronze_ingestao.py`

Baixa `landing/eventos/` e lê com **schema fixo** (`params` como `map<string,string>`, `items` como `array<struct>`). Acrescenta proveniência (**TODO 1**) e grava `bronze/eventos/` particionado por `dt`, em modo append.

A Bronze **não deduplica**. Rodar o gerador duas vezes sobre o mesmo dia, simulando um coletor que reenviou o lote, deixa duplicatas na Bronze de propósito.

### 6.2 `02_silver_eventos.py` — etapas em ordem

1. Deduplicação por `event_id`, mantendo a primeira ocorrência por `_ingerido_em`.
2. Extração tipada dos `params` (**TODO 2**).
3. Validação com motivo (**TODO 3**), seção 6.3.
4. Sessionização (**TODO 4**): por `user_pseudo_id`, ordenado por `event_ts`, uma sessão nova começa quando o intervalo para o evento anterior passa de 30 minutos. Gera `session_id = user_pseudo_id || '-' || n`.
5. Identity stitching: para cada `user_pseudo_id`, o `user_id` não nulo mais recente vira `user_id_resolvido` em **todos** os eventos daquele pseudo, inclusive os anteriores ao login.
6. `explode(items)` para formar `silver/itens`.

### 6.3 Regras de validação (prioridade de cima para baixo; vale o primeiro motivo)

| # | Motivo | Condição |
| --- | --- | --- |
| 1 | `evento fora do catalogo` | `event_name` fora dos 20 da seção 2 |
| 2 | `timestamp ausente ou futuro` | `event_ts` nulo ou maior que `_ingerido_em` |
| 3 | `transacao sem id` | `purchase` ou `refund` com `transaction_id` nulo |
| 4 | `valor monetario invalido` | `value` < 0, ou `currency` ≠ `BRL` em evento com `value` |
| 5 | `marco fora do padrao` | `percent_scrolled` ∉ {25, 50, 75, 90} ou `video_percent` ∉ {0, 10, 25, 50, 75, 100} |
| 6 | `reembolso sem compra` | `refund` cujo `transaction_id` não existe em nenhum `purchase` aprovado (`left_anti` join) |

### 6.4 `03_gold_funil.py`

- **`funil_compra`:** etapas em ordem — `view_item_list → select_item → view_item → add_to_cart → view_cart → begin_checkout → add_payment_info → purchase`. Conta sessões que atingiram cada etapa e calcula a conversão sobre a etapa anterior (**TODO 5**) e sobre o topo. Grava no total e por plano × canal.
- `add_shipping_info` **fica fora** do funil geral e aparece só num recorte do Premium.
- **`funil_lead`:** `page_view` (landing) → `scroll` 50% → `generate_lead`.

### 6.5 `04_gold_engajamento.py`

- **`sessoes`:** uma linha por `session_id`, com duração, número de eventos, scroll máximo, se viu o vídeo, `video_percent` máximo, se fez lead, se comprou, canal e dispositivo.
- **`retencao_video`:** percentual das sessões com `video_start` que chegaram a 10, 25, 50, 75 e 100% (**TODO 6**).
- **`buscas_faq`:** `search_term`, total de buscas e quantas tiveram `search_results = 0`.
- **`saidas_downloads`:** cliques com `outbound` por domínio, e downloads por arquivo.

### 6.6 `05_gold_receita_aquisicao.py`

- **`receita_diaria`:** por **data da compra original**, plano e canal. Inclui receita bruta (`purchase.value`), reembolsos (`refund.value` atribuído à data da compra via `transaction_id`), receita líquida (**TODO 7**), pedidos e ticket médio.
- **`taxa_reembolso`:** por plano e canal.
- **`adesao_order_bump`:** percentual de compras que incluem `BUMP-MENTORIA`.
- **`lead_para_compra`:** leads (por `user_id_resolvido`, ou pelo pseudo se não houver), quantos compraram depois e a mediana de dias entre lead e compra.

### 6.7 `06_gold_ml.py`

- **Propensão (TODO 8):**
  - Uma linha por sessão de `gold/sessoes`; o rótulo é `comprou`.
  - Features **só do que acontece antes de `begin_checkout`**: scroll máximo, `video_percent` máximo, viu a tabela de planos, plano visto, canal, dispositivo, buscas no FAQ e downloads.
  - `StringIndexer` → `OneHotEncoder` → `VectorAssembler` → `LogisticRegression`, com split 80/20 e seed fixa.
  - Grava `propensao_sessao` (sessão, probabilidade) e `metricas_modelo` (AUC, F1).
- **Segmentação:** K-Means sobre perfis agregados por visitante, com k ∈ {3, 4, 5} escolhido pelo silhouette. Grava `segmentos_visitantes` com o perfil médio de cada grupo. Esta parte vem pronta.

---

## 7. O gerador

**`gerar_eventos.py`** — Python puro + `boto3`, sem Spark.

| Parâmetro | Padrão |
| --- | --- |
| `--dias` | 14 |
| `--visitantes` | 3000 |
| `--seed` | 42 |
| `--sujeira` | 0,03 |
| `--data-inicial` | 2026-09-01 |

**Simulação:**
- Cada visitante recebe canal (`direct`, `google/cpc`, `google/organic`, `instagram/paid_social`, `newsletter/email`) e dispositivo.
- O visitante faz de uma a quatro sessões em dias diferentes.
- Cada sessão percorre a máquina de estados de `jornadas.py`. As probabilidades de avançar (rolar, ver o vídeo, ver os planos, escolher, iniciar checkout, pagar) dependem do canal e do dispositivo; por exemplo, mobile vindo de Instagram rola mais e compra menos.

**Comportamentos que precisam aparecer no dado:**
- **Lead que volta:** parte dos leads volta em sessão posterior e compra. Parte deles faz `sign_up` e, depois, `login`.
- **Reembolso tardio:** cerca de 8% das compras recebem `refund` de 1 a 7 dias depois, alguns caindo em outro dia.
- **Order bump e troca de plano:** o order bump é adicionado e às vezes removido; a troca de plano gera `remove_from_cart`.
- **Frete:** só pedidos Premium têm `add_shipping_info`.

**`sujeira.py`** — sobre a fração `--sujeira`, injeta:
- `event_id` duplicado em outro lote;
- `event_ts` no futuro;
- `purchase` sem `transaction_id`;
- `value` negativo;
- `currency = USD`;
- `percent_scrolled = 33`;
- `refund` com `transaction_id` inexistente;
- `event_name` fora do catálogo (`add_to_wishlist`).

O gerador grava, para cada dia, de três a seis lotes `coletor-<lote>.jsonl` em `landing/eventos/dt=…/`. Ao final imprime o resumo: visitantes, sessões, eventos por nome e defeitos injetados por tipo. É esse resumo que permite ao professor conferir a Silver.

**`importar_site.py <arquivo.jsonl>`** — valida que cada linha é JSON com `event_id` e `event_ts` e sobe o arquivo em `landing/eventos/dt=<data do primeiro evento>/site-<timestamp>.jsonl`.

---

## 8. Armadilhas plantadas (cada uma vira uma lição no README)

1. **Frete no funil.** Incluir `add_shipping_info` como etapa obrigatória zera a conversão de Básico e Pro. O funil precisa respeitar que nem toda etapa vale para todo produto.
2. **Reembolso na data errada.** Atribuir o `refund` à data do próprio evento produz dias com receita líquida negativa. A receita líquida pertence à data da compra.
3. **Vazamento no modelo.** Usar features de checkout (`add_payment_info`, `begin_checkout`) dá AUC perto de 0,99, porque o modelo "prevê" a compra olhando a própria compra. O exercício limita as features ao pré-checkout.
4. **Duplicata que a Bronze guarda.** Rodar o gerador duas vezes infla a Bronze. Só a Silver, deduplicando por `event_id`, devolve os números certos.
5. **`session_hint` do cliente.** Ele muda a cada recarga da página. Contar sessões por ele infla a métrica; a sessão é calculada pela Silver.

---

## 9. Números esperados

Rodando o gerador com os parâmetros padrão e o pipeline completo, `docs/professor/GABARITO.md` registra os números medidos:
- eventos gerados;
- duplicatas removidas;
- rejeitados por motivo;
- sessões;
- conversão por etapa;
- receita bruta, reembolsada e líquida;
- AUC do modelo.

Como nos laboratórios, os números publicados são os **medidos na execução**. Se algum resultado contrariar o esperado, o texto diz o que aconteceu.

---

## 10. Fora de escopo

- Coletor HTTP real, streaming (Kafka/Kinesis) ou processamento incremental: o pipeline é em lote.
- Nuvem real ou Azure: só S3 via floci.
- Dashboard ou visualização: as respostas vão no `RELATORIO.md`, tiradas das tabelas Gold.
- Consentimento e LGPD além da regra "nenhum dado pessoal no evento".

---

## 11. Critérios de pronto do material

- [ ] `site/index.html` dispara os 20 eventos no contrato da seção 5 (**já feito e verificado no navegador**).
- [ ] O gerador roda com os padrões, grava em `landing/` e imprime o resumo; mesma seed gera os mesmos eventos.
- [ ] Com o gabarito aplicado, `executar_pipeline.py` roda os seis jobs do início ao fim contra o floci.
- [ ] Sem o gabarito, cada job para no TODO com erro identificável, como nos laboratórios.
- [ ] As cinco armadilhas da seção 8 são observáveis nos dados gerados.
- [ ] `GABARITO.md` traz os números medidos; `ENUNCIADO.md`, `EXERCICIOS.md` e `CONTRATO_EVENTO.md` são consistentes com este spec.
- [ ] `SETUP.md` permite rodar do zero numa máquina Windows sem consultar o `aula-pratica-senai`.
