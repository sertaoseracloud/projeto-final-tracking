# Projeto final — datalake de tracking de uma landing page de conversão

**Data:** 2026-10-10
**Repositório:** `projeto-final-tracking`
**Status:** design aprovado em conversa; aguardando revisão deste documento antes do plano de implementação

---

## 1. Propósito

Projeto final de engenharia de dados. O aluno recebe um problema de negócio e uma fonte de eventos, e **constrói do zero** um datalake em arquitetura medalhão que responde às perguntas da empresa.

**O problema.** Uma empresa vende um curso online por meio de uma landing page de conversão. A página foi instrumentada com 20 eventos de tracking, que chegam a um bucket S3 como arquivos JSON Lines. Ninguém olha esses dados ainda. A empresa quer respostas para quatro perguntas:

1. **Funil** — em que etapa a jornada de compra perde mais gente, por plano e por canal?
2. **Engajamento** — o vídeo de apresentação é visto até o fim? O que o visitante procura no FAQ e não encontra?
3. **Receita e aquisição** — quanto entrou de fato, descontados os reembolsos, e qual canal traz cliente que não desiste?
4. **Previsão** — quais sessões estão perto de comprar, e que perfis de visitante existem?

**Competências avaliadas:**
- ingestão de dado semiestruturado (JSON aninhado, `map`, `array`) com schema explícito;
- arquitetura medalhão com um job por camada;
- validação com motivo de rejeição;
- deduplicação;
- sessionização com window functions;
- identity stitching;
- modelagem de métricas de funil e receita;
- Spark ML sem vazamento de informação;
- leitura e escrita em armazenamento de objetos (S3).

---

## 2. O produto hipotético

**DadosPro — Formação em Análise de Dados**, curso online vendido por uma landing page de página única.

**Seções da landing:** hero com CTA → vídeo de apresentação (VSL) → módulos → depoimentos → tabela de planos → e-book gratuito → garantia de 7 dias → FAQ com busca → rodapé com redes sociais. Fora da landing: checkout, página de obrigado e área de membros.

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

### Os 20 eventos

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

## 3. O que o aluno recebe e o que entrega

### Recebe (pronto)

| Item | Para quê |
| --- | --- |
| `site/index.html` | landing de exemplo que dispara os 20 eventos e exporta JSONL; serve para ver o evento nascer |
| `gerador/` | simulador do coletor: popula `landing/` no S3 com semanas de eventos sintéticos, inclusive defeitos |
| `docker-compose.yml` + `Dockerfile` | dois contêineres: `floci` (S3 emulado) e `app` (Python 3.11, Java 17, PySpark 3.5.3 com o conector `s3a`, `boto3`, `pytest`). **Nada é instalado na máquina além do Docker**; nenhuma conta de nuvem é necessária |
| `infra/verificar_ambiente.py` | confere, de dentro do contêiner `app`, que o Spark lê e grava no floci por `s3a://` |
| `SETUP.md` | ambiente do zero com Docker: subir os contêineres, popular a landing, rodar o pipeline |
| `docs/ENUNCIADO.md` | problema, requisitos, entregáveis e rubrica (seções 1–6 e 8 deste spec) |
| `docs/CONTRATO_EVENTO.md` | formato do evento e layout da landing no S3 (seção 5) |

### Entrega

1. **`pipeline/`** — código PySpark que atende aos requisitos da seção 6, mais um `pipeline/README.md` dizendo como rodar.
2. **`RELATORIO.md`** — respostas às quatro perguntas da seção 1. Cada resposta cita a tabela Gold e os números que a sustentam e termina com uma recomendação para a empresa.
3. **Seção "Qualidade do dado"** no relatório:
   - quantos eventos foram descartados como duplicata;
   - quantos foram rejeitados, por qual motivo;
   - se alguma regra rejeitou dado legítimo;
   - quais armadilhas o aluno encontrou e como as tratou.

**Desafio opcional:** navegar pela página de `site/`, exportar o JSONL, subi-lo com `gerador/importar_site.py`, reprocessar e localizar a própria sessão nas tabelas Gold.

---

## 4. Arquitetura

```
 ┌──────────── recebido pronto ─────────────┐          ┌────────────── construído pelo aluno ──────────────┐
 │                                          │          │                                                    │
 │  site/index.html ──export JSONL──┐       │          │  bronze ─► silver ─┬─► gold funil                  │
 │                                  ▼       │          │                    ├─► gold engajamento            │
 │  gerador/ ──────► s3://<bucket>/landing/eventos/dt=…/*.jsonl ──────────► ├─► gold receita e aquisição     │
 │                                          │          │                    └─► gold ML                     │
 └──────────────────────────────────────────┘          └────────────────────────────────────────────────────┘
                                 fronteira: formato do evento (seção 5) + prefixo landing/
```

### Estrutura do repositório

```
projeto-final-tracking/
├── site/index.html               landing de exemplo
├── gerador/                      simulador do coletor — independente do pipeline
│   ├── gerar_eventos.py          CLI: --dias --visitantes --seed --sujeira --data-inicial
│   ├── jornadas.py               máquina de estados da jornada na landing
│   ├── sujeira.py                defeitos injetados
│   ├── importar_site.py          sobe um JSONL exportado do site/ para a landing
│   ├── destinos.py               destino S3 (boto3) ou pasta local (--saida-local)
│   └── README.md
├── pipeline/                     VAZIO no pacote do aluno (só um README com o que se espera)
├── docs/
│   ├── ENUNCIADO.md
│   ├── CONTRATO_EVENTO.md
│   └── specs/                    este documento
├── professor/                    NÃO vai para o aluno (export-ignore)
│   ├── solucao/                  pipeline de referência completo (seção 7)
│   ├── GABARITO.md               números esperados para a seed padrão + roteiro de correção
│   └── README.md                 como gerar o pacote do aluno
├── infra/verificar_ambiente.py   Spark ↔ floci por s3a (leitura da landing + escrita/leitura de teste)
├── Dockerfile                    imagem app: Python 3.11 + Java 17 + PySpark 3.5.3 + hadoop-aws
├── docker-compose.yml            serviços floci e app
├── requirements.txt              pyspark==3.5.3, boto3 (só o gerador), pytest
├── .gitattributes                professor/ export-ignore
├── SETUP.md
└── README.md
```

**Pacote do aluno.** `git archive --format=zip -o projeto-final-aluno.zip HEAD` gera o zip sem `professor/`, `docs/specs/` e `docs/superpowers/` (todos `export-ignore` no `.gitattributes`, porque descrevem a solução). O professor distribui o zip, nunca o repositório. Isso fica documentado em `professor/README.md`.

**Execução em contêiner.** Todo comando roda no serviço `app`: `docker compose run --rm app python <script>`. O repositório é montado em `/projeto`, e o `app` alcança o floci em `http://floci:4566`.

### Layout da landing no S3

`s3://<bucket>/landing/eventos/dt=YYYY-MM-DD/<origem>-<lote>.jsonl` — vários lotes por dia, como um coletor que descarrega em intervalos. `<origem>` é `coletor` (gerador) ou `site` (importação). Bucket e endpoint vêm de `AWS_S3_BUCKET` e `AWS_ENDPOINT_URL`.

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
| `session_hint` | id de sessão do cliente; muda a cada recarga de página — **não é confiável** |
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
| `video_*` | `video_title`, `video_provider`, `video_duration`, `video_current_time`, `video_percent` (0 no start, 100 no complete) |
| `file_download` | `file_name`, `file_extension`, `link_url` |
| `view_item_list`, `select_item` | `item_list_id`, `item_list_name` |
| `view_item`, `add_to_cart`, `remove_from_cart`, `view_cart`, `begin_checkout` | `currency`, `value` |
| `add_shipping_info` | `currency`, `value`, `shipping_tier`, `shipping` |
| `add_payment_info` | `currency`, `value`, `payment_type` |
| `purchase` | `transaction_id`, `value`, `tax`, `shipping`, `currency`, `payment_type`, `coupon` |
| `refund` | `transaction_id`, `value`, `tax`, `shipping`, `currency`, `reason` |
| `generate_lead` | `lead_source`, `currency`, `value` |
| `sign_up`, `login` | `method` |

`value` é sempre a receita dos itens **sem frete**. Em `purchase` e `refund` ele já vem com o desconto do Pix; nas etapas anteriores (`view_cart` até `add_payment_info`) é o valor de tabela, porque o desconto só existe depois que o pagamento é escolhido.

---

## 6. Requisitos do pipeline (o que o aluno precisa construir)

### 6.1 Obrigatórios de arquitetura

- **R1.** PySpark, lendo de `landing/` e gravando todas as camadas no mesmo bucket S3 (floci), em Parquet, **diretamente pelo Spark com `s3a://`**. O pipeline não usa `boto3` nem outro SDK de nuvem, não baixa arquivos para o disco local e roda no contêiner `app`.
- **R2.** **Um job por camada**, executáveis separadamente:
  - um job Bronze, um job Silver e **quatro jobs Gold, cada um num arquivo próprio** (funil, engajamento, receita e aquisição, ML);
  - cada job verifica que a camada de que depende existe e, se não existir, para com uma mensagem que diz o que rodar antes.
- **R3.** Um script que roda os seis jobs em sequência. Ele **não** gera dados.
- **R4.** Bronze append-only, com schema explícito (sem inferência), particionada por data do evento e com colunas de proveniência: arquivo de origem e instante da ingestão.

### 6.2 Silver

- **R5.** Deduplicação por `event_id`.
- **R6.** Extração dos `params` para colunas tipadas.
- **R7.** Validação com **motivo de rejeição**. Os rejeitados são gravados numa tabela separada, um motivo por linha. As regras mínimas estão abaixo; o aluno pode acrescentar outras, justificando no relatório.

  | Motivo | Condição |
  | --- | --- |
  | evento fora do catálogo | `event_name` fora dos 20 da seção 2 |
  | timestamp ausente ou futuro | `event_ts` nulo ou posterior ao instante de ingestão |
  | transação sem id | `purchase` ou `refund` sem `transaction_id` |
  | valor monetário inválido | `value` negativo, ou `currency` ≠ BRL |
  | marco fora do padrão | `percent_scrolled` ∉ {25, 50, 75, 90} ou `video_percent` ∉ {0, 10, 25, 50, 75, 100} |
  | reembolso sem compra | `refund` cujo `transaction_id` não existe em nenhum `purchase` válido |

- **R8.** Sessionização própria: uma sessão nova começa quando o mesmo `user_pseudo_id` fica mais de 30 minutos sem evento. **`session_hint` não pode ser usado.**
- **R9.** Identity stitching: todo evento de um `user_pseudo_id` que em algum momento tenha `user_id` recebe esse `user_id`, inclusive os eventos anteriores ao login.
- **R10.** Uma tabela de itens com `items[]` explodido, uma linha por item por evento.

### 6.3 Gold — tabelas obrigatórias

Nomes de colunas são livres; o **conteúdo** é obrigatório, porque é o que a correção compara.

| Job | Tabela | Conteúdo mínimo |
| --- | --- | --- |
| Funil | `funil_compra` | sessões que atingiram cada etapa de `view_item_list → select_item → add_to_cart → view_cart → begin_checkout → add_payment_info → purchase`, conversão sobre a etapa anterior e sobre o topo; total e por plano × canal. `view_item` (abrir o detalhe do plano) é opcional na jornada e **não** entra na sequência: aparece como taxa à parte (sessões com `view_item` ÷ sessões com `select_item`) |
| | `funil_lead` | `page_view` da landing → `scroll` ≥ 50% → `generate_lead`, por canal |
| Engajamento | `sessoes` | uma linha por sessão: duração, eventos, scroll máximo, maior `video_percent`, fez lead, comprou, canal, dispositivo |
| | `retencao_video` | das sessões com `video_start`, percentual que atingiu 10, 25, 50, 75 e 100% |
| | `buscas_faq` | termo, total de buscas, buscas sem resultado |
| | `saidas_downloads` | cliques de saída por domínio; downloads por arquivo |
| Receita e aquisição | `receita_diaria` | por **data da compra**, plano e canal: receita bruta, reembolsada e líquida, pedidos, ticket médio |
| | `taxa_reembolso` | por plano e canal |
| | `adesao_order_bump` | percentual de compras com `BUMP-MENTORIA` |
| | `lead_para_compra` | leads, quantos compraram depois, mediana de dias entre lead e compra |
| ML | `propensao_sessao` | probabilidade de compra por sessão, com modelo de classificação |
| | `metricas_modelo` | AUC e F1 no conjunto de teste, e a lista de features usadas |
| | `segmentos_visitantes` | grupo de cada visitante (clusterização) e perfil médio de cada grupo |

**Restrição do modelo de propensão:** as features só podem usar o comportamento na landing **antes do primeiro `add_to_cart` da sessão**: rolagem, vídeo, tabela de planos vista, plano selecionado, detalhe aberto, buscas, downloads, cliques, canal e dispositivo. Qualquer evento do checkout (`view_cart` em diante) já praticamente revela a compra.

---

## 7. Solução de referência (`professor/solucao/`)

Pipeline completo que atende à seção 6. É com ele que o professor gera os números de `GABARITO.md`. Decisões da referência, que servem de roteiro de correção sem obrigar o aluno a segui-las:

- **Arquivos:**
  - `comum.py` (sessão Spark com `s3a`, `caminho(camada)`, `camada_existe(spark, camada)`);
  - os jobs `01_bronze_ingestao.py`, `02_silver_eventos.py`, `03_gold_funil.py`, `04_gold_engajamento.py`, `05_gold_receita_aquisicao.py`, `06_gold_ml.py`;
  - `executar_pipeline.py`.
- **Acesso ao S3:** `comum.py` cria a `SparkSession` com o conector `s3a` (`hadoop-aws` 3.3.4, o mesmo Hadoop do PySpark 3.5.3) apontado para o floci: endpoint `http://floci:4566`, `path.style.access=true`, SSL desligado, credenciais fixas de teste. Cada job lê e grava direto em `s3a://<bucket>/<camada>/`. Para saber se a camada anterior existe, usa a API `FileSystem` do Hadoop pela JVM do Spark (`spark._jvm.org.apache.hadoop.fs.Path`), sem SDK.
- **Bronze:** `params` como `map<string,string>`, `items` como `array<struct>`. A proveniência vem de `input_file_name()` e `current_timestamp()`; a partição é `dt`.
- **Silver, nesta ordem:**
  1. deduplicação (primeira ocorrência por instante de ingestão);
  2. tipagem dos `params`;
  3. validação em cadeia de `F.when` com prioridade, cada condição protegida por `F.coalesce(..., F.lit(False))` contra nulos, e um `left_anti` join para "reembolso sem compra";
  4. sessionização com `Window` + `lag` + soma cumulativa, gerando `session_id = user_pseudo_id || '-' || n`;
  5. stitching com o `user_id` não nulo mais recente por pseudo;
  6. `explode(items)`.
- **Funil:** `add_shipping_info` fica fora do funil geral e aparece só num recorte do Premium.
- **Receita:** o `refund` é atribuído à data da compra original por join em `transaction_id`.
- **ML:**
  - propensão: `StringIndexer` → `OneHotEncoder` → `VectorAssembler` → `LogisticRegression`, split 80/20 com seed fixa;
  - segmentação: K-Means sobre perfil agregado por visitante, com k ∈ {3, 4, 5} escolhido pelo silhouette.

### `GABARITO.md`

- Números medidos com o gerador nos parâmetros padrão:
  - eventos gerados e duplicatas removidas;
  - rejeitados por motivo;
  - sessões;
  - conversão por etapa;
  - receita bruta, reembolsada e líquida;
  - adesão ao order bump;
  - AUC e F1.
- Tolerâncias aceitas, porque o aluno pode ter regras adicionais ou outro desempate na deduplicação.
- Como reconhecer cada armadilha da seção 9 nos números do aluno.

---

## 8. Avaliação (rubrica, 100 pontos)

| Critério | Pontos | Como se verifica |
| --- | --- | --- |
| Arquitetura (R1–R4): jobs separados, verificação de camada, Bronze append-only com proveniência | 15 | leitura do código + rodar o script de sequência do zero |
| Silver (R5–R10) | 25 | contagens de duplicatas, aprovados e rejeitados por motivo dentro da tolerância do gabarito; sessões calculadas sem `session_hint`; stitching presente |
| Gold (seção 6.3) | 30 | as 13 tabelas existem e batem com o gabarito dentro da tolerância; modelo sem vazamento (AUC fora da faixa de vazamento) |
| Relatório: respostas às quatro perguntas, sustentadas por números da Gold | 20 | cada resposta cita tabela e número; recomendação coerente com os dados |
| Qualidade do dado: duplicatas, rejeições e armadilhas identificadas | 10 | identifica ao menos três das cinco armadilhas da seção 9 |

O formato (individual ou em dupla) e o prazo ficam a critério do professor.

---

## 9. Armadilhas no dado (o aluno precisa percebê-las; o gabarito diz como reconhecê-las)

1. **Frete no funil.** Incluir `add_shipping_info` como etapa obrigatória zera a conversão de Básico e Pro.
2. **Reembolso na data errada.** Atribuir o `refund` à data do próprio evento produz dias com receita líquida negativa. A receita líquida pertence à data da compra.
3. **Vazamento no modelo.** Usar eventos do checkout (`view_cart`, `begin_checkout`, `add_payment_info`) como feature leva a AUC perto de 0,99; com só o comportamento na landing, o AUC fica bem abaixo. O `GABARITO.md` registra as duas faixas medidas.
4. **Duplicata de lote.** O gerador reenvia lotes. Sem deduplicação, receita e conversões saem infladas.
5. **`session_hint` do cliente.** Ele muda a cada recarga. Contar sessões por ele infla a métrica.

---

## 10. O gerador

**`gerar_eventos.py`** — Python puro + `boto3`, sem Spark, sem dependência do pipeline. O gerador é o "mundo externo" (o coletor), por isso continua usando o SDK; a regra "só Spark" vale para o pipeline.

| Parâmetro | Padrão |
| --- | --- |
| `--dias` | 14 |
| `--visitantes` | 6000 |
| `--seed` | 42 |
| `--sujeira` | 0,03 |
| `--data-inicial` | 2026-09-01 |

**Simulação:**
- Cada visitante recebe canal (`direct`, `google/cpc`, `google/organic`, `instagram/paid_social`, `newsletter/email`) e dispositivo.
- O visitante faz de uma a quatro sessões em dias diferentes.
- Cada sessão percorre a máquina de estados de `jornadas.py`. As probabilidades de rolar, ver o vídeo, ver os planos, escolher, iniciar checkout e pagar dependem do canal e do dispositivo.
- O `session_hint` muda a cada `page_view` de recarga, imitando o site.

**Comportamentos que precisam aparecer no dado:**
- **Lead que volta:** parte dos leads volta em sessão posterior e compra. Parte deles faz `sign_up` e, depois, `login`.
- **Reembolso tardio:** cerca de 8% das compras recebem `refund` de 1 a 7 dias depois (compras da última semana podem ficar sem reembolso, porque ele cairia fora da janela).
- **Order bump e troca de plano:** o order bump é adicionado e às vezes removido; a troca de plano gera `remove_from_cart`.
- **Frete:** só pedidos Premium têm `add_shipping_info`.
- **Reenvio de lote:** cerca de 1% dos eventos reaparece em outro lote com o mesmo `event_id`.

**`sujeira.py`** — sobre a fração `--sujeira`, injeta:
- `event_ts` no futuro ou nulo;
- `purchase` sem `transaction_id`;
- `value` negativo;
- `currency = USD`;
- `percent_scrolled = 33`;
- `refund` com `transaction_id` inexistente (no máximo 5% das compras);
- `event_name` fora do catálogo (`add_to_wishlist`).

**Escrita e resumo:**
- Para cada dia, grava de três a seis lotes em `landing/eventos/dt=…/`. O dia seguinte ao último pode ter um ou dois lotes, com sessões que viram a meia-noite.
- Imprime um resumo: visitantes, sessões reais, eventos por nome, duplicatas e defeitos injetados por tipo.
- A mesma seed gera exatamente os mesmos eventos.

**`importar_site.py <arquivo.jsonl>`** — valida que cada linha é JSON com `event_id` e `event_ts` e sobe o arquivo em `landing/eventos/dt=<data do primeiro evento>/site-<timestamp>.jsonl`.

---

## 11. Fora de escopo

- Coletor HTTP real, streaming (Kafka/Kinesis) ou processamento incremental: o pipeline é em lote.
- Nuvem real ou Azure: só S3 via floci.
- Dashboard: as respostas vão no `RELATORIO.md`.
- Consentimento e LGPD além da regra "nenhum dado pessoal no evento".

---

## 12. Critérios de pronto do material

- [x] `site/index.html` dispara os 20 eventos no contrato da seção 5 (verificado no navegador).
- [x] O gerador roda com os padrões, grava em `landing/`, imprime o resumo e é determinístico pela seed.
- [x] As cinco armadilhas da seção 9 são observáveis nos dados gerados.
- [ ] `professor/solucao/` roda os seis jobs do início ao fim contra o floci e produz as 13 tabelas Gold.
- [ ] `GABARITO.md` traz os números medidos e as tolerâncias.
- [ ] `git archive` gera um zip sem `professor/`, conferido listando o conteúdo do zip.
- [ ] `ENUNCIADO.md` e `CONTRATO_EVENTO.md` batem com este spec.
- [ ] `SETUP.md` permite rodar do zero numa máquina com apenas Docker instalado (Windows, macOS ou Linux).
