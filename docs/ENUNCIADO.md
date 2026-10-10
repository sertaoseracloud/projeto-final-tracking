# Enunciado — datalake de tracking da DadosPro

## 1. O problema

Uma empresa vende um curso online por meio de uma landing page de conversão. A página foi instrumentada com 20 eventos de tracking, que chegam a um bucket S3 como arquivos JSON Lines. Ninguém olha esses dados ainda.

Você vai **construir do zero** um datalake em arquitetura medalhão que responda a quatro perguntas da empresa:

1. **Funil** — em que etapa a jornada de compra perde mais gente, por plano e por canal?
2. **Engajamento** — o vídeo de apresentação é visto até o fim? O que o visitante procura no FAQ e não encontra?
3. **Receita e aquisição** — quanto entrou de fato, descontados os reembolsos, e qual canal traz cliente que não desiste?
4. **Previsão** — quais sessões estão perto de comprar, e que perfis de visitante existem?

Competências avaliadas:

- ingestão de dado semiestruturado (JSON aninhado, `map`, `array`) com schema explícito;
- arquitetura medalhão com um job por camada;
- validação com motivo de rejeição;
- deduplicação;
- sessionização com window functions;
- identity stitching;
- modelagem de métricas de funil e receita;
- Spark ML sem vazamento de informação;
- leitura e escrita em armazenamento de objetos (S3).

## 2. O produto: DadosPro

**DadosPro — Formação em Análise de Dados** é um curso online vendido por uma landing page de página única.

**Seções da landing:** hero com CTA, vídeo de apresentação (VSL), módulos, depoimentos, tabela de planos, e-book gratuito, garantia de 7 dias, FAQ com busca e rodapé com redes sociais. Fora da landing ficam o checkout, a página de obrigado e a área de membros.

| Item | `item_id` | Preço | Observação |
| --- | --- | --- | --- |
| Plano Básico | `PLANO-BASICO` | R$ 297,00 | acesso por 1 ano |
| Plano Pro | `PLANO-PRO` | R$ 597,00 | acesso vitalício + mentoria |
| Plano Premium | `PLANO-PREMIUM` | R$ 997,00 | inclui **apostila impressa** enviada pelos Correios |
| Mentoria em grupo | `BUMP-MENTORIA` | R$ 147,00 | *order bump* oferecido no checkout |

- Frete só existe no Premium: PAC R$ 24,90 ou SEDEX R$ 49,90.
- Pix tem 5% de desconto (cupom `PIX5`).
- `tax` é o ISS: 5% sobre `value`.
- Moeda única: BRL.

Os 20 eventos, o formato de cada um e o layout dos arquivos no S3 estão em [`CONTRATO_EVENTO.md`](CONTRATO_EVENTO.md).

## 3. O que você recebe e o que entrega

### Você recebe (pronto)

| Item | Para quê |
| --- | --- |
| `site/index.html` | landing de exemplo que dispara os 20 eventos e exporta JSONL; serve para ver o evento nascer |
| `gerador/` | simulador do coletor: popula `landing/` no S3 com semanas de eventos sintéticos |
| `docker-compose.yml` + `Dockerfile` | dois contêineres: `floci` (S3 emulado) e `app` (Python 3.11, Java 17, PySpark 3.5.3 com o conector `s3a`, `boto3`, `pytest`). Nada é instalado na máquina além do Docker; nenhuma conta de nuvem é necessária |
| `infra/verificar_ambiente.py` | confere, de dentro do contêiner `app`, que o Spark lê e grava no floci por `s3a://` |
| `SETUP.md` | ambiente do zero com Docker |
| `docs/CONTRATO_EVENTO.md` | formato do evento e layout da landing no S3 |

### Você entrega

1. **`pipeline/`** — código PySpark que atende aos requisitos da seção 5, e a atualização do `pipeline/README.md` (já existe, com o acesso ao S3) para dizer também como rodar o seu pipeline.
2. **`RELATORIO.md`** — respostas às quatro perguntas da seção 1. Cada resposta cita a tabela Gold e os números que a sustentam e termina com uma recomendação para a empresa.
3. **Seção "Qualidade do dado"** no relatório:
   - quantos eventos foram descartados como duplicata;
   - quantos foram rejeitados, por qual motivo;
   - se alguma regra rejeitou dado legítimo;
   - quais armadilhas você encontrou e como as tratou.

**Desafio opcional:** navegue pela página de `site/`, exporte o JSONL, suba-o com `gerador/importar_site.py`, reprocesse e localize a sua própria sessão nas tabelas Gold.

## 4. Arquitetura esperada

```text
 ┌──────────── recebido pronto ─────────────┐          ┌────────────── construído por você ────────────────┐
 │                                          │          │                                                    │
 │  site/index.html ──export JSONL──┐       │          │  bronze ─► silver ─┬─► gold funil                  │
 │                                  ▼       │          │                    ├─► gold engajamento            │
 │  gerador/ ──────► s3://<bucket>/landing/eventos/year=…/month=…/day=…/*.jsonl ──────────► ├─► gold receita e aquisição     │
 │                                          │          │                    └─► gold ML                     │
 └──────────────────────────────────────────┘          └────────────────────────────────────────────────────┘
                                 fronteira: formato do evento (CONTRATO_EVENTO.md) + prefixo landing/
```

Todas as camadas ficam no **mesmo bucket**, em prefixos próprios, gravadas em Parquet:

```text
s3://datalake-tracking/
├── landing/   eventos brutos (já existe; você só lê)
├── bronze/
├── silver/
└── gold/
```

## 5. Requisitos

### 5.1 Obrigatórios de arquitetura

- **R1.** PySpark, lendo de `landing/` e gravando todas as camadas no mesmo bucket S3 (floci), em Parquet, **diretamente pelo Spark com `s3a://`**. O pipeline não usa `boto3` nem outro SDK de nuvem, não baixa arquivos para o disco local e roda no contêiner `app`.
- **R2.** **Um job por camada**, executáveis separadamente:
  - um job Bronze, um job Silver e **quatro jobs Gold, cada um num arquivo próprio** (funil, engajamento, receita e aquisição, ML);
  - cada job verifica que a camada de que depende existe e, se não existir, para com uma mensagem que diz o que rodar antes.
- **R3.** Um script que roda os seis jobs em sequência. Ele **não** gera dados.
- **R4.** Bronze append-only, com schema explícito (sem inferência), particionada por data no formato `year=AAAA/month=MM/day=DD` (o dia de coleta, tirado do caminho da landing) e com colunas de proveniência: arquivo de origem e instante da ingestão.

### 5.2 Silver

- **R5.** Deduplicação por `event_id`.
- **R6.** Extração dos `params` para colunas tipadas.
- **R7.** Validação com **motivo de rejeição**. Os rejeitados são gravados numa tabela separada, um motivo por linha. As regras mínimas estão abaixo; você pode acrescentar outras, justificando no relatório.

  | Motivo | Condição |
  | --- | --- |
  | evento fora do catálogo | `event_name` fora dos 20 eventos do contrato |
  | timestamp ausente ou futuro | `event_ts` nulo ou posterior ao instante de ingestão |
  | transação sem id | `purchase` ou `refund` sem `transaction_id` |
  | valor monetário inválido | `value` negativo, ou `currency` diferente de `BRL` em evento que tem `value` |
  | marco fora do padrão | `percent_scrolled` ∉ {25, 50, 75, 90} ou `video_percent` ∉ {0, 10, 25, 50, 75, 100} |
  | reembolso sem compra | `refund` cujo `transaction_id` não existe em nenhum `purchase` válido |

- **R8.** Sessionização própria: uma sessão nova começa quando o mesmo `user_pseudo_id` fica mais de 30 minutos sem evento. **`session_hint` não pode ser usado.**
- **R9.** Identity stitching: todo evento de um `user_pseudo_id` que em algum momento tenha `user_id` recebe esse `user_id`, inclusive os eventos anteriores ao login.
- **R10.** Uma tabela de itens com `items[]` explodido, uma linha por item por evento.

### 5.3 Gold: as 13 tabelas obrigatórias

Nomes de colunas são livres; o **conteúdo** é obrigatório, porque é o que a correção compara.

| Job | Tabela | Conteúdo mínimo |
| --- | --- | --- |
| Funil | `funil_compra` | sessões que atingiram cada etapa de `view_item_list → select_item → add_to_cart → view_cart → begin_checkout → add_payment_info → purchase`, conversão sobre a etapa anterior e sobre o topo; total e por plano × canal. `view_item` (abrir o detalhe do plano) é opcional na jornada e **não** entra na sequência: aparece como taxa à parte (sessões com `view_item` ÷ sessões com `select_item`). |
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

**Recomendação:** como compradores são raros, registre também precisão e recall em `metricas_modelo`, além de AUC e F1. Isso ajuda a interpretar o modelo.

### 5.4 Definições que valem para a correção

Para que a sua tabela possa ser comparada com o esperado, use exatamente estas definições.

1. **Caminhos das camadas.** `bronze/eventos/`, `silver/eventos/`, `silver/itens/`, `silver/eventos_rejeitados/` e `gold/<nome da tabela>/`, no bucket `datalake-tracking`, em Parquet. `bronze/eventos/`, `silver/eventos/` e `silver/itens/` são particionadas por dia no formato `year=AAAA/month=MM/day=DD` (mês e dia com dois dígitos), o mesmo da landing.
2. **Receita.**
   - Bruta: soma de `value` dos `purchase` válidos (sem frete, já com o desconto do Pix, incluindo o order bump).
   - Reembolsada: soma de `value` dos `refund` válidos, atribuída à compra pelo `transaction_id`.
   - Líquida: bruta − reembolsada.
   - Ticket médio: bruta ÷ pedidos.
3. **Canal e plano.**
   - Canal de uma sessão: `"<source> / <medium>"` do primeiro evento da sessão.
   - Plano de uma sessão: `item_id` do primeiro `select_item` da sessão (`(nenhum)` se não houver).
   - Plano de uma compra: o item de categoria `plano` no `items` do `purchase`.
   - Canal de uma compra: o canal da sessão em que ela aconteceu.
4. **Funil.** Uma sessão conta numa etapa se teve aquele evento em qualquer momento, sem exigir as etapas anteriores. Conversão sobre a etapa anterior = sessões da etapa ÷ sessões da etapa anterior.
5. **Lead → compra.** A unidade é a pessoa (`user_id` resolvido; se não houver, `user_pseudo_id`). Vale o primeiro `generate_lead` da pessoa e a primeira compra estritamente depois dele. Dias = diferença em segundos ÷ 86400 (fracionário); use a mediana desses dias.
6. **Propensão.** Uma linha por sessão (todas as sessões da Silver). O rótulo é verdadeiro quando a sessão contém um `purchase`.
7. **Reembolso.** Taxa de reembolso = pedidos com reembolso ÷ pedidos (por plano e canal da compra).
8. **Mediana de dias.** Use `percentile_approx(dias, 0.5)` do Spark.
9. **Canal de um lead.** É o canal da sessão do primeiro `generate_lead` da pessoa. A tabela `lead_para_compra` traz o total e uma linha por canal do lead.
10. **Datas.** Datas (data da compra etc.) em UTC.
11. **Métricas do modelo.** F1, precisão e recall referem-se à classe positiva (sessão com compra).

## 6. Como rodar

Todo comando roda no contêiner `app`; a única coisa instalada na máquina é o Docker. Detalhes do ambiente em [`SETUP.md`](../SETUP.md).

```bash
# 1. subir o S3 emulado (floci)
docker compose up -d floci

# 2. popular a landing com os eventos
docker compose run --rm app python gerador/gerar_eventos.py

# 3. conferir que o Spark lê e grava no S3 por s3a://
docker compose run --rm app python infra/verificar_ambiente.py

# 4. rodar um job seu
docker compose run --rm app python pipeline/<seu_job>.py
```

O código do pipeline vai em `pipeline/`, e o [`pipeline/README.md`](../pipeline/README.md) explica como acessar o S3 pelo Spark.

## 7. Avaliação

Rubrica de 100 pontos:

| Critério | Pontos | Como se verifica |
| --- | --- | --- |
| Arquitetura (R1–R4): jobs separados, verificação de camada, Bronze append-only com proveniência | 15 | leitura do código + rodar o script de sequência do zero |
| Silver (R5–R10) | 25 | contagens de duplicatas, aprovados e rejeitados por motivo dentro da tolerância esperada; sessões calculadas sem `session_hint`; stitching presente |
| Gold (seção 5.3) | 30 | as 13 tabelas existem e batem com os valores esperados dentro da tolerância; modelo sem vazamento de informação |
| Relatório: respostas às quatro perguntas, sustentadas por números da Gold | 20 | cada resposta cita tabela e número; recomendação coerente com os dados |
| Qualidade do dado: duplicatas, rejeições e armadilhas identificadas | 10 | identifica ao menos três armadilhas escondidas no dado e explica como as tratou |

O formato (individual ou em dupla) e o prazo ficam a combinar com a turma.
