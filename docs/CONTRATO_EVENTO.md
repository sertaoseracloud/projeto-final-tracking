# Contrato do evento

Este documento descreve o formato dos eventos de tracking e o layout da landing no S3. É a fronteira entre o que você recebe pronto e o que você constrói.

> **Aviso:** o coletor pode reenviar um lote: o mesmo `event_id` pode aparecer mais de uma vez.

## Layout da landing no S3

```text
s3://datalake-tracking/landing/eventos/year=AAAA/month=MM/day=DD/<origem>-...jsonl
```

- Cada dia (partição `year=…/month=…/day=…`) tem de 3 a 6 lotes, como um coletor que descarrega em intervalos. O dia seguinte ao último pode ter 1 ou 2 lotes (sessões que atravessam a meia-noite).
- `<origem>` é `coletor` (eventos do gerador) ou `site` (eventos que você exportou da página de exemplo e subiu com `gerador/importar_site.py`).
- Cada arquivo é JSON Lines: um evento por linha.

## O evento

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

## `params` por evento

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

## A regra do `value`

`value` é sempre a receita dos itens **sem frete**. Em `purchase` e `refund` ele já vem com o desconto do Pix; nas etapas anteriores (`view_cart` até `add_payment_info`) é o valor de tabela, porque o desconto só existe depois que o pagamento é escolhido.

## Os 20 eventos

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
