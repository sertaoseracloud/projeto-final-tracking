# Mapa de calor — design

**Data:** 2026-10-10
**Status:** design aprovado em conversa; aguardando revisão deste documento antes do plano

## 1. Objetivo

Uma quinta Gold que alimenta um **mapa de calor** da página de exemplo (`site/index.html`). O mapa precisa poder ser **adicionado e ativado no site**.

- **Cliques por elemento:** cada elemento clicável da tela atual ganha uma camada colorida, de frio a quente.
- **Faixa de rolagem:** na landing, uma faixa lateral mostra que fração das sessões chegou a 25/50/75/90% da página.

Vale para o aluno (requisito novo no enunciado) e para a solução de referência em `professor/`.

### Critérios de sucesso

1. A sequência abaixo acende o overlay no site, com cores e etiquetas nos elementos e a faixa de rolagem na landing:
   - `docker compose run --rm app python gerador/gerar_eventos.py`;
   - rodar o pipeline completo;
   - abrir `site/index.html` e clicar em **Mapa de calor**.
2. Todos os números atuais do `professor/GABARITO.md` continuam iguais. O gabarito ganha só uma seção nova.
3. Ligar o overlay não dispara nenhum evento de tracking.

## 2. Restrições

- O pipeline continua **só Spark** (s3a). O job de exportação lê as Golds pelo Spark e grava um arquivo local com Python puro. O resultado tem dezenas de linhas, então o job pode trazer tudo para o Python (`collect()`).
- Tudo roda em contêiner; o arquivo gerado vai para `/projeto/site`, a pasta montada.
- O site continua um HTML estático aberto do disco (`file://`), sem servidor.
- O catálogo continua com **20 eventos**. Não há evento novo.
- Os textos para o aluno ficam em pt-BR, para iniciantes.

## 3. Dado de origem: `params.element_id`

### 3.1 Contrato

`element_id` é um parâmetro **opcional**, presente em todo evento disparado por um clique. Ele identifica o elemento clicado e, no HTML, é o atributo `data-heat` daquele elemento. O campo não entra nas regras de rejeição: um evento sem ele continua válido.

| Evento | `element_id` |
| --- | --- |
| `click` (saída, `outbound=true`) | `social-instagram`, `social-youtube`, `social-whatsapp`, `social-linkedin` (pelo domínio do link) |
| `click` (interno, `outbound=false`) | `cta-` + `link_id` com `_` trocado por `-` (ex.: `cta-hero-ver-planos`, `cta-hero-assistir`, `cta-logo`) |
| `video_start` | `btn-play` |
| `file_download` | `dl-ementa` |
| `select_item` | `plano-basico`, `plano-pro`, `plano-premium` (o cartão do plano) |
| `add_to_cart` / `remove_from_cart` de plano | `modal-comprar` (no site, quando a compra vem do botão "Quero este plano" do próprio cartão, vale o cartão: `plano-…`; o gerador usa sempre `modal-comprar`) |
| `add_to_cart` / `remove_from_cart` do order bump | `bump` |
| `generate_lead` | `form-lead` |
| `site_search` | `faq-busca` |
| `begin_checkout` | `btn-continuar` |
| `add_shipping_info` | `frete-pac`, `frete-sedex` |
| `add_payment_info` | `pagto-pix`, `pagto-cartao-avista`, `pagto-cartao-parcelado` |
| `purchase` | `btn-comprar` |
| `refund` | `btn-refund` |
| `login` / `sign_up` | `btn-login` / `btn-signup` |

Os demais eventos não trazem `element_id`: `page_view`, `scroll`, `video_progress`, `video_complete`, `view_item_list`, `view_item` e `view_cart`.

### 3.2 Site

- Cada elemento da tabela ganha `data-heat="<element_id>"`. Os planos e as opções de frete e de pagamento são gerados ou repetidos; cada item recebe o seu.
- As chamadas de `track()` que nascem de um clique passam `element_id` nos `params`, conforme a tabela.

### 3.3 Gerador

- Um passo final **determinístico** (sem sorteio), `gerador/elementos.py::marcar_elementos(eventos)`, põe o `element_id` em cada evento a partir de `event_name` e `params`, conforme a tabela.
- Ele roda depois da sujeira e antes dos lotes.
- Como não consome o `rng`, os eventos são os mesmos de antes, com um campo a mais.
- Os eventos inseridos pela sujeira também passam pelo passo:
  - `refund_orfao` ganha `btn-refund`;
  - `add_to_wishlist` (fora do catálogo) fica sem `element_id`.
- `gerador/elementos.py` também exporta `ELEMENTOS`: o conjunto de todos os ids possíveis, usado nos testes.

### 3.4 Silver

`element_id` entra na lista de params de texto extraídos para coluna (R6).

## 4. Golds — `professor/solucao/07_gold_mapa_calor.py`

O job lê a Silver (`ler_eventos(spark, "silver")`) e grava em `gold/`, em Parquet, com overwrite. Ele exige a camada Silver antes de rodar (R2).

### 4.1 `gold/mapa_calor_cliques`

Uma linha por `pagina` e `element_id`, considerando só eventos com `element_id` não nulo.

| coluna | definição |
| --- | --- |
| `pagina` | caminho de `page_location`, sem domínio, `#` nem `?`: `/`, `/checkout`, `/obrigado`, `/membros` |
| `element_id` | texto |
| `cliques` | número de eventos |
| `sessoes` | `session_id` distintos com clique nesse elemento |
| `sessoes_na_pagina` | `session_id` distintos com `page_view` nessa `pagina` |
| `taxa_clique` | `sessoes / sessoes_na_pagina`, arredondado a 4 casas; nulo se o denominador for 0 |
| `intensidade` | `cliques / max(cliques)` da mesma `pagina`, arredondado a 4 casas (de 0 a 1) |

### 4.2 `gold/mapa_calor_rolagem`

Uma linha por marco da landing (`pagina = "/"`): 0, 25, 50, 75 e 90.

| coluna | definição |
| --- | --- |
| `pagina` | `/` |
| `marco` | inteiro |
| `sessoes_que_chegaram` | entre as sessões com `page_view` na landing: marco 0 = todas; demais = as que têm `scroll` na landing com `percent_scrolled >= marco` |
| `pct_sessoes` | `sessoes_que_chegaram / sessões do marco 0`, arredondado a 4 casas |

A série não cresce: um marco nunca tem mais sessões que o anterior, porque cada marco conta as sessões que chegaram nele ou além.

### 4.3 Funções testáveis

- `pagina_do_evento(col)`;
- `mapa_cliques(eventos) -> DataFrame`;
- `mapa_rolagem(eventos) -> DataFrame`.

## 5. Exportação — `professor/solucao/08_exportar_mapa_calor.py`

- O job lê `gold/mapa_calor_cliques` e `gold/mapa_calor_rolagem` pelo Spark e exige que elas existam. Ele traz as linhas para o Python e grava `site/mapa_calor.js`, com o caminho tirado de `Path(__file__)`: a raiz do repositório mais `site/`.
- Formato do arquivo (é a fronteira com o site e vai documentado no enunciado):

  ```js
  window.MAPA_CALOR = {"gerado_em": "2026-10-10T21:00:00Z", "cliques": [{"pagina": "/", "element_id": "plano-pro", "cliques": 1719, "sessoes": 1500, "taxa_clique": 0.1789, "intensidade": 1.0}, ...], "rolagem": [{"pagina": "/", "marco": 0, "sessoes_que_chegaram": 8382, "pct_sessoes": 1.0}, ...]};
  ```

  (os números são ilustrativos)
- A gravação é atômica: o job grava um arquivo temporário e depois renomeia.
- Funções testáveis:
  - `montar_js(cliques: list[dict], rolagem: list[dict], gerado_em: str) -> str`;
  - `gravar(texto, destino: Path)`.
- O `executar_pipeline.py` passa a rodar o 07 e o 08 depois do 06.
- O `site/mapa_calor.js` entra no `.gitignore`: é um resultado gerado e não vai no pacote do aluno.

## 6. Overlay no site

- No `<head>`, `<script src="mapa_calor.js"></script>` vem antes do script principal. Se o arquivo não existir, o 404 em `file://` não quebra nada e `window.MAPA_CALOR` fica indefinido.
- No painel, um botão **"Mapa de calor"** liga e desliga o overlay.
  - Sem `window.MAPA_CALOR`, ele mostra um aviso: "Ainda não há mapa. Rode o pipeline (jobs 07 e 08) para gerar `site/mapa_calor.js` e recarregue a página."
- **Cliques:**
  - para cada `[data-heat]` visível na tela atual, o site procura a linha (`pagina` da tela, `element_id`);
  - se achar, desenha por cima do elemento uma camada absoluta com cor interpolada de azul (`intensidade` 0) a vermelho (1), opacidade 0,45, e uma etiqueta "N cliques · X% das sessões";
  - elemento sem linha fica sem camada.
- **Rolagem** (só na landing):
  - uma faixa fixa na lateral direita, com marcas em 25/50/75/90% da altura da página e o `pct_sessoes` de cada marco;
  - um gradiente escurece a página conforme o percentual cai.
- As camadas usam `pointer-events: none` e ficam fora do fluxo do tracking, então ligar o overlay não gera evento. Elas são redesenhadas ao trocar de tela (rota) e ao redimensionar.
- A `pagina` da tela vem da mesma lógica de `urlAtual()`: `/`, `/checkout`, `/obrigado` ou `/membros`.

## 7. Documentação para o aluno

- **`docs/CONTRATO_EVENTO.md`:** `element_id` na tabela de `params` e a tabela da seção 3.1.
- **`docs/ENUNCIADO.md`:**
  - requisito novo para a Gold de mapa de calor (as duas tabelas, com as definições das seções 4.1 e 4.2) e para a exportação (o formato da seção 5);
  - "quatro Golds" passa a ser "cinco", e R2/R3 passam a ter oito jobs;
  - o glossário ganha "mapa de calor".
- **`pipeline/README.md`:** um parágrafo sobre `collect()` (trazer um resultado pequeno para o Python) e sobre gravar em `/projeto/site`.
- **`site/` e `README.md`:** como ativar o mapa.
- **`professor/GABARITO.md`:** seção nova com as duas tabelas e as tolerâncias: contagens exatas, taxas com ±0,001.

## 8. Testes

- **Gerador:**
  - todo evento disparado por clique da seção 3.1 recebe um `element_id` que está em `ELEMENTOS`, e os outros eventos não recebem;
  - `marcar_elementos` não muda nenhum outro campo;
  - a mesma seed gera os mesmos `event_id` e as mesmas contagens de antes.
- **Site:** um teste (Python, lendo o HTML) confere que todo valor de `ELEMENTOS` tem um `data-heat` no `site/index.html` e que todo `data-heat` está em `ELEMENTOS`. Os planos são gerados por JS, então o teste aceita o padrão `plano-` mais o id gerado no script.
- **Solução (Spark local):**
  - `pagina_do_evento`;
  - `mapa_cliques`: contagens, `taxa_clique` e `intensidade`, que vale 1 no máximo da página;
  - `mapa_rolagem`: o marco 0 vale 1,0 e a série não cresce;
  - `montar_js`: começa com `window.MAPA_CALOR = `, e o JSON dentro dele é válido e tem as chaves esperadas.
- **Conferência real:**
  - pipeline completo, com os números do gabarito iguais;
  - no navegador embutido: ligar o overlay na landing e no checkout, ver as camadas e a faixa, e conferir que o contador de eventos do painel não mudou.

## 9. Fora de escopo

- Mapa por coordenada (x, y).
- Servidor para o site, ou leitura direta do S3 pelo navegador.
- Filtros no overlay (por canal, por dispositivo ou por data).
