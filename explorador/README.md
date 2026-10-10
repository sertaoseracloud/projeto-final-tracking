# Explorador do datalake

Um site que roda no seu computador para **olhar o bucket** sem escrever código: ver os arquivos que o gerador e o seu pipeline gravaram, ver as partições `year=/month=/day=` de cada pasta e fazer **consultas SQL** sobre os dados.

## Como abrir

Antes, construa a imagem uma vez (é o Passo 2 do [SETUP.md](../SETUP.md)): `docker compose build app`. O explorador usa essa mesma imagem; sem ela, ele não sobe.

```bash
docker compose up -d floci explorador
```

Abra **http://localhost:8501** no navegador. Para desligar: `docker compose stop explorador`.

## As três abas

- **Arquivos** — navegue pelas pastas do bucket (`landing/`, `bronze/`, `silver/`, `gold/`) e veja nome, tamanho e data de cada arquivo. Escolha um `.parquet` ou `.jsonl` para ver as primeiras 100 linhas.
- **Partições** — escolha um dataset e veja uma linha por partição (`event_name`, `year`, `month`, `day`) com quantos arquivos e quantos bytes ela tem. Quando o dataset não tem uma dessas partições, a coluna mostra `—`. O botão "Contar linhas por partição" conta as linhas de cada uma.
- **Consultas SQL** — escreva SQL e veja o resultado numa tabela. O campo **Limite de linhas** diz quantas linhas trazer: o padrão é 1 000 e o máximo é 50 000. Se a consulta tiver mais linhas que o limite, o resultado é cortado e aparece um aviso. O último resultado continua na tela, junto com a SQL que o gerou, e você pode baixá-lo em CSV. Se a consulta tiver um erro, o site explica em português o que costuma causá-lo (nome de view ou coluna errado, erro de escrita…) e mostra embaixo a mensagem original do DuckDB, que vem em inglês.

## Views: os nomes que você usa no SQL

Cada pasta com dados vira uma **view**, uma tabela virtual que lê os arquivos na hora. O nome é o caminho da pasta, antes das partições `k=v`, com `_` no lugar de `/`:

| Pasta no bucket | View |
| --- | --- |
| `landing/event_name=…/` | `landing` |
| `bronze/event_name=…/` | `bronze` |
| `silver/event_name=…/` | `silver` |
| `silver/itens/` | `silver_itens` |
| `gold/funil_compra/` | `gold_funil_compra` |

A view `silver` lê só as pastas `event_name=…`: as vizinhas `silver/itens/` e `silver/eventos_rejeitados/` têm views próprias.

Se uma pasta tiver arquivos `.parquet` e `.jsonl` misturados, ela vira duas views, com o formato no fim do nome (ex.: `landing_jsonl` e `landing_parquet`).

As views das pastas particionadas têm as colunas de partição (`event_name`, `year`, `month`, `day`), como texto (`month` é `'09'`). A lista completa de views e colunas aparece em "Views disponíveis", na aba de SQL.

Exemplos:

```sql
SELECT event_name, count(*) AS eventos FROM landing GROUP BY event_name ORDER BY eventos DESC;
SELECT year, month, day, count(*) AS linhas FROM silver WHERE event_name = 'purchase' GROUP BY ALL ORDER BY ALL;
DESCRIBE silver;
```

## Somente leitura

O explorador só **lê**. Ele tem duas proteções:

1. Aceita consultas que começam com `SELECT`, `WITH`, `DESCRIBE`, `SHOW`, `SUMMARIZE`, `FROM`, `EXPLAIN` ou `PRAGMA table_info`, uma por vez. Comandos que mudam dados (`COPY`, `CREATE`, `DROP`, `INSERT`…) são recusados. `EXPLAIN ANALYZE` também é recusado, porque ele executa o comando de verdade; use só `EXPLAIN`.
2. A conexão com o DuckDB é **trancada** no bucket: ela não lê arquivos do computador nem muda configurações.

Assim, você não estraga o seu datalake por engano. Se algo inesperado acontecer, o site mostra uma mensagem amigável e, embaixo dela, uma linha curta de "Detalhe técnico" para ajudar a achar a causa.

As consultas usam o **DuckDB**, um motor SQL rápido para arquivos. Ele serve só para explorar. O seu pipeline continua sendo construído com o Spark, como pede o enunciado.

Depois de rodar um job novo, clique em **Recarregar** para o explorador encontrar as pastas novas e as colunas novas das views. (Sem clicar, a lista de arquivos se atualiza sozinha a cada 30 segundos, mas a lista de colunas não.)
