# Explorador do datalake — design

**Data:** 2026-10-10
**Status:** design aprovado em conversa; aguardando revisão deste documento antes do plano

## 1. Objetivo

Um app web para **ver os arquivos e as partições** do bucket S3 (floci) e **fazer queries SQL** sobre ele.

- **Aluno:** confere o que o próprio pipeline gravou (da landing à Gold) e tira números para o relatório.
- **Professor:** inspeciona o bucket na correção.

O app vai no pacote do aluno.

### Critérios de sucesso

Com o pipeline já rodado, abrindo `http://localhost:8501`, o usuário consegue:

1. navegar de `landing/` até `gold/` e ver nome, tamanho e data de cada arquivo, com prévia das primeiras linhas;
2. ver, para cada dataset, uma linha por partição `year/month/day`, com nº de arquivos e bytes (e nº de linhas sob demanda);
3. escrever `SELECT ... FROM silver_eventos` (ou qualquer outra view) e ver o resultado em segundos, com download em CSV.

## 2. Restrições

- **Só leitura.** O app nunca grava nem apaga nada no bucket. O SQL aceito é só de consulta.
- **A regra "só Spark" vale para o pipeline, não para o explorador.** O explorador usa:
  - DuckDB para queries e prévias;
  - `boto3` para listar objetos com tamanho e data.
- **Execução em contêiner,** como o resto do projeto: novo serviço `explorador` no `docker compose`, mesma imagem `app`, porta 8501.
- **Textos em português do Brasil, para iniciantes.**
- **As partições aparecem como texto:** `month` é `"09"`, não `9`, igual ao pipeline.

## 3. Estrutura

```
explorador/
├── __init__.py
├── s3.py          lista prefixos e objetos do bucket (boto3)
├── particoes.py   funções puras: chaves → partições year/month/day e datasets
├── consultas.py   conexão DuckDB, views por dataset, validação de SQL, prévias e limite de linhas
├── app.py         interface Streamlit (três abas); só chama as funções acima
└── tests/
    ├── __init__.py
    ├── test_particoes.py
    ├── test_validacao_sql.py
    └── test_consultas.py   views sobre arquivos locais (sem S3)
```

### Responsabilidades e interfaces

- **`s3.py`**
  - `listar(prefixo="") -> list[Objeto]`, em que `Objeto` tem `chave`, `tamanho` e `modificado`.
  - Bucket e endpoint vêm de `AWS_S3_BUCKET` e `AWS_ENDPOINT_URL`, com os mesmos padrões do resto do projeto.
  - Se não conseguir conectar, lança `S3Indisponivel` com a mensagem do usuário.
- **`particoes.py`** (puro, sem I/O)
  - `particao_da_chave(chave) -> dict | None`: tira `year`, `month` e `day` dos segmentos `k=v` da chave.
  - `datasets(chaves) -> list[Dataset]`. Um dataset é o prefixo comum antes do primeiro segmento `k=v`, ou a pasta do arquivo quando não há partição. Cada `Dataset` traz `prefixo` (ex.: `silver/eventos`), `formato` (`parquet` ou `jsonl`) e `nome_view` (ex.: `silver_eventos`).
  - `tabela_particoes(objetos, prefixo) -> list[LinhaParticao]`, com `year`, `month`, `day`, `arquivos` e `bytes`. As chaves sem partição entram como uma linha com `—`.
  - Ignora `_SUCCESS` e arquivos que começam com `_` ou `.`.
- **`consultas.py`**
  - `conectar(raiz, s3=True) -> duckdb.DuckDBPyConnection`. `raiz` é `s3://<bucket>` ou uma pasta local, o que permite os testes. Com `s3=True`, carrega `httpfs` e cria o `SECRET` do floci: endpoint sem `http://`, `URL_STYLE 'path'`, `USE_SSL false` e as credenciais de teste.
  - `criar_views(con, raiz, datasets) -> dict[str, str]` (view → origem).
    - Parquet: `read_parquet('<raiz>/<prefixo>/**/*.parquet', hive_partitioning=true, hive_types_autocast=false, union_by_name=true)`.
    - JSONL: `read_json_auto('<raiz>/<prefixo>/**/*.jsonl', hive_partitioning=true, hive_types_autocast=false, union_by_name=true)`.
  - `validar_sql(sql) -> None`.
    - Lança `SqlNaoPermitido` se houver mais de um comando, ou se a primeira palavra (ignorando comentários `--` e `/* */` e espaços) não for `SELECT`, `WITH`, `DESCRIBE`, `SHOW`, `SUMMARIZE`, `FROM`, `PRAGMA table_info` ou `EXPLAIN`.
    - Também lança se aparecer `COPY`, `ATTACH`, `INSTALL`, `LOAD`, `CREATE`, `DROP`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `EXPORT`, `IMPORT` ou `SET` fora de strings.
  - `executar(con, sql, limite=1000) -> Resultado`, com `dataframe` (até `limite` linhas), `cortado: bool` e `colunas`. Valida o SQL antes; o limite máximo é 50 000.
  - `previa(con, raiz, chave, linhas=100)`: lê um único arquivo, Parquet ou JSONL.
- **`app.py`**
  - **Topo:** bucket, endpoint e botão "Recarregar". A listagem fica em cache de 30 s (`st.cache_data`); a conexão e as views ficam em `st.cache_resource`, refeitas pelo botão.
  - **Aba Arquivos:**
    - um seletor de pasta, navegação por níveis, por exemplo `silver/` → `silver/eventos/` → `year=2026/` …;
    - uma tabela de arquivos com tamanho legível e data;
    - ao escolher um arquivo, a prévia de até 100 linhas.
  - **Aba Partições:**
    - um seletor de dataset e a tabela `year | month | day | arquivos | bytes`;
    - um botão "Contar linhas", que roda `SELECT year, month, day, count(*) … GROUP BY ALL`.
  - **Aba Consultas SQL:**
    - a lista de views com suas colunas (num expansor) e alguns exemplos prontos para clicar;
    - um editor de SQL, um seletor de limite (padrão 1 000) e o botão "Executar";
    - o resultado aparece em tabela, com aviso quando for cortado, e pode ser baixado em CSV.

## 4. Erros (sempre mensagem em português, sem stack trace)

| Situação | O que o app mostra |
| --- | --- |
| floci desligado | "Não consegui falar com o S3 em `<endpoint>`. Rode `docker compose up -d floci`." |
| bucket vazio ou camada inexistente | "Ainda não há nada em `<prefixo>`." com sugestão do que rodar (gerador ou job da camada); as abas continuam utilizáveis |
| SQL que não é de leitura, ou vários comandos | "O explorador só faz consultas de leitura (SELECT, WITH, DESCRIBE…)." Nada é executado |
| erro de SQL | a mensagem do DuckDB, mais a dica "veja as views e colunas disponíveis" |
| arquivo grande na prévia | só as primeiras 100 linhas |
| resultado acima do limite | as primeiras N linhas e o aviso "resultado cortado em N linhas" |

## 5. Infraestrutura

- **`requirements.txt`:** ganha `duckdb` e `streamlit`, ambos com versão fixa.
- **`Dockerfile`:** depois do `pip install`, roda `python -c "import duckdb; duckdb.sql('INSTALL httpfs')"`. Assim a extensão já está na imagem e o app funciona offline.
- **`docker-compose.yml`:** serviço `explorador`.
  - Mesma `image: datalake-tracking-app`, mesmo volume e mesmo ambiente do `app`.
  - `command: streamlit run explorador/app.py --server.port 8501 --server.address 0.0.0.0 --server.headless true`.
  - `ports: ["8501:8501"]` e `depends_on: [floci]`.
- **`pyproject.toml`:** `testpaths` passa a incluir `explorador/tests`, para o `python -m pytest` rodar também os testes do explorador.

## 6. Testes

- **`test_particoes.py`:**
  - extração de `year/month/day`, com `"09"` mantido;
  - chave sem partição;
  - partição parcial;
  - `_SUCCESS` ignorado;
  - agrupamento em datasets (landing JSONL e Silver Parquet);
  - somas de arquivos e bytes.
- **`test_validacao_sql.py`:**
  - aceita `SELECT`, `WITH`, `DESCRIBE`, `SHOW`, `SUMMARIZE`, `FROM` e comentários antes;
  - recusa `COPY … TO`, `CREATE`, `ATTACH`, `INSTALL`, `SET`, dois comandos, e `SELECT 1; DROP …`;
  - não recusa palavras proibidas dentro de strings, por exemplo `WHERE event_name = 'drop'`.
- **`test_consultas.py`:**
  - grava Parquet particionado e JSONL particionado numa pasta temporária;
  - `conectar(raiz_local, s3=False)` + `criar_views` geram os nomes de view esperados;
  - `month` vem como texto `"09"`;
  - `executar` respeita o limite e marca `cortado`;
  - `previa` lê um arquivo;
  - SQL proibido não executa.
- **Conferência real:**
  - sobe o `explorador` com o pipeline rodado;
  - no navegador embutido, confere as três abas;
  - `SELECT count(*) FROM silver_eventos` = 61813 e `SELECT count(*) FROM landing_eventos` = 64372.

## 7. Documentação

- **`README.md`:** linha na tabela "O que há aqui" para `explorador/`.
- **`SETUP.md`:** passo novo "Explore o bucket". Explica `docker compose up -d explorador`, o endereço `http://localhost:8501` e o que cada aba faz, para iniciantes.
- **`explorador/README.md`:** curto. Traz as views disponíveis, exemplos de SQL e a regra de só leitura.

## 8. Fora de escopo

- Gravar, apagar ou mover arquivos.
- Autenticação e multiusuário: é uma ferramenta local.
- Gráficos: o resultado é uma tabela, que pode ser baixada em CSV.
- Usar o explorador no lugar do pipeline. Ele não substitui nenhum requisito R1–R10.
