# Explorador do datalake — plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Um app Streamlit (`explorador/`) que lista os arquivos do bucket do floci, mostra as partições `year/month/day` de cada dataset e roda consultas SQL somente leitura com DuckDB, como mais um serviço do `docker compose`.

**Architecture:** As funções puras sobre chaves ficam em `particoes.py`. A listagem do bucket (boto3) fica em `s3.py`, e o DuckDB (views, validação de SQL, prévia, limite) em `consultas.py`. O `app.py` só monta a interface e chama esses módulos. Todos os testes rodam sem S3: os de `consultas.py` usam arquivos locais, e os do app usam `streamlit.testing.v1.AppTest` com o `s3.listar` substituído.

**Tech Stack:** Python 3.11, DuckDB 1.5.6 (extensão `httpfs`), Streamlit 1.65.0, boto3, pandas (dependência do Streamlit), pytest, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-10-explorador-s3-design.md`

## Global Constraints

- Somente leitura. O app nunca grava nem apaga nada no bucket. O SQL aceito começa com `SELECT`, `WITH`, `DESCRIBE`, `SHOW`, `SUMMARIZE`, `FROM`, `EXPLAIN` ou `PRAGMA table_info`.
  - Recusado: mais de um comando, ou qualquer uma das palavras `COPY`, `ATTACH`, `DETACH`, `INSTALL`, `LOAD`, `CREATE`, `DROP`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `EXPORT`, `IMPORT`, `SET`, `CALL`, `CHECKPOINT`, `VACUUM` ou `USE` fora de strings e comentários.
- A regra "só Spark" vale para o pipeline, não para o explorador, que usa DuckDB e `boto3`.
- Tudo roda em contêiner, com a mesma imagem `datalake-tracking-app`. O serviço `explorador` fica na porta 8501.
- Bucket e endpoint vêm de `AWS_S3_BUCKET` (padrão `datalake-tracking`) e `AWS_ENDPOINT_URL` (padrão `http://floci:4566`).
- Partições `year`, `month` e `day` como texto: `month` é `"09"`. Use `hive_types_autocast=false`.
- Limites: o resultado tem 1000 linhas por padrão e no máximo 50000; a prévia mostra 100 linhas; a listagem fica em cache por 30 s.
- Os textos que o usuário vê ficam em português do Brasil, para iniciantes, sem stack trace.
- As views têm o prefixo do dataset com `/` trocado por `_`: `landing_eventos`, `silver_eventos`, `gold_funil_compra`.
- As mensagens de erro vêm do spec, seção 4.
  - floci desligado: "Não consegui falar com o S3 em `<endpoint>`. Rode `docker compose up -d floci`."
  - SQL proibido: "O explorador só faz consultas de leitura (SELECT, WITH, DESCRIBE…)."
- Nunca rode Python no host: tudo em `docker compose run --rm app ...` a partir de `C:\Repo\projeto-final-tracking`. No Git Bash, use `MSYS_NO_PATHCONV=1`.

## Review Focus

1. **floci desligado** ao abrir o app: aparece a mensagem amigável e nada quebra. Teste na Task 6.
2. **Bucket vazio** (antes do gerador): aparece a orientação de rodar o gerador. Teste na Task 6.
3. **SQL legítimo com palavra "proibida" dentro de uma string ou comentário** (`WHERE event_name = 'drop'`): é aceito. Teste na Task 3.
4. **Pastas internas do Spark** (`_temporary/`, `_verificacao/`) e arquivos `_SUCCESS`: não viram datasets nem views. Teste na Task 2.
5. **Resultado gigante** (`SELECT * FROM landing_eventos`): é cortado no limite, com aviso, sem travar. Teste na Task 4.

---

## Mapa de arquivos

| Arquivo | Responsabilidade |
| --- | --- |
| `requirements.txt` | + `duckdb==1.5.6`, `streamlit==1.65.0` |
| `Dockerfile` | instala a extensão `httpfs` do DuckDB no build |
| `docker-compose.yml` | serviço `explorador` (porta 8501) |
| `pyproject.toml` | `testpaths` inclui `explorador/tests` |
| `explorador/__init__.py`, `explorador/tests/__init__.py` | pacotes vazios |
| `explorador/particoes.py` | `Objeto`, `Dataset`, `LinhaParticao`, `formato_do_arquivo`, `particao_da_chave`, `datasets`, `tabela_particoes`, `navegar`, `tamanho_legivel` |
| `explorador/consultas.py` | `SqlNaoPermitido`, `validar_sql`, `conectar`, `criar_views`, `Resultado`, `executar`, `previa` |
| `explorador/s3.py` | `ENDPOINT`, `BUCKET`, credenciais, `raiz()`, `S3Indisponivel`, `listar` |
| `explorador/app.py` | interface Streamlit |
| `explorador/README.md`, `README.md`, `SETUP.md` | documentação |

---

### Task 1: Dependências, imagem e serviço

**Files:**
- Modify: `requirements.txt`, `Dockerfile`, `docker-compose.yml`, `pyproject.toml`
- Create: `explorador/__init__.py`, `explorador/tests/__init__.py` (vazios)

**Interfaces:**
- Produces: a imagem `datalake-tracking-app`, com `duckdb`, `streamlit` e `pandas` importáveis e a extensão `httpfs` carregável **sem internet**; o serviço `explorador`.

- [ ] **Step 1: `requirements.txt` passa a ser:**

```text
pyspark==3.5.3
numpy==1.26.4
boto3==1.43.111
pytest==9.1.1
duckdb==1.5.6
streamlit==1.65.0
```

- [ ] **Step 2: No `Dockerfile`, logo depois da linha `RUN pip install --no-cache-dir -r /tmp/requirements.txt`, acrescentar:**

```dockerfile
# Extensão S3 do DuckDB baixada no build: o explorador funciona sem internet.
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs')"
```

- [ ] **Step 3: No `docker-compose.yml`, acrescentar o serviço depois de `app`:**

```yaml
  explorador:
    image: datalake-tracking-app
    working_dir: /projeto
    volumes:
      - .:/projeto
    environment:
      AWS_ENDPOINT_URL: http://floci:4566
      AWS_S3_BUCKET: datalake-tracking
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_DEFAULT_REGION: us-east-1
    command: streamlit run explorador/app.py --server.port 8501 --server.address 0.0.0.0 --server.headless true --browser.gatherUsageStats false
    ports:
      - "8501:8501"
    depends_on:
      - floci
```

- [ ] **Step 4: No `pyproject.toml`, trocar `testpaths = ["tests"]` por:**

```toml
testpaths = ["tests", "explorador/tests"]
```

- [ ] **Step 5: Criar `explorador/__init__.py` e `explorador/tests/__init__.py` vazios.**

- [ ] **Step 6: Reconstruir e conferir**

```bash
docker compose build app
docker compose run --rm --no-deps --network none app python -c "import duckdb, streamlit, pandas; duckdb.sql('LOAD httpfs'); print('ok', duckdb.__version__, streamlit.__version__)"
docker compose run --rm app python -m pytest
```

Expected:
- O segundo comando imprime `ok 1.5.6 1.65.0`. Ele roda com `--network none`, o que prova que a extensão carrega sem internet.
- O `pytest` mostra `56 passed`; `explorador/tests` ainda está vazio.
- Se o pip apontar conflito de dependência (por exemplo pandas × numpy 1.26.4), relate a mensagem exata e pare. Não troque a versão do `numpy`, que o PySpark 3.5 exige.

- [ ] **Step 7: Commit**

```bash
git add requirements.txt Dockerfile docker-compose.yml pyproject.toml explorador/__init__.py explorador/tests/__init__.py
git commit -m "Explorador: dependencias (duckdb, streamlit), httpfs na imagem e servico na porta 8501"
```

---

### Task 2: `particoes.py` — chaves, datasets e partições

**Files:**
- Create: `explorador/particoes.py`
- Test: `explorador/tests/test_particoes.py`

**Interfaces:**
- Produces:
  - `Objeto(chave: str, tamanho: int, modificado: datetime | None = None)`, um dataclass congelado.
  - `Dataset(prefixo: str, formato: str, nome_view: str)`, congelado e hashable.
  - `LinhaParticao(year: str, month: str, day: str, arquivos: int, bytes: int)`.
  - `SEM_PARTICAO = "—"`.
  - `formato_do_arquivo(chave) -> str | None` (`"parquet"`, `"jsonl"` ou `None`).
  - `particao_da_chave(chave) -> dict[str, str] | None`.
  - `datasets(chaves: Iterable[str]) -> list[Dataset]`, ordenado por prefixo.
  - `tabela_particoes(objetos: Iterable[Objeto], prefixo: str) -> list[LinhaParticao]`, ordenado.
  - `navegar(objetos: Iterable[Objeto], pasta: str) -> tuple[list[str], list[Objeto]]`. `pasta` é `""` ou termina em `/`. Devolve as subpastas e os arquivos diretos, ordenados.
  - `tamanho_legivel(n: int) -> str`, por exemplo `"512 B"` ou `"1,5 KB"`.

- [ ] **Step 1: Escrever o teste que falha**

`explorador/tests/test_particoes.py`:

```python
from explorador.particoes import (SEM_PARTICAO, Dataset, LinhaParticao, Objeto, datasets,
                                  formato_do_arquivo, navegar, particao_da_chave,
                                  tabela_particoes, tamanho_legivel)

CHAVES = [
    "landing/eventos/year=2026/month=09/day=01/coletor-s42-001.jsonl",
    "landing/eventos/year=2026/month=09/day=02/coletor-s42-001.jsonl",
    "silver/eventos/year=2026/month=09/day=01/part-0.snappy.parquet",
    "silver/eventos/_SUCCESS",
    "silver/eventos_rejeitados/part-0.snappy.parquet",
    "gold/funil_compra/part-0.snappy.parquet",
    "gold/funil_compra/_SUCCESS",
    "_verificacao/part-0.parquet",
    "bronze/eventos/_temporary/0/part-0.parquet",
    "solto.parquet",
    "silver/eventos/leia-me.txt",
]


def test_formato_do_arquivo():
    assert formato_do_arquivo("a/b.snappy.parquet") == "parquet"
    assert formato_do_arquivo("a/b.jsonl") == "jsonl"
    assert formato_do_arquivo("a/_SUCCESS") is None
    assert formato_do_arquivo("a/b.txt") is None


def test_particao_da_chave_mantem_o_mes_com_dois_digitos():
    assert particao_da_chave(CHAVES[0]) == {"year": "2026", "month": "09", "day": "01"}
    assert particao_da_chave("gold/funil_compra/part-0.parquet") is None
    assert particao_da_chave("x/year=2026/part-0.parquet") == {"year": "2026"}


def test_datasets_ignora_pastas_internas_arquivos_soltos_e_nao_dados():
    assert datasets(CHAVES) == [
        Dataset("gold/funil_compra", "parquet", "gold_funil_compra"),
        Dataset("landing/eventos", "jsonl", "landing_eventos"),
        Dataset("silver/eventos", "parquet", "silver_eventos"),
        Dataset("silver/eventos_rejeitados", "parquet", "silver_eventos_rejeitados"),
    ]


def test_tabela_particoes_soma_arquivos_e_bytes_por_dia():
    objetos = [
        Objeto(CHAVES[0], 100), Objeto(CHAVES[1], 50),
        Objeto("landing/eventos/year=2026/month=09/day=01/coletor-s42-002.jsonl", 10),
        Objeto("landing/eventos_outros/year=2026/month=09/day=01/x.jsonl", 999),
    ]
    assert tabela_particoes(objetos, "landing/eventos") == [
        LinhaParticao("2026", "09", "01", 2, 110),
        LinhaParticao("2026", "09", "02", 1, 50),
    ]


def test_tabela_particoes_de_dataset_sem_particao():
    objetos = [Objeto("gold/funil_compra/part-0.parquet", 7), Objeto("gold/funil_compra/_SUCCESS", 0)]
    assert tabela_particoes(objetos, "gold/funil_compra/") == [
        LinhaParticao(SEM_PARTICAO, SEM_PARTICAO, SEM_PARTICAO, 1, 7)]


def test_navegar_devolve_subpastas_e_arquivos_diretos():
    objetos = [Objeto(c, 1) for c in CHAVES]
    pastas, arquivos = navegar(objetos, "")
    assert pastas == ["_verificacao", "bronze", "gold", "landing", "silver"]
    assert [o.chave for o in arquivos] == ["solto.parquet"]
    pastas, arquivos = navegar(objetos, "gold/funil_compra/")
    assert pastas == []
    assert [o.chave for o in arquivos] == ["gold/funil_compra/_SUCCESS", "gold/funil_compra/part-0.snappy.parquet"]


def test_tamanho_legivel():
    assert tamanho_legivel(0) == "0 B"
    assert tamanho_legivel(512) == "512 B"
    assert tamanho_legivel(1536) == "1,5 KB"
    assert tamanho_legivel(5 * 1024 * 1024) == "5,0 MB"
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `docker compose run --rm app python -m pytest explorador/tests/test_particoes.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'explorador.particoes'`

- [ ] **Step 3: Criar `explorador/particoes.py`**

```python
"""Funções puras sobre as chaves do bucket: datasets, partições e navegação.

Uma chave é o "caminho" de um arquivo no S3, por exemplo
landing/eventos/year=2026/month=09/day=01/coletor-s42-001.jsonl.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

CAMPOS = ("year", "month", "day")
SEM_PARTICAO = "—"
EXTENSOES = {".parquet": "parquet", ".jsonl": "jsonl"}


@dataclass(frozen=True)
class Objeto:
    chave: str
    tamanho: int
    modificado: datetime | None = None


@dataclass(frozen=True)
class Dataset:
    prefixo: str
    formato: str
    nome_view: str


@dataclass(frozen=True)
class LinhaParticao:
    year: str
    month: str
    day: str
    arquivos: int
    bytes: int


def _interno(segmento: str) -> bool:
    return segmento.startswith(("_", "."))


def formato_do_arquivo(chave: str) -> str | None:
    nome = chave.rsplit("/", 1)[-1]
    if _interno(nome):
        return None
    return next((f for ext, f in EXTENSOES.items() if nome.endswith(ext)), None)


def particao_da_chave(chave: str) -> dict[str, str] | None:
    valores = {}
    for segmento in chave.split("/")[:-1]:
        campo, igual, valor = segmento.partition("=")
        if igual and campo in CAMPOS:
            valores[campo] = valor
    return valores or None


def _prefixo_do_dataset(chave: str) -> str:
    base = []
    for segmento in chave.split("/")[:-1]:
        if "=" in segmento:
            break
        base.append(segmento)
    return "/".join(base)


def _nome_view(prefixo: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", prefixo).strip("_").lower()


def datasets(chaves: Iterable[str]) -> list[Dataset]:
    """Uma pasta com arquivos .parquet ou .jsonl vira um dataset (e uma view)."""
    encontrados: dict[str, str] = {}
    for chave in chaves:
        formato = formato_do_arquivo(chave)
        if formato is None or any(_interno(s) for s in chave.split("/")[:-1]):
            continue
        prefixo = _prefixo_do_dataset(chave)
        if prefixo:
            encontrados.setdefault(prefixo, formato)
    return [Dataset(p, f, _nome_view(p)) for p, f in sorted(encontrados.items())]


def tabela_particoes(objetos: Iterable[Objeto], prefixo: str) -> list[LinhaParticao]:
    base = prefixo.rstrip("/") + "/"
    grupos: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0])
    for o in objetos:
        if not o.chave.startswith(base) or formato_do_arquivo(o.chave) is None:
            continue
        resto = o.chave[len(base):]
        if any(_interno(s) for s in resto.split("/")[:-1]) or ("/" in resto and "=" not in resto.split("/")[0]):
            continue
        particao = particao_da_chave(o.chave) or {}
        chave = tuple(particao.get(c, SEM_PARTICAO) for c in CAMPOS)
        grupos[chave][0] += 1
        grupos[chave][1] += o.tamanho
    return [LinhaParticao(*k, arquivos=a, bytes=b) for k, (a, b) in sorted(grupos.items())]


def navegar(objetos: Iterable[Objeto], pasta: str) -> tuple[list[str], list[Objeto]]:
    subpastas: set[str] = set()
    arquivos: list[Objeto] = []
    for o in objetos:
        if not o.chave.startswith(pasta):
            continue
        resto = o.chave[len(pasta):]
        if "/" in resto:
            subpastas.add(resto.split("/", 1)[0])
        elif resto:
            arquivos.append(o)
    return sorted(subpastas), sorted(arquivos, key=lambda o: o.chave)


def tamanho_legivel(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    valor = float(n)
    for unidade in ("KB", "MB", "GB", "TB"):
        valor /= 1024
        if valor < 1024 or unidade == "TB":
            return f"{valor:.1f} {unidade}".replace(".", ",")
    return f"{n} B"
```

A condição `"/" in resto and "=" not in resto.split("/")[0]` impede que `landing/eventos_outros/...` entre na tabela de `landing/eventos`, e que uma subpasta comum seja tratada como partição.

- [ ] **Step 4: Rodar e ver passar**

Run: `docker compose run --rm app python -m pytest explorador/tests/test_particoes.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add explorador/particoes.py explorador/tests/test_particoes.py
git commit -m "Explorador: datasets, particoes year/month/day e navegacao a partir das chaves"
```

---

### Task 3: Validação de SQL somente leitura

**Files:**
- Create: `explorador/consultas.py` (só a validação nesta tarefa)
- Test: `explorador/tests/test_validacao_sql.py`

**Interfaces:**
- Produces:
  - `SqlNaoPermitido(ValueError)`, com `str(erro)` sendo a mensagem para o usuário.
  - `validar_sql(sql: str) -> None`, que lança `SqlNaoPermitido`.
  - As constantes `MENSAGEM_SO_LEITURA`, `INICIOS_PERMITIDOS` e `PROIBIDAS`.

- [ ] **Step 1: Escrever o teste que falha**

`explorador/tests/test_validacao_sql.py`:

```python
import pytest

from explorador.consultas import MENSAGEM_SO_LEITURA, SqlNaoPermitido, validar_sql


@pytest.mark.parametrize("sql", [
    "SELECT 1",
    "select * from silver_eventos limit 5;",
    "WITH x AS (SELECT 1) SELECT * FROM x",
    "DESCRIBE silver_eventos",
    "SHOW TABLES",
    "SUMMARIZE silver_eventos",
    "FROM silver_eventos LIMIT 3",
    "EXPLAIN SELECT 1",
    "PRAGMA table_info('silver_eventos')",
    "-- comentário\nSELECT 1",
    "/* bloco */ SELECT 1",
    "SELECT * FROM silver_eventos WHERE event_name = 'drop'",
    "SELECT 'copy; create' AS texto",
    "SELECT 1 -- delete tudo",
    "SELECT \"set\" FROM (SELECT 1 AS \"set\")",
    "SELECT user_id, updated FROM (SELECT 1 AS user_id, 2 AS updated)",
])
def test_aceita_consultas_de_leitura(sql):
    validar_sql(sql)


@pytest.mark.parametrize("sql, trecho", [
    ("COPY silver_eventos TO 'x.csv'", MENSAGEM_SO_LEITURA),
    ("CREATE TABLE t AS SELECT 1", MENSAGEM_SO_LEITURA),
    ("ATTACH 'outro.db'", MENSAGEM_SO_LEITURA),
    ("INSTALL spatial", MENSAGEM_SO_LEITURA),
    ("SET threads = 1", MENSAGEM_SO_LEITURA),
    ("PRAGMA enable_profiling", MENSAGEM_SO_LEITURA),
    ("SELECT 1; DROP VIEW silver_eventos", "um comando por vez"),
    ("SELECT 1; SELECT 2", "um comando por vez"),
    ("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", "INSERT"),
    ("", "Escreva uma consulta"),
    ("   -- só comentário", "Escreva uma consulta"),
])
def test_recusa_o_que_nao_e_leitura(sql, trecho):
    with pytest.raises(SqlNaoPermitido, match=trecho):
        validar_sql(sql)
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `docker compose run --rm app python -m pytest explorador/tests/test_validacao_sql.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'explorador.consultas'`

- [ ] **Step 3: Criar `explorador/consultas.py` com a validação**

```python
"""Consultas SQL no bucket com DuckDB — somente leitura."""

from __future__ import annotations

import re

MENSAGEM_SO_LEITURA = "O explorador só faz consultas de leitura (SELECT, WITH, DESCRIBE…)."
INICIOS_PERMITIDOS = ("select", "with", "describe", "show", "summarize", "from", "explain", "pragma")
PROIBIDAS = ("copy", "attach", "detach", "install", "load", "create", "drop", "insert", "update",
             "delete", "alter", "export", "import", "set", "call", "checkpoint", "vacuum", "use")


class SqlNaoPermitido(ValueError):
    """SQL que o explorador não executa; a mensagem vai direto para o usuário."""


def _somente_codigo(sql: str) -> str:
    """Troca o conteúdo de strings por vazio e remove comentários, para analisar só o código."""
    saida, i, n = [], 0, len(sql)
    while i < n:
        if sql.startswith("--", i):
            fim = sql.find("\n", i)
            i = n if fim < 0 else fim
            continue
        if sql.startswith("/*", i):
            fim = sql.find("*/", i + 2)
            i = n if fim < 0 else fim + 2
            saida.append(" ")
            continue
        aspa = sql[i]
        if aspa in "'\"":
            j = i + 1
            while j < n:
                if sql[j] == aspa:
                    if j + 1 < n and sql[j + 1] == aspa:
                        j += 2
                        continue
                    break
                j += 1
            saida.append(aspa * 2)
            i = j + 1
            continue
        saida.append(aspa)
        i += 1
    return "".join(saida)


def validar_sql(sql: str) -> None:
    codigo = _somente_codigo(sql).strip().rstrip(";").strip()
    if not codigo:
        raise SqlNaoPermitido("Escreva uma consulta SQL.")
    if ";" in codigo:
        raise SqlNaoPermitido("Rode um comando por vez (sem `;` no meio).")
    palavras = re.findall(r"[a-z_][a-z0-9_]*", codigo.lower())
    primeira = palavras[0] if palavras else ""
    if primeira not in INICIOS_PERMITIDOS:
        raise SqlNaoPermitido(MENSAGEM_SO_LEITURA)
    if primeira == "pragma" and not re.match(r"pragma\s+table_info\b", codigo.lower()):
        raise SqlNaoPermitido(MENSAGEM_SO_LEITURA)
    proibida = next((p for p in palavras if p in PROIBIDAS), None)
    if proibida:
        raise SqlNaoPermitido(f"{MENSAGEM_SO_LEITURA} O comando `{proibida.upper()}` não é permitido.")
```

- [ ] **Step 4: Rodar e ver passar**

Run: `docker compose run --rm app python -m pytest explorador/tests/test_validacao_sql.py -v`
Expected: 27 passed

- [ ] **Step 5: Commit**

```bash
git add explorador/consultas.py explorador/tests/test_validacao_sql.py
git commit -m "Explorador: validacao de SQL somente leitura"
```

---

### Task 4: Conexão, views, execução e prévia

**Files:**
- Modify: `explorador/consultas.py` (acrescentar ao fim)
- Test: `explorador/tests/test_consultas.py`

**Interfaces:**
- Consumes: `particoes.Dataset`, `particoes.formato_do_arquivo`.
- Produces:
  - `conectar(s3: bool = True, endpoint: str = ..., chave: str = ..., segredo: str = ..., regiao: str = ...) -> duckdb.DuckDBPyConnection`. Com `s3=True`, faz `LOAD httpfs` e cria o `SECRET` do floci.
  - `criar_views(con, raiz: str, datasets: list[Dataset]) -> dict[str, str]`, de nome da view para a expressão de origem.
  - `Resultado(tabela: pandas.DataFrame, cortado: bool, colunas: list[str])`.
  - `executar(con, sql: str, limite: int = LIMITE_PADRAO) -> Resultado`.
  - `previa(con, raiz: str, chave: str, linhas: int = LINHAS_PREVIA) -> pandas.DataFrame`, que lança `ValueError` para arquivo que não seja `.parquet` nem `.jsonl`.
  - Constantes: `LIMITE_PADRAO = 1000`, `LIMITE_MAXIMO = 50_000`, `LINHAS_PREVIA = 100`.

O spec cita `conectar(raiz, s3=True)`. Aqui `raiz` sai da assinatura, porque a conexão não depende dela: a raiz só entra em `criar_views` e `previa`.

- [ ] **Step 1: Escrever o teste que falha**

`explorador/tests/test_consultas.py`:

```python
import duckdb
import pytest

from explorador import consultas, particoes


@pytest.fixture
def lago(tmp_path):
    """Monta um mini-bucket local: Silver particionada, landing JSONL e uma Gold."""
    con = duckdb.connect()
    for dia in ("01", "02"):
        pasta = tmp_path / "silver" / "eventos" / "year=2026" / "month=09" / f"day={dia}"
        pasta.mkdir(parents=True)
        con.sql(f"COPY (SELECT 'e{dia}-' || i AS event_id, 'page_view' AS event_name FROM range(3) t(i)) "
                f"TO '{pasta}/part-0.parquet' (FORMAT parquet)")
    gold = tmp_path / "gold" / "funil_compra"
    gold.mkdir(parents=True)
    con.sql(f"COPY (SELECT 'purchase' AS etapa, 7 AS sessoes) TO '{gold}/part-0.parquet' (FORMAT parquet)")
    (gold / "_SUCCESS").write_text("")
    landing = tmp_path / "landing" / "eventos" / "year=2026" / "month=09" / "day=01"
    landing.mkdir(parents=True)
    (landing / "coletor-s42-001.jsonl").write_text(
        '{"event_id":"a","event_name":"scroll"}\n{"event_id":"b","event_name":"click"}\n', encoding="utf-8")
    chaves = [p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*") if p.is_file()]
    conexao = consultas.conectar(s3=False)
    views = consultas.criar_views(conexao, str(tmp_path), particoes.datasets(chaves))
    return conexao, views, tmp_path


def test_cria_uma_view_por_dataset(lago):
    _, views, _ = lago
    assert set(views) == {"gold_funil_compra", "landing_eventos", "silver_eventos"}


def test_particoes_viram_colunas_de_texto(lago):
    con, _, _ = lago
    r = consultas.executar(con, "SELECT DISTINCT month, day, typeof(month) AS tipo FROM silver_eventos ORDER BY day")
    assert r.tabela.to_dict("records") == [
        {"month": "09", "day": "01", "tipo": "VARCHAR"},
        {"month": "09", "day": "02", "tipo": "VARCHAR"},
    ]


def test_contagens_das_views(lago):
    con, _, _ = lago
    contar = lambda v: consultas.executar(con, f"SELECT count(*) AS n FROM {v}").tabela["n"][0]
    assert (contar("silver_eventos"), contar("landing_eventos"), contar("gold_funil_compra")) == (6, 2, 1)


def test_executar_corta_no_limite_e_avisa(lago):
    con, _, _ = lago
    r = consultas.executar(con, "SELECT * FROM range(10)", limite=3)
    assert len(r.tabela) == 3 and r.cortado is True and r.colunas == ["range"]
    r = consultas.executar(con, "SELECT * FROM range(10)", limite=10)
    assert len(r.tabela) == 10 and r.cortado is False


def test_executar_nunca_passa_do_limite_maximo(lago):
    con, _, _ = lago
    r = consultas.executar(con, f"SELECT * FROM range({consultas.LIMITE_MAXIMO + 5})", limite=10**9)
    assert len(r.tabela) == consultas.LIMITE_MAXIMO and r.cortado is True


def test_executar_recusa_sql_proibido_sem_executar(lago):
    con, _, tmp_path = lago
    destino = tmp_path / "copia.csv"
    with pytest.raises(consultas.SqlNaoPermitido):
        consultas.executar(con, f"COPY silver_eventos TO '{destino}'")
    assert not destino.exists()


def test_previa_le_um_arquivo(lago):
    con, _, tmp_path = lago
    chave = "landing/eventos/year=2026/month=09/day=01/coletor-s42-001.jsonl"
    assert consultas.previa(con, str(tmp_path), chave)["event_id"].tolist() == ["a", "b"]
    assert len(consultas.previa(con, str(tmp_path), "gold/funil_compra/part-0.parquet")) == 1
    with pytest.raises(ValueError, match="Prévia"):
        consultas.previa(con, str(tmp_path), "gold/funil_compra/_SUCCESS")
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `docker compose run --rm app python -m pytest explorador/tests/test_consultas.py -v`
Expected: FAIL com `AttributeError: module 'explorador.consultas' has no attribute 'conectar'`

- [ ] **Step 3: Acrescentar a `explorador/consultas.py`**

No topo, junto dos imports:

```python
from dataclasses import dataclass

import duckdb
import pandas as pd

from explorador.particoes import Dataset, formato_do_arquivo
```

Ao final do arquivo:

```python
LIMITE_PADRAO = 1000
LIMITE_MAXIMO = 50_000
LINHAS_PREVIA = 100
OPCOES_LEITURA = "hive_partitioning=true, hive_types_autocast=false, union_by_name=true"


@dataclass
class Resultado:
    tabela: pd.DataFrame
    cortado: bool
    colunas: list[str]


def _texto_sql(valor: str) -> str:
    return "'" + valor.replace("'", "''") + "'"


def conectar(s3: bool = True, endpoint: str = "http://floci:4566", chave: str = "test",
             segredo: str = "test", regiao: str = "us-east-1") -> duckdb.DuckDBPyConnection:
    """Conexão DuckDB em memória. Com s3=True, aponta o S3 do DuckDB para o floci."""
    con = duckdb.connect()
    if s3:
        con.sql("LOAD httpfs")
        ssl = endpoint.startswith("https://")
        host = endpoint.removeprefix("https://").removeprefix("http://").rstrip("/")
        con.sql(
            "CREATE OR REPLACE SECRET floci (TYPE s3, "
            f"KEY_ID {_texto_sql(chave)}, SECRET {_texto_sql(segredo)}, REGION {_texto_sql(regiao)}, "
            f"ENDPOINT {_texto_sql(host)}, URL_STYLE 'path', USE_SSL {'true' if ssl else 'false'})"
        )
    return con


def _origem(raiz: str, dataset: Dataset) -> str:
    extensao = "parquet" if dataset.formato == "parquet" else "jsonl"
    funcao = "read_parquet" if dataset.formato == "parquet" else "read_json_auto"
    padrao = f"{raiz.rstrip('/')}/{dataset.prefixo}/**/*.{extensao}"
    return f"{funcao}({_texto_sql(padrao)}, {OPCOES_LEITURA})"


def criar_views(con: duckdb.DuckDBPyConnection, raiz: str, datasets: list[Dataset]) -> dict[str, str]:
    views = {}
    for dataset in datasets:
        origem = _origem(raiz, dataset)
        con.sql(f'CREATE OR REPLACE VIEW "{dataset.nome_view}" AS SELECT * FROM {origem}')
        views[dataset.nome_view] = origem
    return views


def executar(con: duckdb.DuckDBPyConnection, sql: str, limite: int = LIMITE_PADRAO) -> Resultado:
    validar_sql(sql)
    limite = max(1, min(int(limite), LIMITE_MAXIMO))
    relacao = con.sql(sql)
    if relacao is None:
        return Resultado(pd.DataFrame(), False, [])
    tabela = relacao.limit(limite + 1).df()
    cortado = len(tabela) > limite
    tabela = tabela.head(limite)
    return Resultado(tabela, cortado, list(tabela.columns))


def previa(con: duckdb.DuckDBPyConnection, raiz: str, chave: str, linhas: int = LINHAS_PREVIA) -> pd.DataFrame:
    formato = formato_do_arquivo(chave)
    if formato is None:
        raise ValueError("Prévia só para arquivos .parquet e .jsonl.")
    funcao = "read_parquet" if formato == "parquet" else "read_json_auto"
    caminho = f"{raiz.rstrip('/')}/{chave}"
    return con.sql(f"SELECT * FROM {funcao}({_texto_sql(caminho)}) LIMIT {int(linhas)}").df()
```

- [ ] **Step 4: Rodar e ver passar**

Run: `docker compose run --rm app python -m pytest explorador/tests -v`
Expected: 41 passed (7 + 27 + 7)

- [ ] **Step 5: Commit**

```bash
git add explorador/consultas.py explorador/tests/test_consultas.py
git commit -m "Explorador: conexao DuckDB com o floci, views por dataset, execucao com limite e previa"
```

---

### Task 5: `s3.py` — listagem do bucket

**Files:**
- Create: `explorador/s3.py`
- Test: `explorador/tests/test_s3.py`

**Interfaces:**
- Consumes: `particoes.Objeto`.
- Produces:
  - As constantes `ENDPOINT`, `BUCKET`, `CHAVE`, `SEGREDO` e `REGIAO`, vindas do ambiente.
  - `raiz() -> str`, que devolve `"s3://<BUCKET>"`.
  - `S3Indisponivel(RuntimeError)`.
  - `listar(prefixo: str = "", cliente=None) -> list[Objeto]`. Devolve `[]` se o bucket não existe. Lança `S3Indisponivel` se não conseguir conectar.

- [ ] **Step 1: Escrever o teste que falha**

`explorador/tests/test_s3.py`:

```python
from datetime import datetime, timezone

import pytest

from explorador import s3
from explorador.particoes import Objeto

AGORA = datetime(2026, 10, 10, tzinfo=timezone.utc)


class Paginador:
    def __init__(self, paginas):
        self.paginas = paginas
        self.argumentos = None

    def paginate(self, **argumentos):
        self.argumentos = argumentos
        return iter(self.paginas)


class S3Falso:
    def __init__(self, paginas=(), erro=None):
        self.paginador = Paginador(list(paginas))
        self.erro = erro

    def get_paginator(self, nome):
        assert nome == "list_objects_v2"
        if self.erro:
            raise self.erro
        return self.paginador


class EndpointConnectionError(Exception):
    pass


class ErroS3(Exception):
    def __init__(self, codigo):
        super().__init__(codigo)
        self.response = {"Error": {"Code": codigo}}


def test_listar_junta_todas_as_paginas():
    falso = S3Falso([
        {"Contents": [{"Key": "a/1.parquet", "Size": 10, "LastModified": AGORA}]},
        {"Contents": [{"Key": "a/2.parquet", "Size": 20, "LastModified": AGORA}]},
        {},
    ])
    assert s3.listar("a/", cliente=falso) == [Objeto("a/1.parquet", 10, AGORA), Objeto("a/2.parquet", 20, AGORA)]
    assert falso.paginador.argumentos == {"Bucket": s3.BUCKET, "Prefix": "a/"}


def test_bucket_inexistente_vira_lista_vazia():
    assert s3.listar(cliente=S3Falso(erro=ErroS3("NoSuchBucket"))) == []


def test_floci_desligado_vira_mensagem_amigavel():
    with pytest.raises(s3.S3Indisponivel, match="docker compose up -d floci"):
        s3.listar(cliente=S3Falso(erro=EndpointConnectionError("sem rede")))


def test_outros_erros_sobem_como_estao():
    with pytest.raises(ErroS3):
        s3.listar(cliente=S3Falso(erro=ErroS3("AccessDenied")))


def test_raiz_usa_o_bucket():
    assert s3.raiz() == f"s3://{s3.BUCKET}"
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `docker compose run --rm app python -m pytest explorador/tests/test_s3.py -v`
Expected: FAIL com `ImportError: cannot import name 's3' from 'explorador'`

- [ ] **Step 3: Criar `explorador/s3.py`**

```python
"""Lista o bucket do floci com boto3. O explorador só lê: nada aqui grava ou apaga."""

from __future__ import annotations

import os

from explorador.particoes import Objeto

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL") or "http://floci:4566"
BUCKET = os.environ.get("AWS_S3_BUCKET") or "datalake-tracking"
CHAVE = os.environ.get("AWS_ACCESS_KEY_ID") or "test"
SEGREDO = os.environ.get("AWS_SECRET_ACCESS_KEY") or "test"
REGIAO = os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
ERROS_DE_CONEXAO = {"EndpointConnectionError", "ConnectTimeoutError", "ConnectionClosedError",
                    "ReadTimeoutError", "ConnectionError"}


class S3Indisponivel(RuntimeError):
    """O floci não respondeu; a mensagem vai direto para o usuário."""


def raiz() -> str:
    return f"s3://{BUCKET}"


def _cliente():
    import boto3
    from botocore.config import Config

    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=CHAVE,
                        aws_secret_access_key=SEGREDO, region_name=REGIAO,
                        config=Config(connect_timeout=3, read_timeout=10, retries={"max_attempts": 1}))


def listar(prefixo: str = "", cliente=None) -> list[Objeto]:
    s3 = cliente if cliente is not None else _cliente()
    try:
        objetos = []
        for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefixo):
            for item in pagina.get("Contents", []):
                objetos.append(Objeto(item["Key"], item["Size"], item.get("LastModified")))
        return objetos
    except Exception as erro:
        codigo = getattr(erro, "response", {}).get("Error", {}).get("Code")
        if codigo in ("NoSuchBucket", "404"):
            return []
        if type(erro).__name__ in ERROS_DE_CONEXAO:
            raise S3Indisponivel(
                f"Não consegui falar com o S3 em {ENDPOINT}. Rode `docker compose up -d floci`."
            ) from None
        raise
```

- [ ] **Step 4: Rodar e ver passar**

Run: `docker compose run --rm app python -m pytest explorador/tests -v`
Expected: 46 passed

- [ ] **Step 5: Commit**

```bash
git add explorador/s3.py explorador/tests/test_s3.py
git commit -m "Explorador: listagem do bucket com mensagem amigavel quando o floci esta desligado"
```

---

### Task 6: `app.py` — interface Streamlit

**Files:**
- Create: `explorador/app.py`
- Test: `explorador/tests/test_app.py`

**Interfaces:**
- Consumes:
  - `s3.listar`, `s3.raiz`, `s3.S3Indisponivel`, `s3.BUCKET`, `s3.ENDPOINT`, `s3.CHAVE`, `s3.SEGREDO`, `s3.REGIAO`;
  - `particoes.datasets`, `tabela_particoes`, `navegar`, `tamanho_legivel`, `formato_do_arquivo`, `SEM_PARTICAO`;
  - `consultas.conectar`, `criar_views`, `executar`, `previa`, `SqlNaoPermitido`, `LIMITE_PADRAO`, `LIMITE_MAXIMO`.
- Produces: a página com as abas "Arquivos", "Partições" e "Consultas SQL". Widgets com chave fixa, que os testes usam:
  - `text_area` `"sql"`;
  - `number_input` `"limite"`;
  - `button` `"executar"` e `"recarregar"`;
  - `selectbox` `"dataset"` e `"previa"`.

- [ ] **Step 1: Escrever o teste que falha**

`explorador/tests/test_app.py`:

```python
import functools
from pathlib import Path

import duckdb
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from explorador import consultas, s3
from explorador.particoes import Objeto

APP = str(Path(__file__).resolve().parents[1] / "app.py")


@pytest.fixture(autouse=True)
def limpar_cache():
    st.cache_data.clear()
    st.cache_resource.clear()
    yield
    st.cache_data.clear()
    st.cache_resource.clear()


def _rodar():
    return AppTest.from_file(APP, default_timeout=60).run()


def test_floci_desligado_mostra_mensagem_amigavel(monkeypatch):
    def desligado(*_, **__):
        raise s3.S3Indisponivel("Não consegui falar com o S3 em http://floci:4566. Rode `docker compose up -d floci`.")
    monkeypatch.setattr(s3, "listar", desligado)
    at = _rodar()
    assert not at.exception
    assert "docker compose up -d floci" in at.error[0].value


def test_bucket_vazio_orienta_a_rodar_o_gerador(monkeypatch):
    monkeypatch.setattr(s3, "listar", lambda *_, **__: [])
    at = _rodar()
    assert not at.exception
    assert "gerar_eventos.py" in at.info[0].value


@pytest.fixture
def bucket_local(tmp_path, monkeypatch):
    pasta = tmp_path / "silver" / "eventos" / "year=2026" / "month=09" / "day=01"
    pasta.mkdir(parents=True)
    duckdb.sql(f"COPY (SELECT 'e' || i AS event_id FROM range(4) t(i)) TO '{pasta}/part-0.parquet' (FORMAT parquet)")
    objetos = [Objeto(p.relative_to(tmp_path).as_posix(), p.stat().st_size)
               for p in tmp_path.rglob("*") if p.is_file()]
    monkeypatch.setattr(s3, "listar", lambda *_, **__: objetos)
    monkeypatch.setattr(s3, "raiz", lambda: str(tmp_path))
    monkeypatch.setattr(consultas, "conectar", functools.partial(consultas.conectar, s3=False))
    return tmp_path


def test_app_monta_as_tres_abas_e_executa_sql(bucket_local):
    at = _rodar()
    assert not at.exception
    assert [t.label for t in at.tabs] == ["Arquivos", "Partições", "Consultas SQL"]
    at.text_area(key="sql").input("SELECT count(*) AS n FROM silver_eventos").run()
    at.button(key="executar").click().run()
    assert not at.exception
    assert at.dataframe[-1].value["n"].tolist() == [4]


def test_app_recusa_sql_de_escrita(bucket_local):
    at = _rodar()
    at.text_area(key="sql").input("DROP VIEW silver_eventos").run()
    at.button(key="executar").click().run()
    assert any("só faz consultas de leitura" in w.value for w in at.warning)


def test_app_mostra_aviso_quando_corta_o_resultado(bucket_local):
    at = _rodar()
    at.text_area(key="sql").input("SELECT * FROM range(20)").run()
    at.number_input(key="limite").set_value(5).run()
    at.button(key="executar").click().run()
    assert len(at.dataframe[-1].value) == 5
    assert any("cortado em 5 linhas" in w.value for w in at.warning)
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `docker compose run --rm app python -m pytest explorador/tests/test_app.py -v`
Expected: FAIL. O `AppTest` não acha `explorador/app.py` (erro de arquivo inexistente).

- [ ] **Step 3: Criar `explorador/app.py`**

```python
"""Explorador do datalake: arquivos, partições e consultas SQL no S3 do floci.

    docker compose up -d explorador      # e abra http://localhost:8501

Somente leitura: nada aqui grava ou apaga no bucket.
"""

from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import pandas as pd
import streamlit as st

from explorador import consultas, particoes, s3

EXEMPLOS = {
    "Eventos por nome (landing)": (
        "SELECT event_name, count(*) AS eventos\nFROM landing_eventos\n"
        "GROUP BY event_name\nORDER BY eventos DESC"
    ),
    "Linhas por dia (Silver)": (
        "SELECT year, month, day, count(*) AS linhas\nFROM silver_eventos\nGROUP BY ALL\nORDER BY ALL"
    ),
    "Colunas de uma view": "DESCRIBE silver_eventos",
}


@st.cache_data(ttl=30, show_spinner="Listando o bucket…")
def _listar() -> list[particoes.Objeto]:
    return s3.listar()


@st.cache_resource(show_spinner="Conectando ao S3 e criando as views…")
def _conexao(datasets: tuple[particoes.Dataset, ...], raiz: str):
    con = consultas.conectar(endpoint=s3.ENDPOINT, chave=s3.CHAVE, segredo=s3.SEGREDO, regiao=s3.REGIAO)
    views = consultas.criar_views(con, raiz, list(datasets))
    colunas = {nome: con.sql(f'DESCRIBE "{nome}"').df()["column_name"].tolist() for nome in views}
    return con, colunas


def _aba_arquivos(objetos, con, raiz):
    pasta = st.session_state.get("pasta", "")
    st.markdown(f"**Pasta atual:** `/{pasta}`")
    if pasta and st.button("⬆ Voltar uma pasta", key="voltar"):
        partes = pasta.rstrip("/").split("/")[:-1]
        st.session_state["pasta"] = "/".join(partes) + "/" if partes else ""
        st.rerun()
    subpastas, arquivos = particoes.navegar(objetos, pasta)
    colunas = st.columns(4)
    for i, sub in enumerate(subpastas):
        if colunas[i % 4].button(f"📁 {sub}", key=f"pasta:{pasta}{sub}"):
            st.session_state["pasta"] = f"{pasta}{sub}/"
            st.rerun()
    if not arquivos:
        st.caption("Nenhum arquivo direto nesta pasta. Entre numa subpasta.")
        return
    st.dataframe(pd.DataFrame([{"arquivo": o.chave[len(pasta):], "tamanho": particoes.tamanho_legivel(o.tamanho),
                                "modificado": o.modificado} for o in arquivos]), hide_index=True)
    legiveis = [o.chave[len(pasta):] for o in arquivos if particoes.formato_do_arquivo(o.chave)]
    escolhido = st.selectbox("Prévia de um arquivo", legiveis, index=None, key="previa",
                             placeholder="Escolha um arquivo .parquet ou .jsonl")
    if escolhido:
        try:
            st.dataframe(consultas.previa(con.cursor(), raiz, pasta + escolhido), hide_index=True)
            st.caption(f"Primeiras {consultas.LINHAS_PREVIA} linhas, no máximo.")
        except duckdb.Error as erro:
            st.error(f"Não consegui ler o arquivo: {erro}")


def _aba_particoes(objetos, datasets, con):
    if not datasets:
        st.info("Ainda não há datasets (pastas com .parquet ou .jsonl) no bucket.")
        return
    escolha = st.selectbox("Dataset", [d.prefixo for d in datasets], key="dataset")
    linhas = particoes.tabela_particoes(objetos, escolha)
    tabela = pd.DataFrame([asdict(l) for l in linhas])
    tabela["tamanho"] = tabela["bytes"].map(particoes.tamanho_legivel)
    st.dataframe(tabela, hide_index=True)
    st.caption(f"{len(linhas)} partição(ões) · {int(tabela['arquivos'].sum())} arquivo(s) · "
               f"{particoes.tamanho_legivel(int(tabela['bytes'].sum()))}")
    if st.button("Contar linhas por partição", key="contar"):
        view = next(d.nome_view for d in datasets if d.prefixo == escolha)
        particionado = any(l.year != particoes.SEM_PARTICAO for l in linhas)
        sql = (f'SELECT year, month, day, count(*) AS linhas FROM "{view}" GROUP BY ALL ORDER BY ALL'
               if particionado else f'SELECT count(*) AS linhas FROM "{view}"')
        try:
            st.dataframe(consultas.executar(con.cursor(), sql, consultas.LIMITE_MAXIMO).tabela, hide_index=True)
        except duckdb.Error as erro:
            st.error(f"Não consegui contar: {erro}")


def _aba_sql(colunas, con):
    with st.expander(f"Views disponíveis ({len(colunas)})"):
        for nome, cols in colunas.items():
            st.markdown(f"**`{nome}`** — " + ", ".join(f"`{c}`" for c in cols))
    st.caption("Exemplos:")
    botoes = st.columns(len(EXEMPLOS))
    for coluna, (titulo, sql) in zip(botoes, EXEMPLOS.items()):
        coluna.button(titulo, key=f"exemplo:{titulo}", on_click=st.session_state.update, kwargs={"sql": sql})
    st.text_area("SQL (somente leitura)", key="sql", height=160,
                 placeholder="SELECT * FROM silver_eventos LIMIT 10")
    limite = st.number_input("Limite de linhas", min_value=1, max_value=consultas.LIMITE_MAXIMO,
                             value=consultas.LIMITE_PADRAO, step=100, key="limite")
    if not st.button("Executar", key="executar", type="primary"):
        return
    try:
        resultado = consultas.executar(con.cursor(), st.session_state.get("sql", ""), limite)
    except consultas.SqlNaoPermitido as erro:
        st.warning(str(erro))
        return
    except duckdb.Error as erro:
        st.error(f"Erro no SQL: {erro}")
        st.info("Confira os nomes em \"Views disponíveis\", logo acima.")
        return
    if resultado.cortado:
        st.warning(f"Resultado cortado em {int(limite)} linhas. Aumente o limite ou filtre a consulta.")
    st.dataframe(resultado.tabela, hide_index=True)
    st.download_button("Baixar CSV", resultado.tabela.to_csv(index=False).encode("utf-8"),
                       file_name="resultado.csv", mime="text/csv", key="baixar")


def main() -> None:
    st.set_page_config(page_title="Explorador do datalake", layout="wide")
    st.title("Explorador do datalake")
    topo, botao = st.columns([5, 1])
    topo.caption(f"Bucket `{s3.BUCKET}` em `{s3.ENDPOINT}` · somente leitura")
    if botao.button("Recarregar", key="recarregar"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    try:
        objetos = _listar()
    except s3.S3Indisponivel as erro:
        st.error(str(erro))
        st.stop()
    if not objetos:
        st.info("O bucket está vazio. Gere os eventos com "
                "`docker compose run --rm app python gerador/gerar_eventos.py` e clique em Recarregar.")
        st.stop()

    datasets = particoes.datasets(o.chave for o in objetos)
    raiz = s3.raiz()
    try:
        con, colunas = _conexao(tuple(datasets), raiz)
    except duckdb.Error as erro:
        st.error(f"Não consegui preparar as views no DuckDB: {erro}")
        st.stop()

    arquivos, particoes_, sql = st.tabs(["Arquivos", "Partições", "Consultas SQL"])
    with arquivos:
        _aba_arquivos(objetos, con, raiz)
    with particoes_:
        _aba_particoes(objetos, datasets, con)
    with sql:
        _aba_sql(colunas, con)


main()
```

O `_conexao` recebe `raiz` como argumento, e não a lê de `s3.raiz()` lá dentro. Assim a chave do cache inclui a raiz, e os testes (que trocam `s3.raiz`) não reaproveitam uma conexão de outro teste.

- [ ] **Step 4: Rodar e ver passar**

Run: `docker compose run --rm app python -m pytest explorador/tests -v`
Expected: 51 passed

Se algum teste de `AppTest` falhar por diferença de API do Streamlit 1.65 (por exemplo `.input()` × `.set_value()` no `text_area`), ajuste **só o teste** para a API correta, mantendo o que ele verifica, e registre o ajuste no relatório.

- [ ] **Step 5: Rodar a suíte inteira**

Run: `docker compose run --rm app python -m pytest`
Expected: 107 passed (56 gerador + 51 explorador)

- [ ] **Step 6: Commit**

```bash
git add explorador/app.py explorador/tests/test_app.py
git commit -m "Explorador: interface Streamlit com abas de arquivos, particoes e SQL"
```

---

### Task 7: Documentação e conferência real

**Files:**
- Create: `explorador/README.md`
- Modify: `README.md`, `SETUP.md`

**Interfaces:**
- Consumes: o serviço `explorador` (Task 1) e o app (Task 6).

- [ ] **Step 1: Criar `explorador/README.md`**

````markdown
# Explorador do datalake

Um site que roda no seu computador para **olhar o bucket** sem escrever código: ver os arquivos que o gerador e o seu pipeline gravaram, ver as partições `year=/month=/day=` de cada pasta e fazer **consultas SQL** sobre os dados.

## Como abrir

```bash
docker compose up -d floci explorador
```

Abra **http://localhost:8501** no navegador. Para desligar: `docker compose stop explorador`.

## As três abas

- **Arquivos** — navegue pelas pastas do bucket (`landing/`, `bronze/`, `silver/`, `gold/`) e veja nome, tamanho e data de cada arquivo. Escolha um `.parquet` ou `.jsonl` para ver as primeiras 100 linhas.
- **Partições** — escolha um dataset e veja uma linha por dia (`year`, `month`, `day`) com quantos arquivos e quantos bytes ele tem. O botão "Contar linhas por partição" conta as linhas de cada dia.
- **Consultas SQL** — escreva SQL e veja o resultado numa tabela, que você pode baixar em CSV.

## Views: os nomes que você usa no SQL

Cada pasta com dados vira uma **view**, uma tabela virtual que lê os arquivos na hora. O nome é o caminho da pasta com `_` no lugar de `/`:

| Pasta no bucket | View |
| --- | --- |
| `landing/eventos/` | `landing_eventos` |
| `silver/eventos/` | `silver_eventos` |
| `gold/funil_compra/` | `gold_funil_compra` |

As views das pastas particionadas têm as colunas `year`, `month` e `day`, como texto (`month` é `'09'`). A lista completa de views e colunas aparece em "Views disponíveis", na aba de SQL.

Exemplos:

```sql
SELECT event_name, count(*) AS eventos FROM landing_eventos GROUP BY event_name ORDER BY eventos DESC;
SELECT year, month, day, count(*) AS linhas FROM silver_eventos GROUP BY ALL ORDER BY ALL;
DESCRIBE silver_eventos;
```

## Somente leitura

O explorador só **lê**. Ele aceita consultas que começam com `SELECT`, `WITH`, `DESCRIBE`, `SHOW`, `SUMMARIZE`, `FROM`, `EXPLAIN` ou `PRAGMA table_info`, uma por vez. Comandos que mudam dados (`COPY`, `CREATE`, `DROP`, `INSERT`…) são recusados. Assim, você não estraga o seu datalake por engano.

As consultas usam o **DuckDB**, um motor SQL rápido para arquivos. Ele serve só para explorar. O seu pipeline continua sendo construído com o Spark, como pede o enunciado.

Depois de rodar um job novo, clique em **Recarregar** para o explorador encontrar as pastas novas.
````

- [ ] **Step 2: Atualizar `README.md` e `SETUP.md`**
  - **`README.md`:** na tabela "O que há aqui", acrescentar a linha ``| [`explorador/`](explorador/README.md) | um site para ver os arquivos, as partições e fazer consultas SQL no bucket (http://localhost:8501) |``.
  - **`SETUP.md`:**
    - Depois do passo que confere o Spark (`infra/verificar_ambiente.py`), acrescentar um passo **"Explore o bucket"**. Ele diz o que o passo faz, para iniciantes, com o comando `docker compose up -d explorador`, o endereço `http://localhost:8501` e o que deve aparecer: o título "Explorador do datalake" e as abas Arquivos, Partições e Consultas SQL. Aponta para `explorador/README.md`.
    - Na tabela de problemas comuns, acrescentar: "a página `localhost:8501` não abre → o serviço não está rodando ou a porta 8501 está ocupada → `docker compose up -d explorador`; veja `docker compose logs explorador`".

- [ ] **Step 3: Conferência real**

Com a landing e o pipeline já gravados no floci:

```bash
docker compose up -d floci explorador
docker compose logs explorador
```

Expected: o log mostra `You can now view your Streamlit app` (ou equivalente). Depois, o controlador confere no navegador, em `http://localhost:8501`:
- **Topo:** título e caption com o bucket.
- **Arquivos:** navegar `landing` → `eventos` → `year=2026` → `month=09` → `day=01` e ver a prévia de um `.jsonl`.
- **Partições:** `silver/eventos` com dias de `01` a `15` e `month` igual a `09`.
- **SQL:**
  - `SELECT count(*) FROM silver_eventos` → **61813**;
  - `SELECT count(*) FROM landing_eventos` → **64372**;
  - `COPY silver_eventos TO 'x.csv'` → aviso de "só faz consultas de leitura".

- [ ] **Step 4: Conferir que o pacote do aluno inclui o explorador**

```bash
git archive --format=zip -o "$TEMP/aluno.zip" HEAD && unzip -l "$TEMP/aluno.zip" | grep -c "explorador/" ; rm "$TEMP/aluno.zip"
```

Expected: número maior que 0. Antes, faça o commit do Step 5, porque o `git archive` só vê o que está commitado.

- [ ] **Step 5: Commit**

```bash
git add explorador/README.md README.md SETUP.md
git commit -m "Explorador: documentacao para iniciantes no README, SETUP e explorador/README"
```
