# Plano 1 — Gerador de eventos e infraestrutura

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir o simulador do coletor de tracking (`gerador/`) que popula `landing/eventos/` no S3 (floci) com 14 dias de eventos da landing DadosPro, no contrato do spec, com defeitos e armadilhas observáveis, mais a infraestrutura mínima do repositório.

**Architecture:** Python puro, sem Spark e sem dependência de `pipeline/`. Um único `random.Random(seed)` atravessa toda a geração, para que a mesma seed produza exatamente os mesmos arquivos. A geração é uma sequência de funções puras:

```
simular (jornadas.py) → aplicar_sujeira (sujeira.py) → montar_lotes (lotes.py) → Destino.gravar (destinos.py)
```

`gerar_eventos.py` só orquestra e imprime o resumo. O destino é o S3 via `boto3` ou uma pasta local (`--saida-local`), o que permite testar tudo sem Docker.

**Tech Stack:** Python 3.11 (o código roda em 3.10+), `boto3`, `pytest`, floci (S3 emulado) via Docker Compose.

**Spec:** `docs/specs/2026-10-10-projeto-final-datalake-tracking-design.md`, seções 2, 4, 5, 9 e 10.

**Fora deste plano (Plano 2):** solução de referência em `professor/solucao/`, `GABARITO.md`, `docs/ENUNCIADO.md`, `docs/CONTRATO_EVENTO.md`, `pipeline/README.md`. O Plano 2 será escrito depois deste rodar, com o dado real em mãos.

---

## Mapa de arquivos

| Arquivo | Responsabilidade |
| --- | --- |
| `pyproject.toml` | configuração do pytest (`pythonpath = ["."]`) |
| `requirements.txt` | `pyspark==3.5.3`, `boto3`, `pytest` |
| `docker-compose.yml` | floci na porta 4566 |
| `.gitattributes` | `export-ignore` de `professor/`, `docs/specs/` e `docs/superpowers/` |
| `.gitignore` | ambiente, saídas locais e artefatos |
| `gerador/__init__.py` | marca o pacote |
| `gerador/catalogo.py` | constantes de negócio: eventos, planos, preços, canais, dispositivos, textos |
| `gerador/eventos.py` | `Contexto`, `criar_evento` (o contrato do evento), `item`, `iso`, `novo_uuid`, `hex8` |
| `gerador/jornadas.py` | máquina de estados de sessão e de visitante; `simular()` |
| `gerador/sujeira.py` | `aplicar_sujeira()`: defeitos injetados com cotas por tipo |
| `gerador/lotes.py` | `montar_lotes()`: agrupa por dia, divide em lotes, reenvia ~1%; `serializar()` |
| `gerador/destinos.py` | `DestinoLocal` e `DestinoS3` (substitui o `s3.py` citado no spec) |
| `gerador/gerar_eventos.py` | CLI; `gerar()` e `imprimir_resumo()` |
| `gerador/importar_site.py` | CLI; valida e sobe o JSONL exportado por `site/index.html` |
| `gerador/README.md` | uso do gerador + resumo medido com os parâmetros padrão |
| `tests/gerador/*.py` | testes unitários de cada módulo |
| `SETUP.md`, `README.md` | ambiente do zero e visão geral do repositório |

**Desvio do spec, registrado na Task 11:** o cliente S3 do gerador fica em `destinos.py`, junto com o destino local, em vez de `s3.py`. Além de `professor/`, também `docs/specs/` e `docs/superpowers/` saem do pacote do aluno, porque descrevem a solução.

Todos os comandos abaixo são PowerShell, executados na raiz `C:\Repo\projeto-final-tracking`.

---

### Task 1: Esqueleto do repositório e ambiente

**Files:**
- Create: `pyproject.toml`, `requirements.txt`, `docker-compose.yml`, `.gitattributes`, `gerador/__init__.py`, `tests/gerador/__init__.py`
- Modify: `.gitignore`

- [ ] **Step 1: Criar `pyproject.toml`**

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
addopts = "-q"
```

- [ ] **Step 2: Criar `requirements.txt`**

```text
pyspark==3.5.3
boto3
pytest
```

- [ ] **Step 3: Criar `docker-compose.yml`**

```yaml
services:
  floci:
    image: floci/floci:latest
    container_name: floci-datalake-tracking
    ports:
      - "4566:4566"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

- [ ] **Step 4: Criar `.gitattributes`**

```text
* text=auto eol=lf
*.ps1 text eol=crlf
professor/ export-ignore
docs/specs/ export-ignore
docs/superpowers/ export-ignore
```

- [ ] **Step 5: Substituir `.gitignore`**

```text
.venv/
__pycache__/
*.pyc
.pytest_cache/
hadoop/
dados/
camadas/
saida/
spark-warehouse/
*.jsonl
*.zip
.env
```

- [ ] **Step 6: Criar `gerador/__init__.py` e `tests/gerador/__init__.py`**

Os dois com o mesmo conteúdo:

```python
```

(arquivos vazios)

- [ ] **Step 7: Criar o venv e instalar**

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Se `py -3.11` não existir, **pare e pergunte ao usuário** antes de instalar Python (`winget install -e --id Python.Python.3.11`). Para as Tasks 2–8, um venv com qualquer Python 3.10+ serve (`py -m venv .venv` + `pip install boto3 pytest`). O PySpark só é necessário no Plano 2.

- [ ] **Step 8: Rodar o pytest vazio**

Run: `.\.venv\Scripts\python.exe -m pytest`
Expected: `no tests ran` (código de saída 5)

- [ ] **Step 9: Commit**

```powershell
git add pyproject.toml requirements.txt docker-compose.yml .gitattributes .gitignore gerador/__init__.py tests/gerador/__init__.py
git commit -m "Infra: pytest, requirements, floci e export-ignore do pacote do aluno"
```

---

### Task 2: Catálogo e contrato do evento

**Files:**
- Create: `gerador/catalogo.py`, `gerador/eventos.py`
- Test: `tests/gerador/test_eventos.py`

- [ ] **Step 1: Escrever o teste que falha**

`tests/gerador/test_eventos.py`:

```python
import random
from datetime import datetime, timezone

from gerador import catalogo as cat
from gerador.eventos import Contexto, criar_evento, iso, item, novo_uuid


def _ctx():
    return Contexto("anon-1", cat.CANAIS[1], cat.DISPOSITIVOS[0], "s-1", pagina="landing")


def test_iso_tem_milissegundos_e_z():
    ts = datetime(2026, 9, 1, 14, 22, 5, 123456, tzinfo=timezone.utc)
    assert iso(ts) == "2026-09-01T14:22:05.123Z"


def test_uuid_e_deterministico_pela_seed():
    assert novo_uuid(random.Random(1)) == novo_uuid(random.Random(1))
    assert len(novo_uuid(random.Random(1))) == 36


def test_criar_evento_segue_o_contrato():
    ts = datetime(2026, 9, 1, tzinfo=timezone.utc)
    ev = criar_evento(random.Random(1), "purchase", ts, _ctx(), {"value": 1.0}, [item(cat.PLANOS[0])])
    assert set(ev) == {
        "event_id", "event_name", "event_ts", "user_pseudo_id", "user_id", "session_hint",
        "page", "traffic", "device", "params", "items",
    }
    assert ev["event_ts"] == "2026-09-01T00:00:00.000Z"
    assert ev["user_id"] is None
    assert ev["page"] == {"location": "https://loja.exemplo/", "referrer": None}
    assert ev["traffic"] == {"source": "google", "medium": "cpc", "campaign": "formacao_dados"}
    assert ev["device"] == {"category": "mobile", "os": "Android", "browser": "Chrome"}
    assert ev["items"] == [{
        "item_id": "PLANO-BASICO", "item_name": "Plano Básico", "item_category": "plano",
        "price": 297.0, "quantity": 1,
    }]


def test_item_aceita_campos_da_lista():
    it = item(cat.PLANOS[1], index=1, **cat.LISTA)
    assert it["index"] == 1
    assert it["item_list_id"] == "tabela_planos"


def test_catalogo_tem_os_22_nomes_de_evento():
    assert len(cat.EVENTOS) == 22
    assert len(set(cat.EVENTOS)) == 22
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_eventos.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'gerador.catalogo'`

- [ ] **Step 3: Criar `gerador/catalogo.py`**

```python
"""Catálogo da landing hipotética DadosPro.

Tudo o que é regra de negócio do gerador mora aqui: eventos, produtos,
preços, canais, dispositivos e textos da página. jornadas.py só descreve
o comportamento do visitante.

Os "20 eventos" do enunciado são 22 nomes, porque o engajamento de mídia
conta como um item só e tem três nomes (start, progress, complete).
"""

from __future__ import annotations

EVENTOS = (
    "page_view", "scroll", "click", "site_search",
    "video_start", "video_progress", "video_complete", "file_download",
    "view_item_list", "select_item", "view_item", "add_to_cart", "remove_from_cart",
    "view_cart", "begin_checkout", "add_shipping_info", "add_payment_info",
    "purchase", "refund",
    "generate_lead", "sign_up", "login",
)

PLANOS = (
    {"item_id": "PLANO-BASICO", "item_name": "Plano Básico", "item_category": "plano", "price": 297.0},
    {"item_id": "PLANO-PRO", "item_name": "Plano Pro", "item_category": "plano", "price": 597.0},
    {"item_id": "PLANO-PREMIUM", "item_name": "Plano Premium", "item_category": "plano", "price": 997.0},
)
PESOS_PLANO = (0.35, 0.45, 0.20)
PREMIUM = "PLANO-PREMIUM"
BUMP = {"item_id": "BUMP-MENTORIA", "item_name": "Mentoria em grupo", "item_category": "order_bump", "price": 147.0}

FRETE = {"PAC": 24.90, "SEDEX": 49.90}
PESOS_FRETE = (0.6, 0.4)
ALIQUOTA_ISS = 0.05
DESCONTO_PIX = 0.05
MOEDA = "BRL"
PAGAMENTOS = ("pix", "cartao_avista", "cartao_parcelado")
PESOS_PAGAMENTO = (0.40, 0.20, 0.40)

LISTA = {"item_list_id": "tabela_planos", "item_list_name": "Tabela de planos"}

# peso: participação no tráfego; fator_compra: multiplica a propensão de compra
CANAIS = (
    {"source": "direct", "medium": "(none)", "campaign": None,
     "referrer": None, "peso": 0.20, "fator_compra": 1.0},
    {"source": "google", "medium": "cpc", "campaign": "formacao_dados",
     "referrer": "https://www.google.com/", "peso": 0.30, "fator_compra": 1.2},
    {"source": "google", "medium": "organic", "campaign": None,
     "referrer": "https://www.google.com/", "peso": 0.20, "fator_compra": 1.0},
    {"source": "instagram", "medium": "paid_social", "campaign": "lancamento_nov",
     "referrer": "https://l.instagram.com/", "peso": 0.25, "fator_compra": 0.6},
    {"source": "newsletter", "medium": "email", "campaign": "ebook_nutricao",
     "referrer": None, "peso": 0.05, "fator_compra": 1.5},
)
NEWSLETTER = CANAIS[4]

DISPOSITIVOS = (
    {"category": "mobile", "os": "Android", "browser": "Chrome", "peso": 0.38, "fator_compra": 0.8, "fator_scroll": 1.1},
    {"category": "mobile", "os": "iOS", "browser": "Safari", "peso": 0.22, "fator_compra": 0.9, "fator_scroll": 1.1},
    {"category": "desktop", "os": "Windows", "browser": "Chrome", "peso": 0.25, "fator_compra": 1.15, "fator_scroll": 0.95},
    {"category": "desktop", "os": "macOS", "browser": "Safari", "peso": 0.10, "fator_compra": 1.2, "fator_scroll": 0.95},
    {"category": "tablet", "os": "iOS", "browser": "Safari", "peso": 0.05, "fator_compra": 1.0, "fator_scroll": 1.0},
)

VIDEO = {"video_title": "VSL Formação em Análise de Dados", "video_provider": "self_hosted", "video_duration": 240}
RETENCAO_VIDEO = ((10, 0.90), (25, 0.80), (50, 0.70), (75, 0.75))   # chance de passar do marco anterior a este
P_VIDEO_COMPLETO = 0.80

# termo -> quantidade de respostas no FAQ (0 = lacuna de conteúdo)
BUSCAS_FAQ = (
    ("certificado", 1), ("parcelar", 1), ("reembolso", 1), ("apostila", 1), ("acesso", 1),
    ("linux", 0), ("presencial", 0), ("bolsa", 0), ("estagio", 0), ("power bi", 0),
)
SAIDAS = (
    ("Instagram", "https://instagram.com/dadospro"),
    ("YouTube", "https://youtube.com/@dadospro"),
    ("WhatsApp", "https://wa.me/5548999999999"),
    ("LinkedIn", "https://linkedin.com/company/dadospro"),
)
EMENTA = {
    "file_name": "ementa-formacao-dados.pdf",
    "file_extension": "pdf",
    "link_url": "https://loja.exemplo/arquivos/ementa-formacao-dados.pdf",
}
LEAD_SOURCE = "ebook_primeiro_pipeline"

BASE_URL = "https://loja.exemplo"
PAGINAS = {
    "landing": BASE_URL + "/",
    "checkout": BASE_URL + "/checkout",
    "obrigado": BASE_URL + "/obrigado",
    "membros": BASE_URL + "/membros",
}
TITULO = "Formação em Análise de Dados"

# peso de cada hora do dia (UTC) no início de uma sessão
PESOS_HORA = (1, 1, 1, 1, 1, 1, 2, 3, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 8, 9, 9, 7, 4)
```

- [ ] **Step 4: Criar `gerador/eventos.py`**

```python
"""O contrato do evento (spec, seção 5) e utilitários deterministas."""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from gerador.catalogo import PAGINAS


@dataclass
class Contexto:
    """Estado do navegador do visitante no instante em que um evento é emitido."""

    user_pseudo_id: str
    canal: dict
    dispositivo: dict
    session_hint: str
    user_id: str | None = None
    pagina: str | None = None      # None antes do primeiro page_view da sessão
    referrer: str | None = None


def hex8(rng: random.Random) -> str:
    return f"{rng.getrandbits(32):08x}"


def novo_uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def iso(ts: datetime) -> str:
    ts = ts.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def ler_iso(texto: str) -> datetime:
    return datetime.fromisoformat(texto.replace("Z", "+00:00"))


def item(produto: dict, **extra) -> dict:
    return {
        "item_id": produto["item_id"],
        "item_name": produto["item_name"],
        "item_category": produto["item_category"],
        "price": produto["price"],
        "quantity": 1,
        **extra,
    }


def criar_evento(rng: random.Random, nome: str, ts: datetime, ctx: Contexto,
                 params: dict | None = None, items: list | None = None) -> dict:
    return {
        "event_id": novo_uuid(rng),
        "event_name": nome,
        "event_ts": iso(ts),
        "user_pseudo_id": ctx.user_pseudo_id,
        "user_id": ctx.user_id,
        "session_hint": ctx.session_hint,
        "page": {"location": PAGINAS[ctx.pagina], "referrer": ctx.referrer},
        "traffic": {
            "source": ctx.canal["source"],
            "medium": ctx.canal["medium"],
            "campaign": ctx.canal["campaign"],
        },
        "device": {
            "category": ctx.dispositivo["category"],
            "os": ctx.dispositivo["os"],
            "browser": ctx.dispositivo["browser"],
        },
        "params": dict(params or {}),
        "items": [dict(i) for i in (items or [])],
    }
```

- [ ] **Step 5: Rodar e ver passar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_eventos.py -v`
Expected: 5 passed

- [ ] **Step 6: Commit**

```powershell
git add gerador/catalogo.py gerador/eventos.py tests/gerador/test_eventos.py
git commit -m "Gerador: catalogo da landing e contrato do evento"
```

---

### Task 3: Jornadas — sessão, visitante e `simular()`

**Files:**
- Create: `gerador/jornadas.py`, `tests/gerador/conftest.py`
- Test: `tests/gerador/test_jornadas.py`

- [ ] **Step 1: Criar a fixture compartilhada**

`tests/gerador/conftest.py`:

```python
from datetime import datetime, timezone

import pytest

from gerador.jornadas import simular

DATA_INICIAL = datetime(2026, 9, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def simulacao():
    """1500 visitantes em 14 dias, sem sujeira. Devolve (eventos, sessoes)."""
    eventos, sessoes, _ = simular(seed=7, dias=14, visitantes=1500, data_inicial=DATA_INICIAL)
    return eventos, sessoes
```

- [ ] **Step 2: Escrever os testes que falham**

`tests/gerador/test_jornadas.py`:

```python
from collections import defaultdict
from datetime import timedelta

from gerador import catalogo as cat
from gerador.eventos import ler_iso


def _por_pseudo(eventos):
    grupos = defaultdict(list)
    for e in eventos:
        grupos[e["user_pseudo_id"]].append(e)
    for evs in grupos.values():
        evs.sort(key=lambda e: e["event_ts"])
    return grupos


def test_todos_os_eventos_do_catalogo_aparecem(simulacao):
    eventos, _ = simulacao
    assert {e["event_name"] for e in eventos} == set(cat.EVENTOS)


def test_toda_jornada_comeca_com_page_view(simulacao):
    for evs in _por_pseudo(simulacao[0]).values():
        assert evs[0]["event_name"] == "page_view"


def test_frete_so_em_carrinho_premium(simulacao):
    eventos, _ = simulacao
    fretes = [e for e in eventos if e["event_name"] == "add_shipping_info"]
    assert fretes
    for e in fretes:
        assert any(i["item_id"] == cat.PREMIUM for i in e["items"])
    for e in eventos:
        if e["event_name"] == "purchase" and e["params"]["shipping"] > 0:
            assert any(i["item_id"] == cat.PREMIUM for i in e["items"])


def test_compra_tem_id_valor_e_imposto_coerentes(simulacao):
    compras = [e for e in simulacao[0] if e["event_name"] == "purchase"]
    assert compras
    for e in compras:
        p = e["params"]
        assert p["transaction_id"].startswith("T-")
        bruto = round(sum(i["price"] * i["quantity"] for i in e["items"]), 2)
        esperado = round(bruto - round(bruto * 0.05, 2), 2) if p["payment_type"] == "pix" else bruto
        assert p["value"] == esperado
        assert p["tax"] == round(p["value"] * 0.05, 2)
        assert p["currency"] == "BRL"
        assert p["coupon"] == ("PIX5" if p["payment_type"] == "pix" else None)


def test_reembolso_referencia_compra_de_1_a_7_dias_antes(simulacao):
    eventos, _ = simulacao
    compras = {e["params"]["transaction_id"]: e for e in eventos if e["event_name"] == "purchase"}
    reembolsos = [e for e in eventos if e["event_name"] == "refund"]
    assert reembolsos
    for r in reembolsos:
        c = compras[r["params"]["transaction_id"]]
        assert timedelta(days=1) <= ler_iso(r["event_ts"]) - ler_iso(c["event_ts"]) <= timedelta(days=7)
        assert r["params"]["value"] == c["params"]["value"]
        assert r["items"] == c["items"]


def test_session_hint_muda_mais_vezes_que_as_sessoes(simulacao):
    eventos, sessoes = simulacao
    assert len({e["session_hint"] for e in eventos}) > sessoes


def test_user_id_so_aparece_a_partir_do_sign_up(simulacao):
    casos = 0
    for evs in _por_pseudo(simulacao[0]).values():
        logados = [i for i, e in enumerate(evs) if e["user_id"]]
        if not logados:
            continue
        casos += 1
        primeiro = logados[0]
        assert evs[primeiro]["event_name"] == "sign_up"
        assert all(e["user_id"] is None for e in evs[:primeiro])
    assert casos > 0


def test_login_so_depois_de_sign_up_do_mesmo_user_id(simulacao):
    logins = 0
    for evs in _por_pseudo(simulacao[0]).values():
        for i, e in enumerate(evs):
            if e["event_name"] == "login":
                logins += 1
                assert any(a["event_name"] == "sign_up" and a["user_id"] == e["user_id"] for a in evs[:i])
    assert logins > 0


def test_existe_lead_que_volta_outro_dia_e_compra(simulacao):
    achou = False
    for evs in _por_pseudo(simulacao[0]).values():
        leads = [e for e in evs if e["event_name"] == "generate_lead"]
        compras = [e for e in evs if e["event_name"] == "purchase"]
        if leads and compras and compras[0]["event_ts"][:10] > leads[0]["event_ts"][:10]:
            achou = True
            break
    assert achou


def test_order_bump_e_troca_de_plano_geram_remove_from_cart(simulacao):
    removidos = {e["items"][0]["item_category"] for e in simulacao[0] if e["event_name"] == "remove_from_cart"}
    assert removidos == {"plano", "order_bump"}


def test_marcos_de_scroll_e_video_sao_os_do_contrato(simulacao):
    for e in simulacao[0]:
        if e["event_name"] == "scroll":
            assert e["params"]["percent_scrolled"] in (25, 50, 75, 90)
        elif e["event_name"] == "video_start":
            assert e["params"]["video_percent"] == 0
        elif e["event_name"] == "video_progress":
            assert e["params"]["video_percent"] in (10, 25, 50, 75)
        elif e["event_name"] == "video_complete":
            assert e["params"]["video_percent"] == 100


def test_busca_sem_resultado_existe(simulacao):
    buscas = [e for e in simulacao[0] if e["event_name"] == "site_search"]
    assert any(e["params"]["search_results"] == 0 for e in buscas)
    assert any(e["params"]["search_results"] > 0 for e in buscas)


def test_simular_e_deterministico():
    from datetime import datetime, timezone

    from gerador.jornadas import simular

    data = datetime(2026, 9, 1, tzinfo=timezone.utc)
    a, _, _ = simular(seed=3, dias=3, visitantes=50, data_inicial=data)
    b, _, _ = simular(seed=3, dias=3, visitantes=50, data_inicial=data)
    assert a == b
```

- [ ] **Step 3: Rodar e ver falhar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_jornadas.py -v`
Expected: ERROR com `ModuleNotFoundError: No module named 'gerador.jornadas'`

- [ ] **Step 4: Criar `gerador/jornadas.py`**

```python
"""Máquina de estados da jornada na landing DadosPro.

Uma sessão percorre a página na ordem das seções: hero → vídeo → módulos →
tabela de planos → e-book → FAQ → rodapé. Cada passo é uma chance; canal e
dispositivo mudam as chances. Um visitante tem de uma a quatro sessões em
dias diferentes; quem virou lead volta com mais intenção de compra, e quem
comprou volta para a área de membros.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from urllib.parse import urlparse

from gerador import catalogo as cat
from gerador.eventos import Contexto, criar_evento, hex8, item, ler_iso

P_RECARGA = 0.25          # chance de um page_view ser recarga completa (novo session_hint)
P_REEMBOLSO = 0.08


@dataclass
class Sessao:
    rng: random.Random
    ctx: Contexto
    agora: datetime
    eventos: list[dict] = field(default_factory=list)

    def chance(self, p: float) -> bool:
        return self.rng.random() < min(p, 0.98)

    def emitir(self, nome: str, params: dict | None = None, items: list | None = None,
               segundos: float | None = None) -> dict:
        self.agora += timedelta(seconds=segundos if segundos is not None else self.rng.uniform(2, 40))
        evento = criar_evento(self.rng, nome, self.agora, self.ctx, params, items)
        self.eventos.append(evento)
        return evento

    def abrir(self, pagina: str) -> dict:
        if self.ctx.pagina is None:
            self.ctx.referrer = self.ctx.canal["referrer"]
        else:
            self.ctx.referrer = cat.PAGINAS[self.ctx.pagina]
            if self.chance(P_RECARGA):
                self.ctx.session_hint = "s-" + hex8(self.rng)
        self.ctx.pagina = pagina
        return self.emitir("page_view", {"page_title": f"{cat.TITULO} · {pagina}"})


@dataclass
class ResultadoSessao:
    eventos: list[dict]
    compra: dict | None
    fez_lead: bool


def _valor(carrinho: list[dict]) -> float:
    return round(sum(i["price"] * i["quantity"] for i in carrinho), 2)


def _monetario(carrinho: list[dict], **extra) -> dict:
    return {"currency": cat.MOEDA, "value": _valor(carrinho), **extra}


def _video(s: Sessao) -> None:
    base = dict(cat.VIDEO)
    duracao = base["video_duration"]
    s.emitir("video_start", {**base, "video_current_time": 0, "video_percent": 0})
    anterior = 0
    for marco, p in cat.RETENCAO_VIDEO:
        if not s.chance(p):
            return
        s.emitir("video_progress", {**base, "video_current_time": duracao * marco // 100, "video_percent": marco},
                 segundos=duracao * (marco - anterior) / 100)
        anterior = marco
    if s.chance(cat.P_VIDEO_COMPLETO):
        s.emitir("video_complete", {**base, "video_current_time": duracao, "video_percent": 100},
                 segundos=duracao * (100 - anterior) / 100)


def _funil_compra(s: Sessao, fator: float) -> tuple[dict | None, bool]:
    """Da tabela de planos até a compra. Devolve (purchase ou None, saiu_da_landing)."""
    rng = s.rng
    if not s.chance(0.35 * fator):
        return None, False
    idx = rng.choices(range(len(cat.PLANOS)), weights=cat.PESOS_PLANO)[0]
    plano = cat.PLANOS[idx]
    s.emitir("select_item", dict(cat.LISTA), [item(plano, index=idx, **cat.LISTA)])
    if s.chance(0.60):
        s.emitir("view_item", {"currency": cat.MOEDA, "value": plano["price"]}, [item(plano)])
    if not s.chance(0.75):
        return None, False

    s.emitir("add_to_cart", {"currency": cat.MOEDA, "value": plano["price"]}, [item(plano)])
    if s.chance(0.10):
        outro = rng.choice([p for p in cat.PLANOS if p is not plano])
        s.emitir("remove_from_cart", {"currency": cat.MOEDA, "value": plano["price"]}, [item(plano)])
        s.emitir("add_to_cart", {"currency": cat.MOEDA, "value": outro["price"]}, [item(outro)])
        plano = outro
    carrinho = [item(plano)]

    s.abrir("checkout")
    s.emitir("view_cart", _monetario(carrinho), list(carrinho))
    if s.chance(0.25):
        carrinho.append(item(cat.BUMP))
        s.emitir("add_to_cart", {"currency": cat.MOEDA, "value": cat.BUMP["price"]}, [item(cat.BUMP)])
        if s.chance(0.30):
            carrinho.pop()
            s.emitir("remove_from_cart", {"currency": cat.MOEDA, "value": cat.BUMP["price"]}, [item(cat.BUMP)])
    if not s.chance(0.70):
        return None, True
    s.emitir("begin_checkout", _monetario(carrinho), list(carrinho))

    frete = 0.0
    if plano["item_id"] == cat.PREMIUM:
        if not s.chance(0.90):
            return None, True
        tier = rng.choices(tuple(cat.FRETE), weights=cat.PESOS_FRETE)[0]
        frete = cat.FRETE[tier]
        s.emitir("add_shipping_info", _monetario(carrinho, shipping_tier=tier, shipping=frete), list(carrinho))

    if not s.chance(0.75 * fator ** 0.5):
        return None, True
    tipo = rng.choices(cat.PAGAMENTOS, weights=cat.PESOS_PAGAMENTO)[0]
    s.emitir("add_payment_info", _monetario(carrinho, payment_type=tipo), list(carrinho))
    if not s.chance(0.85):
        return None, True

    bruto = _valor(carrinho)
    valor = round(bruto - round(bruto * cat.DESCONTO_PIX, 2), 2) if tipo == "pix" else bruto
    compra = s.emitir("purchase", {
        "transaction_id": "T-" + hex8(rng).upper(),
        "value": valor,
        "tax": round(valor * cat.ALIQUOTA_ISS, 2),
        "shipping": frete,
        "currency": cat.MOEDA,
        "payment_type": tipo,
        "coupon": "PIX5" if tipo == "pix" else None,
    }, list(carrinho))
    s.abrir("obrigado")
    if s.chance(0.60):
        s.abrir("membros")
        s.ctx.user_id = "u-" + hex8(rng)
        s.emitir("sign_up", {"method": "email"})
    return compra, True


def simular_sessao(rng: random.Random, ctx: Contexto, inicio: datetime,
                   intencao: float = 1.0, ja_e_lead: bool = False) -> ResultadoSessao:
    s = Sessao(rng, ctx, inicio)
    fs = ctx.dispositivo["fator_scroll"]
    fator = ctx.canal["fator_compra"] * ctx.dispositivo["fator_compra"] * intencao

    s.abrir("landing")
    if s.chance(0.30):
        s.emitir("click", {"link_url": cat.BASE_URL + "/#planos", "link_text": "Ver planos",
                           "link_id": "hero_ver_planos", "outbound": False})
    alcance = 0
    if s.chance(0.85 * fs):
        s.emitir("scroll", {"percent_scrolled": 25})
        alcance = 25
    if alcance >= 25 and s.chance(0.40):
        _video(s)
    if alcance >= 25 and s.chance(0.75 * fs):
        s.emitir("scroll", {"percent_scrolled": 50})
        alcance = 50
    if alcance >= 50 and s.chance(0.06):
        s.emitir("file_download", dict(cat.EMENTA))
    if alcance >= 50 and s.chance(0.70 * fs):
        s.emitir("scroll", {"percent_scrolled": 75})
        alcance = 75

    compra, saiu = None, False
    if alcance >= 50 and s.chance(0.90):
        s.emitir("view_item_list", dict(cat.LISTA),
                 [item(p, index=i, **cat.LISTA) for i, p in enumerate(cat.PLANOS)])
        compra, saiu = _funil_compra(s, fator)

    fez_lead = False
    if not saiu:
        if alcance >= 50 and not ja_e_lead and s.chance(0.12):
            s.emitir("generate_lead", {"lead_source": cat.LEAD_SOURCE, "currency": cat.MOEDA, "value": 0})
            fez_lead = True
        if alcance >= 75 and s.chance(0.65 * fs):
            s.emitir("scroll", {"percent_scrolled": 90})
            alcance = 90
        if alcance >= 75 and s.chance(0.10):
            termo, resultados = rng.choice(cat.BUSCAS_FAQ)
            s.emitir("site_search", {"search_term": termo, "search_results": resultados, "search_area": "faq"})
        if s.chance(0.05):
            texto, url = rng.choice(cat.SAIDAS)
            s.emitir("click", {"link_url": url, "link_domain": urlparse(url).hostname,
                               "link_text": texto, "outbound": True})
    return ResultadoSessao(s.eventos, compra, fez_lead)


def sessao_membros(rng: random.Random, ctx: Contexto, inicio: datetime, evento: str,
                   novo_user_id: str | None = None) -> list[dict]:
    """Área de membros. No sign_up, o user_id só passa a valer depois do page_view."""
    s = Sessao(rng, ctx, inicio)
    s.abrir("membros")
    if novo_user_id is not None:
        ctx.user_id = novo_user_id
    s.emitir(evento, {"method": "email"})
    return s.eventos


def sessao_reembolso(rng: random.Random, ctx: Contexto, inicio: datetime, compra: dict) -> list[dict]:
    s = Sessao(rng, ctx, inicio)
    s.abrir("obrigado")
    p = compra["params"]
    s.emitir("refund", {
        "transaction_id": p["transaction_id"], "value": p["value"], "tax": p["tax"],
        "shipping": p["shipping"], "currency": p["currency"], "reason": "garantia_7_dias",
    }, compra["items"])
    return s.eventos


def _sortear(rng: random.Random, opcoes: tuple[dict, ...]) -> dict:
    return rng.choices(opcoes, weights=[o["peso"] for o in opcoes])[0]


def _inicio(rng: random.Random, data_inicial: datetime, dia: int) -> datetime:
    hora = rng.choices(range(24), weights=cat.PESOS_HORA)[0]
    return data_inicial + timedelta(days=dia, hours=hora, seconds=rng.uniform(0, 3599))


def simular_visitante(rng: random.Random, data_inicial: datetime, dias: int) -> tuple[list[dict], int]:
    """Devolve (eventos do visitante, número de sessões simuladas)."""
    pseudo = "anon-" + hex8(rng)
    dispositivo = _sortear(rng, cat.DISPOSITIVOS)
    canal = _sortear(rng, cat.CANAIS)
    n = rng.choices((1, 2, 3, 4), weights=(65, 25, 7, 3))[0]
    dias_sessao = sorted(rng.sample(range(dias), k=min(n, dias)))
    fim = data_inicial + timedelta(days=dias)

    eventos: list[dict] = []
    sessoes = 0
    user_id = None
    e_lead = False
    compra = None
    for k, dia in enumerate(dias_sessao):
        if k > 0:
            canal = cat.NEWSLETTER if e_lead and rng.random() < 0.6 else _sortear(rng, cat.CANAIS)
        ctx = Contexto(pseudo, canal, dispositivo, "s-" + hex8(rng), user_id)
        inicio = _inicio(rng, data_inicial, dia)
        sessoes += 1

        if compra is not None:
            if user_id:
                eventos += sessao_membros(rng, ctx, inicio, "login")
            else:
                eventos += sessao_membros(rng, ctx, inicio, "sign_up", novo_user_id="u-" + hex8(rng))
            user_id = ctx.user_id
            continue

        resultado = simular_sessao(rng, ctx, inicio, intencao=1.6 if e_lead else 1.0, ja_e_lead=e_lead)
        eventos += resultado.eventos
        user_id = ctx.user_id
        e_lead = e_lead or resultado.fez_lead
        if resultado.compra is not None:
            compra = resultado.compra
            if rng.random() < P_REEMBOLSO:
                quando = ler_iso(compra["event_ts"]) + timedelta(days=rng.uniform(1, 6.9))
                if quando < fim:
                    ctx_r = Contexto(pseudo, canal, dispositivo, "s-" + hex8(rng), user_id)
                    eventos += sessao_reembolso(rng, ctx_r, quando, compra)
                    sessoes += 1
    return eventos, sessoes


def simular(seed: int, dias: int, visitantes: int, data_inicial: datetime
            ) -> tuple[list[dict], int, random.Random]:
    """Simula todos os visitantes. Devolve (eventos, sessões, rng) — o rng segue para a sujeira e os lotes."""
    rng = random.Random(seed)
    eventos: list[dict] = []
    sessoes = 0
    for _ in range(visitantes):
        evs, n = simular_visitante(rng, data_inicial, dias)
        eventos += evs
        sessoes += n
    return eventos, sessoes, rng
```

A sessão de reembolso começa entre 1 e 6,9 dias depois da compra. Com isso, o `refund` (emitido segundos após o `page_view`) fica dentro de 7 dias, como o teste exige.

- [ ] **Step 5: Rodar e ver passar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_jornadas.py -v`
Expected: 13 passed

Se um teste estatístico falhar (por exemplo, nenhum lead que volta e compra), **aumente `visitantes` na fixture** para 2500. Não mexa nas probabilidades do catálogo para fazer o teste passar.

- [ ] **Step 6: Commit**

```powershell
git add gerador/jornadas.py tests/gerador/conftest.py tests/gerador/test_jornadas.py
git commit -m "Gerador: maquina de estados da jornada na landing"
```

---

### Task 4: Sujeira injetada

**Files:**
- Create: `gerador/sujeira.py`
- Test: `tests/gerador/test_sujeira.py`

- [ ] **Step 1: Escrever os testes que falham**

`tests/gerador/test_sujeira.py`:

```python
import copy
import random

from gerador.sujeira import TIPOS, aplicar_sujeira


def test_total_de_defeitos_respeita_a_fracao_e_todos_os_tipos_aparecem(simulacao):
    eventos = copy.deepcopy(simulacao[0])
    n = len(eventos)
    contagem = aplicar_sujeira(random.Random(1), eventos, 0.03)
    assert set(contagem) == set(TIPOS)
    assert sum(contagem.values()) == round(n * 0.03)
    assert all(contagem[t] > 0 for t in TIPOS)
    assert len(eventos) == n + contagem["refund_orfao"] + contagem["evento_fora_catalogo"]


def test_compras_sem_id_ficam_limitadas_a_5_por_cento(simulacao):
    eventos = copy.deepcopy(simulacao[0])
    compras = sum(1 for e in eventos if e["event_name"] == "purchase")
    contagem = aplicar_sujeira(random.Random(1), eventos, 0.03)
    assert contagem["purchase_sem_id"] <= -(-compras * 5 // 100)


def test_cada_defeito_e_visivel_no_dado(simulacao):
    eventos = copy.deepcopy(simulacao[0])
    ids_compra = {e["params"]["transaction_id"] for e in eventos if e["event_name"] == "purchase"}
    aplicar_sujeira(random.Random(1), eventos, 0.03)
    assert any(e["event_ts"] is None for e in eventos)
    assert any(e["event_ts"] is not None and e["event_ts"] > "2030" for e in eventos)
    assert any(e["event_name"] == "purchase" and e["params"]["transaction_id"] is None for e in eventos)
    assert any(isinstance(e["params"].get("value"), float) and e["params"]["value"] < 0 for e in eventos)
    assert any(e["params"].get("currency") == "USD" for e in eventos)
    assert any(e["event_name"] == "scroll" and e["params"]["percent_scrolled"] == 33 for e in eventos)
    assert any(e["event_name"] == "add_to_wishlist" for e in eventos)
    assert any(e["event_name"] == "refund" and e["params"]["transaction_id"] not in ids_compra for e in eventos)


def test_um_evento_recebe_no_maximo_um_defeito(simulacao):
    eventos = copy.deepcopy(simulacao[0])
    aplicar_sujeira(random.Random(1), eventos, 0.03)
    for e in eventos:
        defeitos = [
            e["event_ts"] is None,
            e["event_ts"] is not None and e["event_ts"] > "2030",
            e["params"].get("currency") == "USD",
            isinstance(e["params"].get("value"), float) and e["params"]["value"] < 0,
        ]
        assert sum(defeitos) <= 1


def test_fracao_zero_nao_altera_nada(simulacao):
    eventos = copy.deepcopy(simulacao[0])
    contagem = aplicar_sujeira(random.Random(1), eventos, 0.0)
    assert sum(contagem.values()) == 0
    assert eventos == simulacao[0]
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_sujeira.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'gerador.sujeira'`

- [ ] **Step 3: Criar `gerador/sujeira.py`**

```python
"""Defeitos injetados no dado, para a Silver encontrar.

Cada tipo recebe uma cota proporcional ao seu peso. Os defeitos que
alteram um evento existente atingem no máximo 5% dos candidatos daquele
tipo, para não destruir a receita (por exemplo, tirar o id de metade das
compras). O que sobra das cotas vai para "ts_futuro", que tem candidatos
de sobra. Um evento recebe no máximo um defeito.
"""

from __future__ import annotations

import copy
import itertools
import math
import random
from datetime import timedelta

from gerador.eventos import hex8, iso, ler_iso, novo_uuid

PESOS = {
    "purchase_sem_id": 1,
    "valor_negativo": 2,
    "moeda_usd": 2,
    "scroll_33": 2,
    "ts_nulo": 2,
    "ts_futuro": 2,
    "refund_orfao": 1,
    "evento_fora_catalogo": 2,
}
TIPOS = tuple(PESOS)
TETO = 0.05


def _monetario_positivo(e: dict) -> bool:
    v = e["params"].get("value")
    return isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0


FILTROS = {
    "purchase_sem_id": lambda e: e["event_name"] == "purchase",
    "valor_negativo": _monetario_positivo,
    "moeda_usd": lambda e: "currency" in e["params"],
    "scroll_33": lambda e: e["event_name"] == "scroll",
    "ts_nulo": lambda e: e["event_ts"] is not None,
    "ts_futuro": lambda e: e["event_ts"] is not None,
}


def _sem_id(e: dict) -> None:
    e["params"]["transaction_id"] = None


def _negativo(e: dict) -> None:
    e["params"]["value"] = -float(e["params"]["value"])


def _usd(e: dict) -> None:
    e["params"]["currency"] = "USD"


def _scroll_33(e: dict) -> None:
    e["params"]["percent_scrolled"] = 33


def _ts_nulo(e: dict) -> None:
    e["event_ts"] = None


def _ts_futuro(e: dict) -> None:
    e["event_ts"] = iso(ler_iso(e["event_ts"]) + timedelta(days=3650))


# ts_futuro por último: absorve o que os outros tipos não conseguiram usar
MUTACOES = {
    "purchase_sem_id": _sem_id,
    "valor_negativo": _negativo,
    "moeda_usd": _usd,
    "scroll_33": _scroll_33,
    "ts_nulo": _ts_nulo,
    "ts_futuro": _ts_futuro,
}


def _clonar(rng: random.Random, base: dict, nome: str, params: dict) -> dict:
    novo = copy.deepcopy(base)
    novo.update(event_id=novo_uuid(rng), event_name=nome, params=params, items=[])
    return novo


def _refund_orfao(rng: random.Random, base: dict) -> dict:
    return _clonar(rng, base, "refund", {
        "transaction_id": "T-" + hex8(rng).upper(), "value": 597.0, "tax": 29.85,
        "shipping": 0.0, "currency": "BRL", "reason": "garantia_7_dias",
    })


def _fora_do_catalogo(rng: random.Random, base: dict) -> dict:
    return _clonar(rng, base, "add_to_wishlist", {})


INSERCOES = {"refund_orfao": _refund_orfao, "evento_fora_catalogo": _fora_do_catalogo}


def _cotas(alvo: int) -> dict[str, int]:
    total = sum(PESOS.values())
    cotas = {t: alvo * p // total for t, p in PESOS.items()}
    for t in itertools.islice(itertools.cycle(TIPOS), alvo - sum(cotas.values())):
        cotas[t] += 1
    return cotas


def aplicar_sujeira(rng: random.Random, eventos: list[dict], fracao: float) -> dict[str, int]:
    """Altera `eventos` no lugar e acrescenta os inseridos. Devolve a contagem por tipo."""
    originais = list(eventos)
    cotas = _cotas(round(len(originais) * fracao))
    contagem = dict.fromkeys(TIPOS, 0)
    usados: set[int] = set()
    sobra = 0

    for tipo, mutar in MUTACOES.items():
        candidatos = [e for e in originais if id(e) not in usados and FILTROS[tipo](e)]
        rng.shuffle(candidatos)
        quero = cotas[tipo] + (sobra if tipo == "ts_futuro" else 0)
        teto = len(candidatos) if tipo == "ts_futuro" else math.ceil(len(candidatos) * TETO)
        n = min(quero, teto)
        if tipo != "ts_futuro":
            sobra += quero - n
        for e in candidatos[:n]:
            mutar(e)
            usados.add(id(e))
        contagem[tipo] = n

    limpos = [e for e in originais if id(e) not in usados]
    for tipo, inserir in INSERCOES.items():
        for _ in range(cotas[tipo]):
            eventos.append(inserir(rng, rng.choice(limpos)))
        contagem[tipo] = cotas[tipo]
    return contagem
```

- [ ] **Step 4: Rodar e ver passar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_sujeira.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```powershell
git add gerador/sujeira.py tests/gerador/test_sujeira.py
git commit -m "Gerador: defeitos injetados com cotas e teto por tipo"
```

---

### Task 5: Lotes do coletor

**Files:**
- Create: `gerador/lotes.py`
- Test: `tests/gerador/test_lotes.py`

- [ ] **Step 1: Escrever os testes que falham**

`tests/gerador/test_lotes.py`:

```python
import json
import random

from gerador.lotes import PREFIXO, montar_lotes, serializar


def _ev(i, dt="2026-09-01"):
    return {"event_id": f"id-{i}", "event_ts": f"{dt}T10:{i // 60 % 60:02d}:{i % 60:02d}.000Z",
            "event_name": "page_view", "_dt": dt}


def test_serializar_remove_campos_internos_e_gera_uma_linha_por_evento():
    texto = serializar([_ev(1), _ev(2)])
    linhas = texto.splitlines()
    assert len(linhas) == 2
    assert texto.endswith("\n")
    assert json.loads(linhas[0]) == {"event_id": "id-1", "event_ts": "2026-09-01T10:00:01.000Z",
                                     "event_name": "page_view"}


def test_serializar_mantem_acentos():
    assert "Básico" in serializar([{"event_id": "x", "nome": "Plano Básico"}])


def test_cada_dia_vira_de_3_a_6_lotes_com_a_chave_do_contrato():
    eventos = [_ev(i) for i in range(300)] + [_ev(i, "2026-09-02") for i in range(300, 600)]
    arquivos, duplicatas = montar_lotes(random.Random(3), eventos, origem="coletor", seed=42, fracao_reenvio=0.0)
    assert duplicatas == 0
    for dt in ("2026-09-01", "2026-09-02"):
        chaves = [k for k in arquivos if k.startswith(f"{PREFIXO}/dt={dt}/coletor-s42-")]
        assert 3 <= len(chaves) <= 6
        assert all(k.endswith(".jsonl") for k in chaves)
    assert sum(len(v.splitlines()) for v in arquivos.values()) == 600


def test_reenvio_repete_event_id_em_outro_lote_nunca_no_mesmo():
    eventos = [_ev(i) for i in range(1000)]
    arquivos, duplicatas = montar_lotes(random.Random(3), eventos, "coletor", 42, fracao_reenvio=0.05)
    assert duplicatas > 0
    ids = [json.loads(linha)["event_id"] for v in arquivos.values() for linha in v.splitlines()]
    assert len(ids) == 1000 + duplicatas
    assert len(set(ids)) == 1000
    for v in arquivos.values():
        do_lote = [json.loads(linha)["event_id"] for linha in v.splitlines()]
        assert len(do_lote) == len(set(do_lote))


def test_evento_sem_timestamp_vai_para_o_dia_de_coleta():
    sem_ts = {"event_id": "nulo", "event_ts": None, "event_name": "scroll", "_dt": "2026-09-01"}
    arquivos, _ = montar_lotes(random.Random(3), [_ev(1), _ev(2), _ev(3), sem_ts], "coletor", 42, 0.0)
    conteudo = "".join(v for k, v in arquivos.items() if "dt=2026-09-01" in k)
    assert '"event_id":"nulo"' in conteudo
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_lotes.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'gerador.lotes'`

- [ ] **Step 3: Criar `gerador/lotes.py`**

```python
"""Divide os eventos em lotes por dia, como um coletor que descarrega em intervalos.

O dia do lote (`_dt`) é o dia de coleta, gravado antes da sujeira, por isso
um evento com timestamp nulo ou no futuro ainda tem uma partição. Cerca de
1% dos eventos é reenviado no lote vizinho com o mesmo event_id; é a
duplicata que a Silver precisa remover.
"""

from __future__ import annotations

import json
import random
from collections import defaultdict

PREFIXO = "landing/eventos"


def serializar(eventos: list[dict]) -> str:
    return "".join(
        json.dumps({k: v for k, v in e.items() if not k.startswith("_")},
                   ensure_ascii=False, separators=(",", ":")) + "\n"
        for e in eventos
    )


def montar_lotes(rng: random.Random, eventos: list[dict], origem: str, seed: int,
                 fracao_reenvio: float = 0.01) -> tuple[dict[str, str], int]:
    """Devolve ({chave S3: conteúdo JSONL}, número de duplicatas por reenvio)."""
    por_dia: dict[str, list[dict]] = defaultdict(list)
    for e in eventos:
        por_dia[e["_dt"]].append(e)

    arquivos: dict[str, str] = {}
    duplicatas = 0
    for dt in sorted(por_dia):
        do_dia = sorted(por_dia[dt], key=lambda e: e["event_ts"] or "")
        n = min(rng.randint(3, 6), len(do_dia))
        lotes = [do_dia[i * len(do_dia) // n:(i + 1) * len(do_dia) // n] for i in range(n)]
        reenvios: list[list[dict]] = [[] for _ in lotes]
        if len(lotes) > 1:
            for i, lote in enumerate(lotes):
                destino = i + 1 if i + 1 < len(lotes) else i - 1
                for e in lote:
                    if rng.random() < fracao_reenvio:
                        reenvios[destino].append(e)
                        duplicatas += 1
        for i, (lote, extra) in enumerate(zip(lotes, reenvios), start=1):
            arquivos[f"{PREFIXO}/dt={dt}/{origem}-s{seed}-{i:03d}.jsonl"] = serializar(lote + extra)
    return arquivos, duplicatas
```

- [ ] **Step 4: Rodar e ver passar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_lotes.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```powershell
git add gerador/lotes.py tests/gerador/test_lotes.py
git commit -m "Gerador: lotes diarios do coletor com reenvio de ~1%"
```

---

### Task 6: Destinos (pasta local e S3)

**Files:**
- Create: `gerador/destinos.py`
- Test: `tests/gerador/test_destinos.py`

- [ ] **Step 1: Escrever os testes que falham**

`tests/gerador/test_destinos.py`:

```python
from gerador.destinos import BUCKET, DestinoLocal, DestinoS3


def test_destino_local_espelha_a_chave_em_pastas(tmp_path):
    destino = DestinoLocal(tmp_path)
    destino.gravar("landing/eventos/dt=2026-09-01/coletor-s42-001.jsonl", '{"a":"é"}\n')
    arquivo = tmp_path / "landing" / "eventos" / "dt=2026-09-01" / "coletor-s42-001.jsonl"
    assert arquivo.read_bytes() == '{"a":"é"}\n'.encode("utf-8")
    assert str(tmp_path.resolve()) in destino.descricao()


class S3Falso:
    def __init__(self):
        self.objetos = {}
        self.buckets = set()

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise LookupError(Bucket)

    def create_bucket(self, Bucket, **_):
        self.buckets.add(Bucket)

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objetos[(Bucket, Key)] = (Body, ContentType)


def test_destino_s3_cria_o_bucket_e_grava_utf8():
    s3 = S3Falso()
    destino = DestinoS3(cliente=s3)
    destino.gravar("landing/x.jsonl", "é\n")
    assert BUCKET in s3.buckets
    assert s3.objetos[(BUCKET, "landing/x.jsonl")] == ("é\n".encode("utf-8"), "application/x-ndjson")
    assert destino.descricao().startswith(f"s3://{BUCKET}")
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_destinos.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'gerador.destinos'`

- [ ] **Step 3: Criar `gerador/destinos.py`**

```python
"""Para onde o gerador grava: o S3 (floci, por padrão) ou uma pasta local.

Variáveis de ambiente, iguais às do pipeline:
  AWS_ENDPOINT_URL não definida -> floci em http://127.0.0.1:4566
  AWS_ENDPOINT_URL="" (vazia)   -> resolução padrão do boto3 (AWS real)
  AWS_S3_BUCKET                 -> bucket do datalake (padrão: datalake-tracking)
"""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

FLOCI_ENDPOINT = "http://127.0.0.1:4566"
_endpoint_env = os.environ.get("AWS_ENDPOINT_URL")
ENDPOINT = FLOCI_ENDPOINT if _endpoint_env is None else (_endpoint_env or None)
BUCKET = os.environ.get("AWS_S3_BUCKET", "datalake-tracking")


class DestinoLocal:
    def __init__(self, pasta: Path | str):
        self.pasta = Path(pasta)

    def gravar(self, chave: str, conteudo: str) -> None:
        caminho = self.pasta / chave
        caminho.parent.mkdir(parents=True, exist_ok=True)
        caminho.write_bytes(conteudo.encode("utf-8"))

    def descricao(self) -> str:
        return str(self.pasta.resolve())


def _cliente_boto3():
    try:
        import boto3
    except ImportError:
        raise SystemExit("boto3 não está instalado: pip install -r requirements.txt") from None
    return boto3.client("s3", endpoint_url=ENDPOINT)


class DestinoS3:
    def __init__(self, cliente=None):
        self.s3 = cliente if cliente is not None else _cliente_boto3()
        self._garantir_bucket()

    def _garantir_bucket(self) -> None:
        try:
            self.s3.head_bucket(Bucket=BUCKET)
            return
        except Exception as erro:  # ClientError (bucket ausente) ou falha de conexão
            if type(erro).__name__ == "EndpointConnectionError":
                raise SystemExit(
                    f"Não consegui falar com o S3 em {ENDPOINT}. "
                    "O floci está rodando? docker compose up -d --wait"
                ) from None
        argumentos = {"Bucket": BUCKET}
        regiao = os.environ["AWS_DEFAULT_REGION"]
        if regiao != "us-east-1":
            argumentos["CreateBucketConfiguration"] = {"LocationConstraint": regiao}
        self.s3.create_bucket(**argumentos)

    def gravar(self, chave: str, conteudo: str) -> None:
        self.s3.put_object(Bucket=BUCKET, Key=chave, Body=conteudo.encode("utf-8"),
                           ContentType="application/x-ndjson")

    def descricao(self) -> str:
        return f"s3://{BUCKET} ({ENDPOINT or 'AWS'})"
```

- [ ] **Step 4: Rodar e ver passar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_destinos.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```powershell
git add gerador/destinos.py tests/gerador/test_destinos.py
git commit -m "Gerador: destinos S3 (floci) e pasta local"
```

---

### Task 7: CLI `gerar_eventos.py`

**Files:**
- Create: `gerador/gerar_eventos.py`
- Test: `tests/gerador/test_gerar.py`

- [ ] **Step 1: Escrever os testes que falham**

`tests/gerador/test_gerar.py`:

```python
from datetime import datetime, timezone

import pytest

from gerador.gerar_eventos import gerar, main

DATA = datetime(2026, 9, 1, tzinfo=timezone.utc)


def _gerar(seed):
    return gerar(seed=seed, dias=3, visitantes=200, sujeira=0.03, data_inicial=DATA)


def test_mesma_seed_gera_os_mesmos_arquivos():
    assert _gerar(5)[0] == _gerar(5)[0]


def test_seeds_diferentes_geram_conteudo_diferente():
    assert set(_gerar(5)[0].values()) != set(_gerar(6)[0].values())


def test_resumo_bate_com_as_linhas_gravadas():
    arquivos, r = _gerar(5)
    linhas = sum(len(v.splitlines()) for v in arquivos.values())
    inseridos = r["sujeira"]["refund_orfao"] + r["sujeira"]["evento_fora_catalogo"]
    assert linhas == r["linhas_gravadas"] == r["eventos_limpos"] + inseridos + r["duplicatas_reenvio"]
    assert r["visitantes"] == 200
    assert r["arquivos"] == len(arquivos)
    assert sum(r["por_evento"].values()) == r["eventos_limpos"]
    assert r["periodo"] == "2026-09-01 a 2026-09-03"


def test_main_grava_na_pasta_local_e_imprime_resumo(tmp_path, capsys):
    assert main(["--dias", "2", "--visitantes", "50", "--saida-local", str(tmp_path)]) == 0
    assert list(tmp_path.glob("landing/eventos/dt=*/coletor-s42-*.jsonl"))
    saida = capsys.readouterr().out
    assert "eventos limpos" in saida
    assert str(tmp_path.resolve()) in saida


def test_main_recusa_fracao_de_sujeira_absurda(tmp_path):
    with pytest.raises(SystemExit):
        main(["--sujeira", "0.9", "--saida-local", str(tmp_path)])
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_gerar.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'gerador.gerar_eventos'`

- [ ] **Step 3: Criar `gerador/gerar_eventos.py`**

```python
"""Simula o coletor de tracking da landing DadosPro e grava na landing do datalake.

    python gerador/gerar_eventos.py                       # 3000 visitantes, 14 dias, no S3 (floci)
    python gerador/gerar_eventos.py --seed 7 --sujeira 0.05
    python gerador/gerar_eventos.py --saida-local saida   # numa pasta, sem S3

Não depende do pipeline nem do Spark. A mesma seed gera exatamente os mesmos arquivos.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gerador import catalogo as cat
from gerador.destinos import DestinoLocal, DestinoS3
from gerador.jornadas import simular
from gerador.lotes import montar_lotes
from gerador.sujeira import aplicar_sujeira

DATA_PADRAO = "2026-09-01"


def gerar(seed: int = 42, dias: int = 14, visitantes: int = 3000, sujeira: float = 0.03,
          data_inicial: datetime = datetime(2026, 9, 1, tzinfo=timezone.utc)) -> tuple[dict[str, str], dict]:
    """Devolve ({chave: conteúdo JSONL}, resumo)."""
    eventos, sessoes, rng = simular(seed, dias, visitantes, data_inicial)
    for e in eventos:
        e["_dt"] = e["event_ts"][:10]           # dia de coleta, antes de a sujeira mexer no timestamp
    por_evento = Counter(e["event_name"] for e in eventos)
    eventos_limpos = len(eventos)
    defeitos = aplicar_sujeira(rng, eventos, sujeira)
    arquivos, duplicatas = montar_lotes(rng, eventos, origem="coletor", seed=seed)

    ultimo_dia = data_inicial + timedelta(days=dias - 1)
    resumo = {
        "seed": seed,
        "periodo": f"{data_inicial:%Y-%m-%d} a {ultimo_dia:%Y-%m-%d}",
        "visitantes": visitantes,
        "sessoes_simuladas": sessoes,
        "eventos_limpos": eventos_limpos,
        "por_evento": {nome: por_evento.get(nome, 0) for nome in cat.EVENTOS},
        "sujeira": defeitos,
        "duplicatas_reenvio": duplicatas,
        "linhas_gravadas": sum(len(v.splitlines()) for v in arquivos.values()),
        "arquivos": len(arquivos),
    }
    return arquivos, resumo


def imprimir_resumo(r: dict, destino: str) -> None:
    print(f"Destino:            {destino}")
    print(f"Período:            {r['periodo']}   seed: {r['seed']}")
    print(f"visitantes          {r['visitantes']:>8}")
    print(f"sessões simuladas   {r['sessoes_simuladas']:>8}")
    print(f"eventos limpos      {r['eventos_limpos']:>8}")
    print(f"duplicatas (lote)   {r['duplicatas_reenvio']:>8}")
    print(f"linhas gravadas     {r['linhas_gravadas']:>8}   em {r['arquivos']} arquivos")
    print("\nEventos por nome (antes da sujeira):")
    for nome, n in r["por_evento"].items():
        print(f"  {nome:<20}{n:>8}")
    print("\nDefeitos injetados:")
    for tipo, n in r["sujeira"].items():
        print(f"  {tipo:<20}{n:>8}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dias", type=int, default=14)
    ap.add_argument("--visitantes", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sujeira", type=float, default=0.03, help="fração de eventos com defeito (0 a 0.5)")
    ap.add_argument("--data-inicial", default=DATA_PADRAO, help="AAAA-MM-DD, em UTC")
    ap.add_argument("--saida-local", type=Path, help="grava nesta pasta em vez do S3")
    a = ap.parse_args(argv)
    if not 0 <= a.sujeira <= 0.5:
        ap.error("--sujeira precisa estar entre 0 e 0.5")
    if a.dias < 1 or a.visitantes < 1:
        ap.error("--dias e --visitantes precisam ser positivos")
    data = datetime.strptime(a.data_inicial, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    arquivos, resumo = gerar(a.seed, a.dias, a.visitantes, a.sujeira, data)
    destino = DestinoLocal(a.saida_local) if a.saida_local else DestinoS3()
    for chave, conteudo in arquivos.items():
        destino.gravar(chave, conteudo)
    imprimir_resumo(resumo, destino.descricao())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Rodar e ver passar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_gerar.py -v`
Expected: 5 passed

- [ ] **Step 5: Conferir que o script roda pelo caminho do arquivo**

Run: `.\.venv\Scripts\python.exe gerador/gerar_eventos.py --dias 2 --visitantes 30 --saida-local saida`
Expected: o resumo é impresso, com `Destino:` apontando para `...\saida`. Depois apague a pasta: `Remove-Item -Recurse -Force saida`.

- [ ] **Step 6: Commit**

```powershell
git add gerador/gerar_eventos.py tests/gerador/test_gerar.py
git commit -m "Gerador: CLI gerar_eventos com resumo e saida local"
```

---

### Task 8: CLI `importar_site.py`

**Files:**
- Create: `gerador/importar_site.py`
- Test: `tests/gerador/test_importar.py`

- [ ] **Step 1: Escrever os testes que falham**

`tests/gerador/test_importar.py`:

```python
from datetime import datetime, timezone

import pytest

from gerador.importar_site import chave, main, validar

UM = '{"event_id":"a","event_ts":"2026-10-10T12:00:00.000Z"}'


def test_validar_aceita_o_jsonl_do_site_e_ignora_linhas_vazias():
    eventos = validar(UM + '\n\n{"event_id":"b","event_ts":"2026-10-10T12:00:01.000Z"}\n')
    assert [e["event_id"] for e in eventos] == ["a", "b"]


@pytest.mark.parametrize("texto, mensagem", [
    ("isto não é json\n", "linha 1: JSON inválido"),
    ('{"event_id":"a"}\n', "linha 1: falta event_id ou event_ts"),
    (UM + "\n[1, 2]\n", "linha 2: falta event_id ou event_ts"),
    ("\n\n", "arquivo sem eventos"),
])
def test_validar_rejeita_com_a_linha_do_problema(texto, mensagem):
    with pytest.raises(ValueError, match=mensagem):
        validar(texto)


def test_chave_usa_a_data_do_primeiro_evento_e_o_instante_do_envio():
    agora = datetime(2026, 10, 11, 8, 30, 0, tzinfo=timezone.utc)
    assert chave([{"event_ts": "2026-10-10T12:00:00.000Z"}], agora) == \
        "landing/eventos/dt=2026-10-10/site-20261011T083000.jsonl"


def test_main_grava_o_arquivo_na_landing(tmp_path):
    origem = tmp_path / "eventos.jsonl"
    origem.write_text(UM + "\n", encoding="utf-8")
    assert main([str(origem), "--saida-local", str(tmp_path / "s3")]) == 0
    gravados = list((tmp_path / "s3").glob("landing/eventos/dt=2026-10-10/site-*.jsonl"))
    assert len(gravados) == 1
    assert gravados[0].read_text(encoding="utf-8") == UM + "\n"


def test_main_devolve_1_e_explica_quando_o_arquivo_e_invalido(tmp_path, capsys):
    origem = tmp_path / "ruim.jsonl"
    origem.write_text("{quebrado\n", encoding="utf-8")
    assert main([str(origem), "--saida-local", str(tmp_path / "s3")]) == 1
    assert "linha 1" in capsys.readouterr().err
    assert not (tmp_path / "s3").exists()
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_importar.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'gerador.importar_site'`

- [ ] **Step 3: Criar `gerador/importar_site.py`**

```python
"""Sobe para a landing do datalake o JSONL exportado pela página site/index.html.

    python gerador/importar_site.py eventos_2026-10-10.jsonl
    python gerador/importar_site.py eventos.jsonl --saida-local saida
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gerador.destinos import DestinoLocal, DestinoS3
from gerador.lotes import PREFIXO


def validar(texto: str) -> list[dict]:
    eventos = []
    for n, linha in enumerate(texto.splitlines(), start=1):
        if not linha.strip():
            continue
        try:
            evento = json.loads(linha)
        except json.JSONDecodeError as erro:
            raise ValueError(f"linha {n}: JSON inválido ({erro.msg})") from None
        if not isinstance(evento, dict) or not isinstance(evento.get("event_id"), str) \
                or not isinstance(evento.get("event_ts"), str):
            raise ValueError(f"linha {n}: falta event_id ou event_ts")
        eventos.append(evento)
    if not eventos:
        raise ValueError("arquivo sem eventos")
    return eventos


def chave(eventos: list[dict], agora: datetime) -> str:
    return f"{PREFIXO}/dt={eventos[0]['event_ts'][:10]}/site-{agora:%Y%m%dT%H%M%S}.jsonl"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("arquivo", type=Path, help="JSONL exportado pelo botão 'Exportar JSONL' do site")
    ap.add_argument("--saida-local", type=Path, help="grava nesta pasta em vez do S3")
    a = ap.parse_args(argv)

    texto = a.arquivo.read_text(encoding="utf-8")
    try:
        eventos = validar(texto)
    except ValueError as erro:
        print(f"erro: {erro}", file=sys.stderr)
        return 1

    destino = DestinoLocal(a.saida_local) if a.saida_local else DestinoS3()
    k = chave(eventos, datetime.now(timezone.utc))
    destino.gravar(k, texto if texto.endswith("\n") else texto + "\n")
    print(f"{len(eventos)} eventos enviados para {destino.descricao()} em {k}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Rodar e ver passar**

Run: `.\.venv\Scripts\python.exe -m pytest tests/gerador/test_importar.py -v`
Expected: 8 passed

- [ ] **Step 5: Rodar a suíte inteira**

Run: `.\.venv\Scripts\python.exe -m pytest`
Expected: 43 passed

- [ ] **Step 6: Commit**

```powershell
git add gerador/importar_site.py tests/gerador/test_importar.py
git commit -m "Gerador: importacao do JSONL exportado pelo site"
```

---

### Task 9: Integração com o floci

Esta tarefa precisa do Docker rodando. Se `docker ps` falhar, **peça ao usuário para abrir o Docker Desktop** e espere a confirmação.

**Files:** nenhum arquivo novo (só execução e conferência).

- [ ] **Step 1: Subir o floci**

```powershell
docker compose up -d --wait
docker ps --filter name=floci-datalake-tracking
```

Expected: o contêiner `floci-datalake-tracking` com status `Up` e porta `0.0.0.0:4566->4566/tcp`.

- [ ] **Step 2: Gerar com os parâmetros padrão**

Run: `.\.venv\Scripts\python.exe gerador/gerar_eventos.py`
Expected: o resumo começa com `Destino:            s3://datalake-tracking (http://127.0.0.1:4566)`. **Guarde a saída inteira**: ela vai para o `gerador/README.md` na Task 10.

- [ ] **Step 3: Conferir o que chegou ao bucket**

```powershell
.\.venv\Scripts\python.exe -c "import boto3, collections; s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:4566', aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1'); chaves = [o['Key'] for p in s3.get_paginator('list_objects_v2').paginate(Bucket='datalake-tracking', Prefix='landing/eventos/') for o in p.get('Contents', [])]; dias = collections.Counter(k.split('/')[2] for k in chaves); print(len(chaves), 'arquivos'); print(min(dias.values()), 'a', max(dias.values()), 'lotes por dia'); print(sorted(dias)[0], '...', sorted(dias)[-1])"
```

Expected: o mesmo número de arquivos do resumo; de 3 a 6 lotes por dia; dias de `dt=2026-09-01` até `dt=2026-09-14` (pode haver um `dt=2026-09-15`, de sessões que viram a meia-noite).

- [ ] **Step 4: Conferir a idempotência**

Run: `.\.venv\Scripts\python.exe gerador/gerar_eventos.py` (segunda vez, mesma seed)
Then: repita o comando do Step 3.
Expected: o mesmo número de arquivos. A mesma seed sobrescreve as mesmas chaves; só uma seed diferente acumula arquivos novos.

- [ ] **Step 5: Conferir a importação do site**

Gere um JSONL pequeno a partir do próprio gerador, com o mesmo formato que o site exporta, e importe:

```powershell
.\.venv\Scripts\python.exe gerador/gerar_eventos.py --dias 1 --visitantes 5 --sujeira 0 --saida-local saida
Get-ChildItem saida -Recurse -Filter *.jsonl | Select-Object -First 1 | ForEach-Object { .\.venv\Scripts\python.exe gerador/importar_site.py $_.FullName }
Remove-Item -Recurse -Force saida
```

Expected: `N eventos enviados para s3://datalake-tracking (...) em landing/eventos/dt=2026-09-01/site-<instante>.jsonl`.

- [ ] **Step 6: Sem commit**

Nada mudou no repositório. Siga para a Task 10 com a saída do Step 2 em mãos.

---

### Task 10: Documentação do gerador e do repositório

**Files:**
- Create: `gerador/README.md`, `SETUP.md`, `README.md`

- [ ] **Step 1: Criar `gerador/README.md`**

Substitua o bloco "Saída com os parâmetros padrão" pela saída **real** do Task 9, Step 2, colada sem edição.

````markdown
# Gerador de eventos — o coletor de tracking simulado

Este diretório faz o papel do **mundo lá fora**: a landing page DadosPro e o coletor de tracking que grava os eventos dela no S3. O pipeline do datalake não conhece este código. A única coisa que os dois compartilham é o **formato do evento** (`docs/CONTRATO_EVENTO.md`) e o prefixo `landing/eventos/` do bucket.

Não usa Spark: é Python puro mais `boto3`.

## Uso

```powershell
docker compose up -d --wait                                     # S3 local (floci)
.\.venv\Scripts\python.exe gerador/gerar_eventos.py              # 3000 visitantes, 14 dias, seed 42
```

| Opção | Padrão | O que faz |
| --- | --- | --- |
| `--dias` | 14 | dias simulados a partir de `--data-inicial` |
| `--visitantes` | 3000 | visitantes únicos (cada um faz de 1 a 4 sessões) |
| `--seed` | 42 | mesma seed, mesmos arquivos, byte a byte |
| `--sujeira` | 0.03 | fração de eventos com defeito (0 a 0.5) |
| `--data-inicial` | 2026-09-01 | primeiro dia, em UTC |
| `--saida-local PASTA` | — | grava numa pasta, com a mesma árvore de chaves, em vez do S3 |

Rodar de novo com a **mesma** seed sobrescreve os mesmos arquivos. Uma seed **diferente** acrescenta arquivos novos (`coletor-s<seed>-*.jsonl`), como um segundo coletor gravando no mesmo bucket.

### Importar os eventos da página de exemplo

Navegue por `site/index.html`, clique em **Exportar JSONL** no painel lateral e suba o arquivo:

```powershell
.\.venv\Scripts\python.exe gerador/importar_site.py C:\Users\voce\Downloads\eventos_2026-10-10.jsonl
```

O arquivo vai para `landing/eventos/dt=<data do primeiro evento>/site-<instante>.jsonl`.

## O que é gravado

`s3://datalake-tracking/landing/eventos/dt=AAAA-MM-DD/coletor-s<seed>-<lote>.jsonl` — um evento JSON por linha, de 3 a 6 lotes por dia.

## Como a jornada é simulada

Cada visitante recebe um canal de origem e um dispositivo, que mudam as chances de rolar a página, ver o vídeo, escolher um plano e comprar. Na ordem da página: hero → vídeo → módulos → tabela de planos → checkout → obrigado → área de membros.

- **Quem vira lead** volta outro dia, em geral pela newsletter, com mais intenção de compra.
- **Quem compra** pode criar conta (`sign_up`) e, em visitas seguintes, faz `login`.
- **Reembolsos:** cerca de 8% das compras recebem `refund` de 1 a 7 dias depois.
- **Frete:** só o plano Premium tem `add_shipping_info`, porque inclui apostila impressa.
- **`session_hint`:** muda em cerca de 25% das trocas de página, como uma recarga do navegador. Por isso não serve para contar sessões.

## Defeitos injetados

| Tipo | O que acontece |
| --- | --- |
| `purchase_sem_id` | `purchase` sem `transaction_id` (no máximo 5% das compras) |
| `valor_negativo` | `value` negativo |
| `moeda_usd` | `currency = "USD"` |
| `scroll_33` | `percent_scrolled = 33`, fora dos marcos |
| `ts_nulo` | `event_ts` nulo |
| `ts_futuro` | `event_ts` dez anos à frente |
| `refund_orfao` | `refund` de uma transação que não existe |
| `evento_fora_catalogo` | `event_name = "add_to_wishlist"` |
| reenvio de lote | cerca de 1% dos eventos aparece de novo, com o mesmo `event_id`, no lote vizinho |

Um `purchase` que perde o `transaction_id` ou o valor deixa o `refund` dele sem compra válida. Esse efeito em cascata é proposital.

## Saída com os parâmetros padrão

```text
(colar aqui, sem editar, a saída de `python gerador/gerar_eventos.py` do Task 9, Step 2)
```

## Testes

```powershell
.\.venv\Scripts\python.exe -m pytest tests/gerador
```
````

- [ ] **Step 2: Criar `SETUP.md`**

````markdown
# Setup — o que instalar e configurar

Tempo total: **cerca de 10 minutos** se você já tem Docker e Java.

## O que é preciso, e por quê

| Item | Versão | Por quê |
| --- | --- | --- |
| **Docker Desktop** (Engine + Compose) | atual | roda o floci, o S3 local; nenhuma conta AWS é necessária |
| **floci** | `floci/floci:latest` | emulador de AWS compatível com o `boto3` |
| **Python** | **3.11** (3.8 a 3.12 funcionam) | o PySpark 3.5 não suporta 3.13 nem 3.14 |
| **Java (JDK)** | 8, 11 ou 17 | o Spark roda numa JVM |
| **PySpark** | 3.5.3 | o motor do pipeline, sempre local |
| **boto3** | atual | quem fala com o S3 é o Python, não o Spark |
| **pytest** | atual | testes do gerador |
| **`winutils.exe` + `hadoop.dll`** | série 3.3.5 | **só no Windows**: o Spark grava Parquet por uma camada de Hadoop |

## Passo 1 — floci (o S3 local)

O `docker-compose.yml` já está na raiz do repositório.

```powershell
docker compose up -d --wait
docker ps --filter name=floci-datalake-tracking
```

> A linha do `docker.sock` no compose dá ao contêiner acesso ao daemon Docker do seu computador. É o que a documentação do floci pede. Use só numa máquina de estudo.

## Passo 2 — Python e Java

```powershell
py -0             # precisa listar -V:3.11 (ou 3.8 a 3.12)
java -version     # 1.8, 11 ou 17
```

Sem o Python 3.11: `winget install -e --id Python.Python.3.11`. Sem o Java: `winget install -e --id EclipseAdoptium.Temurin.17.JDK`, depois feche e reabra o terminal.

## Passo 3 — Ambiente virtual

Na raiz do repositório:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Passo 4 — `winutils.exe` (só no Windows)

```powershell
New-Item -ItemType Directory -Force hadoop\bin | Out-Null
$R = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
curl.exe -sSL -o hadoop\bin\winutils.exe $R/winutils.exe
curl.exe -sSL -o hadoop\bin\hadoop.dll   $R/hadoop.dll
```

A pasta `hadoop/` fica na raiz do repositório e está no `.gitignore`.

## Passo 5 — Popular a landing do datalake

```powershell
.\.venv\Scripts\python.exe gerador/gerar_eventos.py
```

O resumo impresso diz quantos eventos e defeitos foram gerados. Detalhes em [gerador/README.md](gerador/README.md).

## Trocando o floci por uma conta AWS real

```powershell
$env:AWS_ENDPOINT_URL = ""
$env:AWS_ACCESS_KEY_ID = "SEU_ACCESS_KEY"
$env:AWS_SECRET_ACCESS_KEY = "SUA_SECRET_KEY"
$env:AWS_DEFAULT_REGION = "sa-east-1"
$env:AWS_S3_BUCKET = "um-bucket-seu"
```

## Problemas de instalação

| O que você vê | Causa | Solução |
| --- | --- | --- |
| `Não consegui falar com o S3 em http://127.0.0.1:4566` | o floci não está rodando | `docker compose up -d --wait` |
| porta `4566` já em uso | outro floci está rodando | pare o outro contêiner, ou use o mesmo (o bucket é outro) |
| `boto3 não está instalado` | venv errado | use `.\.venv\Scripts\python.exe` |
| `Python worker exited unexpectedly` / erro de `pyspark` ao importar | Python 3.13 ou 3.14 no venv | recrie o venv com `py -3.11` |
| `HADOOP_HOME and hadoop.home.dir are unset` | falta o Passo 4 | baixe `winutils.exe` e `hadoop.dll` |
| `JAVA_HOME is not set` | Java ausente ou terminal antigo | instale o JDK e reabra o terminal |

## Desinstalar

```powershell
docker compose down -v
Remove-Item -Recurse -Force .venv, hadoop
```
````

- [ ] **Step 3: Criar `README.md`**

```markdown
# DadosPro — projeto final de engenharia de dados

Uma empresa vende um curso online por uma landing page de conversão, e cada interação do visitante vira um evento de tracking gravado num bucket S3. O projeto é construir o **datalake em arquitetura medalhão** que transforma esses eventos em respostas de negócio: funil de compra, engajamento, receita líquida e previsão de compra.

## O que há aqui

| Pasta | O que é |
| --- | --- |
| [`site/`](site/index.html) | a landing page de exemplo. Abra no navegador, navegue e veja cada evento nascer no painel lateral |
| [`gerador/`](gerador/README.md) | o coletor simulado: popula o bucket com 14 dias de eventos, inclusive com defeitos |
| `pipeline/` | onde o datalake é construído |
| `docs/` | enunciado e contrato do evento |

## Começar

1. Prepare o ambiente: [SETUP.md](SETUP.md).
2. Popule a landing: `.\.venv\Scripts\python.exe gerador/gerar_eventos.py`.
3. Leia o enunciado em `docs/ENUNCIADO.md` e construa o pipeline em `pipeline/`.
```

- [ ] **Step 4: Commit**

```powershell
git add gerador/README.md SETUP.md README.md
git commit -m "Docs: README do gerador com resumo medido, SETUP e README"
```

---

### Task 11: Alinhar o spec com o que foi construído

**Files:**
- Modify: `docs/specs/2026-10-10-projeto-final-datalake-tracking-design.md`

- [ ] **Step 1: Trocar `s3.py` por `destinos.py` na árvore da seção 4**

Substitua a linha:

```text
│   ├── s3.py                     cliente boto3 do gerador
```

por:

```text
│   ├── destinos.py               destino S3 (boto3) ou pasta local (--saida-local)
```

- [ ] **Step 2: Atualizar a nota do pacote do aluno na seção 4**

Substitua o parágrafo que começa com `**Pacote do aluno.**` por:

```markdown
**Pacote do aluno.** `git archive --format=zip -o projeto-final-aluno.zip HEAD` gera o zip sem `professor/`, `docs/specs/` e `docs/superpowers/` (todos `export-ignore` no `.gitattributes`, porque descrevem a solução). O professor distribui o zip, nunca o repositório. Isso fica documentado em `professor/README.md`.
```

- [ ] **Step 3: Marcar os critérios de pronto concluídos na seção 12**

Troque `- [ ]` por `- [x]` nas duas linhas que começam com "O gerador roda com os padrões" e "As cinco armadilhas da seção 9".

A observabilidade das armadilhas 1, 2, 4 e 5 é garantida pelos testes de `tests/gerador/`. A armadilha 3 (vazamento) depende só das features escolhidas e é conferida no Plano 2.

- [ ] **Step 4: Conferir o pacote do aluno**

```powershell
git archive --format=zip -o $env:TEMP\aluno.zip HEAD
tar -tf $env:TEMP\aluno.zip | Select-String -Pattern "^(professor|docs/specs|docs/superpowers)/"
Remove-Item $env:TEMP\aluno.zip
```

Expected: nenhuma linha impressa pelo `Select-String`.

- [ ] **Step 5: Commit**

```powershell
git add docs/specs/2026-10-10-projeto-final-datalake-tracking-design.md
git commit -m "Spec: alinha estrutura do gerador e pacote do aluno com o implementado"
```

---

## Depois deste plano

Com o gerador rodando e a landing populada, o **Plano 2** cobre:
- `professor/solucao/`: os seis jobs, `comum.py`, `executar_pipeline.py` e `verificar_ambiente.py`;
- `professor/GABARITO.md`, com os números medidos e as tolerâncias;
- `professor/README.md`;
- `docs/ENUNCIADO.md` e `docs/CONTRATO_EVENTO.md`;
- `pipeline/README.md`.

Ele é escrito em cima do dado real que este plano produz.
