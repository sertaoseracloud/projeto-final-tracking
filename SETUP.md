# Setup — o que instalar e configurar

Só é preciso o **Docker**. Python, Java, PySpark e o conector do Spark para S3 vêm dentro da imagem do projeto, e o S3 é emulado pelo floci. Nenhuma conta de nuvem é necessária.

Tempo total: **cerca de 10 minutos** na primeira vez, a maior parte baixando imagens.

## O que roda em cada contêiner

| Serviço | Imagem | O que tem |
| --- | --- | --- |
| `floci` | `floci/floci:latest` | S3 emulado em `http://floci:4566` (e `localhost:4566` na sua máquina) |
| `app` | construída pelo `Dockerfile` | Python 3.11, Java 17, PySpark 3.5.3, conector `s3a` (`hadoop-aws` 3.3.4), `boto3`, `pytest` |

O repositório é montado em `/projeto` dentro do `app`: o que você edita na sua máquina vale na hora, sem reconstruir a imagem.

## Passo 1 — Docker

Instale o Docker Desktop (Windows ou macOS) ou o Docker Engine com o plugin Compose (Linux) e confira:

```bash
docker compose version
```

## Passo 2 — Construir a imagem e subir o S3

Na raiz do repositório:

```bash
docker compose build app
docker compose up -d floci
```

## Passo 3 — Popular a landing do datalake

```bash
docker compose run --rm app python gerador/gerar_eventos.py
```

O resumo impresso diz quantos eventos e defeitos foram gerados. Detalhes em [gerador/README.md](gerador/README.md).

## Passo 4 — Conferir o Spark falando com o S3

```bash
docker compose run --rm app python infra/verificar_ambiente.py
```

Precisa terminar com três linhas `[ok]`. A configuração `s3a` usada pelo Spark está no início do próprio script (`infra/verificar_ambiente.py`).

## Como rodar qualquer coisa

```bash
docker compose run --rm app python <script.py>
docker compose run --rm app python -m pytest
```

## Problemas comuns

| O que você vê | Causa | Solução |
| --- | --- | --- |
| `Cannot connect to the Docker daemon` | Docker parado | abra o Docker Desktop |
| `Não consegui falar com o S3 em http://floci:4566` | floci parado | `docker compose up -d floci` |
| porta `4566` já em uso | outro emulador rodando | pare o outro contêiner |
| `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem` | imagem antiga, sem os jars | `docker compose build --no-cache app` |
| `[erro]` no `verificar_ambiente.py` lendo a landing | o gerador ainda não rodou | Passo 3 |

## Desinstalar

```bash
docker compose down -v
docker image rm datalake-tracking-app
```
