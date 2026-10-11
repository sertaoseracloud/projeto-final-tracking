# Área do professor

Esta pasta **não vai para o aluno**. O `.gitattributes` marca `professor/`, `docs/specs/` e `docs/superpowers/` como `export-ignore`.

## Gerar o pacote do aluno

Se você usou o `usar_solucao.py` (seção "Mostrar a solução no explorador"), tire as cópias antes:

```bash
docker compose run --rm app python professor/usar_solucao.py --desfazer
git archive --format=zip -o dist/projeto-final-aluno.zip HEAD
```

O `git archive` pega só o que está commitado no `HEAD`; a pasta `dist/` fica fora do git. Distribua o zip, nunca o repositório nem um fork dele. Antes de distribuir, confira:

```bash
# 1. nada da solução dentro do zip (a saída tem que ficar vazia)
unzip -l dist/projeto-final-aluno.zip | grep -E "professor/|docs/specs/|docs/superpowers/"

# 2. em pipeline/ só vão o README.md e o comum.py (sobrou cópia do usar_solucao.py? rode o --desfazer)
unzip -Z1 dist/projeto-final-aluno.zip | grep "^pipeline/"

# 3. o pacote roda sozinho: descompacte numa pasta nova e rode os testes dela
unzip -q dist/projeto-final-aluno.zip -d /tmp/pacote-aluno
docker run --rm -v /tmp/pacote-aluno:/projeto -w /projeto datalake-tracking-app python -m pytest -q
```

O pacote leva o `pipeline/comum.py` pronto, o explorador inteiro (S3, Data Catalog, Jobs ETL, Workflows e Athena) e nenhum job: o aluno escreve todos.

### Distribuir pela release do GitHub

Cada versão do pacote vira uma release do repositório, com o zip anexado (`gh` é o GitHub CLI):

```bash
gh release create aluno-v1.0.0 dist/projeto-final-aluno.zip --title "Pacote do aluno v1.0.0" --notes "…"
```

A tag marca o commit de onde o zip saiu. Para uma versão nova, gere o zip de novo e crie outra tag (`aluno-v1.1.0`…). Num repositório privado, só quem tem acesso a ele baixa a release; para a turma, envie o zip.

### Ver o pacote como o aluno vê

Descompacte o zip numa pasta própria e siga o `SETUP.md` dela:

```bash
unzip -q dist/projeto-final-aluno.zip -d ../projeto-final-aluno
cd ../projeto-final-aluno
docker compose build app
docker compose up -d floci        # espere o docker compose ps mostrar (healthy)
docker compose run --rm app python gerador/gerar_eventos.py
docker compose run --rm app python infra/verificar_ambiente.py
docker compose up -d explorador
```

As duas cópias usam os mesmos nomes de contêiner (`floci-datalake-tracking`) e as mesmas portas (4566 e 8501). Desligue uma com `docker compose down`, na pasta dela, antes de subir a outra. O floci guarda tudo na memória: ao trocar, gere os eventos de novo.

## Rodar a solução de referência

```bash
docker compose up -d floci
docker compose run --rm app python gerador/gerar_eventos.py
docker compose run --rm app python professor/solucao/limpar_camadas.py
docker compose run --rm app python professor/solucao/executar_pipeline.py
docker compose run --rm app python professor/solucao/medir_armadilhas.py
```

O mesmo pipeline como workflow (um grafo: as cinco Golds rodam lado a lado depois da Silver, até 2 ao mesmo tempo):

```bash
docker compose run --rm app python -m explorador.workflows professor/solucao/workflows/medalhao.json
```

Medido com os dados padrão: cerca de 11 minutos, todos os 8 jobs com sucesso.

Testes da solução:

```bash
docker compose run --rm app python -m pytest professor/solucao/tests
```

## Mostrar a solução no explorador

O explorador trabalha só com `pipeline/`: o botão **⟳ Sincronizar** envia essa pasta para o bucket `glue-assets` do floci, e os jobs rodam de lá. Ele nunca roda nada direto de `professor/solucao/`. Para mostrar a solução, copie-a antes para as pastas do aluno:

```bash
docker compose run --rm app python professor/usar_solucao.py
docker compose up -d floci explorador
```

O script copia os jobs de `professor/solucao/` para `pipeline/` e o `workflows/medalhao.json` para `pipeline/workflows/`. O `comum.py` não é copiado: a solução já usa o `pipeline/comum.py`. Um arquivo do aluno com o mesmo nome e conteúdo diferente nunca é sobrescrito; ele aparece como "pulado".

No explorador, clique em **⟳ Sincronizar** (em Jobs ETL ou Workflows). Aí aparecem os jobs da solução em **Glue · Jobs ETL** e o workflow `medalhao` em **Glue · Workflows**. Além dos 8 jobs do pipeline, a lista mostra `executar_pipeline`, `limpar_camadas` e `medir_armadilhas`, que também rodam pelo console, e `sessoes`, que é só um módulo de apoio importado pelas Golds: não o execute. O floci começa vazio, então gere os eventos antes (`gerador/gerar_eventos.py`, depois de o `docker compose ps` mostrar o floci `(healthy)`).

Depois da aula, tire as cópias (e sincronize de novo para tirá-las também do floci):

```bash
docker compose run --rm app python professor/usar_solucao.py --desfazer
```

**Atenção:** as cópias em `pipeline/` não podem ser commitadas, senão a solução vai junto no pacote do aluno. Rode o `--desfazer` antes de gerar o zip; a conferência da seção "Gerar o pacote do aluno" também acusa se sobrar algum job em `pipeline/`.

## O que há aqui

| Arquivo | Para quê |
| --- | --- |
| [`GABARITO.md`](GABARITO.md) | números medidos com os dados padrão, tolerâncias e roteiro de correção |
| `solucao/01_bronze_ingestao.py` … `07_gold_mapa_calor.py` | um job por camada, lendo e gravando no S3 só pelo Spark (`s3a://`) |
| `solucao/08_exportar_mapa_calor.py` | exporta as Golds do mapa de calor para `site/mapa_calor.js` |
| `solucao/comum.py`, `solucao/sessoes.py` | `comum.py` só carrega o `pipeline/comum.py` do aluno (fonte única); `sessoes.py` tem as transformações de sessão compartilhadas |
| `solucao/executar_pipeline.py`, `solucao/limpar_camadas.py` | rodar os 8 jobs em sequência (o R3); zerar Bronze, Silver e Gold sem tocar na landing |
| `solucao/workflows/medalhao.json` | os mesmos 8 jobs como workflow do explorador, com as dependências reais entre eles |
| `solucao/medir_armadilhas.py` | como cada armadilha aparece nos números |
| `usar_solucao.py` | copia a solução para `pipeline/` (e `--desfazer` tira as cópias), para mostrá-la no explorador |
| `solucao/tests/` | testes das transformações, com Spark local e sem S3, e do workflow `medalhao` |

A Bronze é append-only: rodar o `executar_pipeline.py` (ou o workflow) duas vezes sem limpar dobra a Bronze. Silver e Gold não mudam, porque a Silver deduplica por `event_id`. Para os números do gabarito, rode sempre a partir do `limpar_camadas.py`.
