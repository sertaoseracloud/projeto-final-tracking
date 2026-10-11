"""Tutorial do console: um tour em modal por tela, que destaca cada elemento e explica para que serve.

Cada roteiro é uma lista de passos (seletor CSS do elemento, título e texto). Os seletores usam as
classes `st-key-<chave>` que o Streamlit põe nos elementos com `key`. Sem Streamlit aqui: só dados.
O "não mostrar mais" é gravado pelo módulo `preferencias`.
"""

from __future__ import annotations

from dataclasses import dataclass

TELA_S3 = "S3 · Buckets"
TELA_CATALOGO = "Glue · Data Catalog"
TELA_JOBS = "Glue · Jobs ETL"
TELA_ATHENA = "Athena · Editor de consultas"

MENU = '[data-testid="stSidebar"] [role="radiogroup"]'
ABAS = '[data-testid="stTabs"] [role="tablist"]'


@dataclass(frozen=True)
class Passo:
    seletor: str | None      # None: balão no centro da tela, sem destaque
    titulo: str
    texto: str


def _k(chave: str) -> str:
    return f'[class*="st-key-{chave}"]'


ROTEIROS: dict[str, list[Passo]] = {
    "s3-buckets": [
        Passo(None, "Bem-vindo ao console do datalake",
              "Este console imita o da AWS para você treinar o fluxo do mercado, mas tudo roda no seu computador: "
              "o floci faz o papel do S3, o Spark do contêiner o do Glue e o DuckDB o do Athena. "
              "Este tour mostra para que serve cada parte da tela. Use Próximo e Anterior, ou feche quando quiser."),
        Passo(".barra-console", "Barra do console",
              "Mostra o bucket, o endereço do S3 local e a região. Também lembra a regra de ouro: o Athena só lê; "
              "só a tela de Jobs ETL executa código."),
        Passo(MENU, "Serviços",
              "O menu lateral leva às quatro telas: Buckets (os arquivos), Data Catalog (as tabelas), "
              "Jobs ETL (rodar o seu pipeline) e Editor de consultas (SQL). Cada tela tem o seu próprio tour."),
        Passo(_k("painel-buckets"), "Lista de buckets",
              "Um bucket é um \"disco\" do S3. O projeto tem um só, o datalake-tracking. Clique no nome dele para ver "
              "as pastas landing/, bronze/, silver/ e gold/."),
        Passo(_k("recarregar"), "Recarregar",
              "Depois de rodar um job, clique aqui para o console ler o bucket de novo e enxergar as pastas e colunas novas."),
        Passo(_k("dock"), "Ajuda e acessibilidade",
              "Esta barra fica sempre no canto inferior direito. 🎓 abre o tour da tela em que você estiver; "
              "a chave \"Mostrar tutorial\" desliga os tours automáticos; ♿ abre as opções de acessibilidade "
              "(tamanho do texto, alto contraste, links sublinhados, espaçamento e menos movimento); "
              "⬆ volta ao topo da página."),
    ],
    "s3-bucket": [
        Passo(_k("trilha-s3"), "Caminho (breadcrumb)",
              "Mostra onde você está: Buckets › datalake-tracking › pasta › subpasta. Clique em qualquer parte para voltar."),
        Passo(ABAS, "Abas do bucket",
              "Objetos lista pastas e arquivos; Partições resume os dados por evento e por dia; Propriedades mostra a "
              "visão geral do bucket."),
        Passo(_k("painel-objetos"), "Objetos",
              "Pastas (📁) e arquivos (📄) da pasta atual, com tipo, data e tamanho. As pastas event_name=…, year=…, "
              "month=… e day=… são as partições. Clique numa pasta para entrar."),
        Passo(_k("painel-objetos"), "Prévia de um arquivo",
              "Clique num arquivo .jsonl ou .parquet: as primeiras 100 linhas aparecem logo abaixo, em formato de tabela. "
              "É a forma mais rápida de conferir o que o seu job gravou."),
    ],
    "catalogo-bancos": [
        Passo(_k("painel-bancos"), "Bancos de dados",
              "O Data Catalog é a lista do que existe no lake. Aqui há um banco por camada do medalhão: landing, bronze, "
              "silver e gold. Clique num banco para ver as tabelas dele."),
        Passo(_k("trilha-catalogo"), "Caminho",
              "Glue › Data Catalog › banco › tabela. Clique em Data Catalog para voltar à lista de bancos."),
    ],
    "catalogo-tabelas": [
        Passo(_k("painel-tabelas"), "Tabelas do banco",
              "Cada pasta com dados vira uma tabela: nome, formato (Parquet ou JSON), localização no S3, quantas partições "
              "e quantas colunas. Clique no nome para ver o esquema."),
    ],
    "catalogo-tabela": [
        Passo(_k("painel-tabela-visao"), "Visão geral da tabela",
              "Onde os arquivos ficam, o formato, as chaves de partição e o nome para usar no Athena (banco.tabela)."),
        Passo(ABAS, "Esquema e partições",
              "Esquema: as colunas e os tipos (as chaves de partição vêm marcadas). Partições: uma linha por partição, "
              "com arquivos e tamanho."),
        Passo(_k("consultar_athena"), "Consultar no Athena",
              "Abre uma consulta nova no editor, já com SELECT * FROM banco.tabela LIMIT 10."),
    ],
    "jobs-lista": [
        Passo(_k("painel-jobs"), "Seus jobs",
              "Cada arquivo .py da pasta pipeline/ (menos o comum.py) é um job. A tabela mostra o status e o início da "
              "última execução. Clique no nome para abrir o job. Com mais de 8 jobs, aparece a caixa Localizar "
              "para filtrar a lista pelo nome."),
        Passo(_k("trilha-jobs"), "Caminho",
              "Glue › Jobs ETL › job. Clique em Jobs ETL para voltar à lista depois de abrir um job."),
    ],
    "jobs-detalhe": [
        Passo(_k("executar_job"), "Executar",
              "Roda o job no Spark do contêiner, o mesmo que docker compose run --rm app python pipeline/<job>.py. "
              "Enquanto ele roda, o botão fica desligado: um job por vez."),
        Passo(_k("painel-execucoes"), "Execuções",
              "Cada execução com status (Em execução, Sucesso ou Falhou), início, duração e código de saída. "
              "Enquanto o job roda, esta tabela se atualiza sozinha."),
        Passo(_k("painel-log"), "Log",
              "Tudo o que o job imprimiu. Se a execução falhar, o motivo quase sempre está nas últimas linhas."),
        Passo(ABAS, "Script",
              "A aba Script mostra o código do job (só leitura). Para mudar o job, edite o arquivo no seu editor."),
    ],
    "athena": [
        Passo(_k("painel-athena-dados"), "Dados",
              "Escolha o banco de dados e veja as tabelas dele. Abra uma tabela para ver as colunas; Inserir no editor "
              "escreve banco.tabela na sua consulta."),
        Passo(_k("athena_banco"), "Banco de dados",
              "landing, bronze, silver e gold: os mesmos bancos do Data Catalog."),
        Passo(_k("painel-athena-editor"), "Editor de consultas",
              "Escreva a SQL aqui. As abas Consulta 1, Consulta 2… guardam consultas diferentes; ＋ abre uma nova e ✕ "
              "fecha a atual. Só são aceitas consultas de leitura."),
        Passo(_k("executar"), "Executar e limite",
              "Executar roda a consulta. O limite diz quantas linhas trazer (padrão 1 000, máximo 50 000)."),
        Passo(_k("painel-athena-resultados"), "Resultados e consultas recentes",
              "Depois de executar: status, tempo, linhas e dados lidos (estimado). Filtrar por event_name diminui os dados "
              "lidos, como no Athena de verdade. A aba Consultas recentes guarda o histórico desta sessão."),
    ],
}


def roteiro(contexto: str) -> list[Passo]:
    return ROTEIROS.get(contexto, [])
