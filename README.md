# DocPipe Processing

Microserviço independente de processamento documental do DocPipe. Implementa
contrato público de entrada, `ProcessingJob`, persistência PostgreSQL e entrada
durável. A API atual expõe somente `/health` e `/metrics`.

**IMPLEMENTADO:** domínio, contrato `document.received.v1`, repository síncrono,
entrada durável na própria `processing_jobs`, migrations e observabilidade HTTP.
**PLANEJADO:** API HTTP de domínio, RabbitMQ, recuperação do original, pipeline,
`ProcessingResult` e publicação. Nenhum worker ou ACK existe nesta entrega.
O [DESIGN](docs/DESIGN.md) e o [PLAN](docs/PLAN.md) permanecem preservados; suas
seções de estado observado registram a revisão documental anterior a este bloco.

## Requisitos e stack

- Python **3.14.4**; o projeto aceita somente a linha minor 3.14.
- `uv`; Docker e CI usam **0.12.20**.
- Docker com Compose para a infraestrutura local.
- PostgreSQL **`postgres:17.11-bookworm`**, SQLAlchemy síncrono, Alembic e
  Psycopg 3 com extra `binary`, resolvido no lockfile próprio.

As bibliotecas compartilhadas existentes foram preservadas. O Ingestion não
está disponível neste ambiente; sua baseline ainda exige conferência externa.
Instalação, build, testes e execução não dependem de outro microsserviço.

## Configuração e PostgreSQL local

```bash
cp .env.example .env
```

Edite `.env` com credenciais locais próprias. Não versione esse arquivo.
Configure as três senhas (bootstrap, migrations e aplicação) e substitua os
placeholders nas URLs. Símbolos especiais das senhas devem ser codificados
nas URLs. `.env` é lido pelo Compose; comandos `uv` usam `--env-file .env`.

| Variável | Uso |
| --- | --- |
| `PROCESSING_DATABASE_URL` | API no host, usuário de aplicação, `127.0.0.1` e porta publicada. |
| `PROCESSING_MIGRATION_DATABASE_URL` | Referência para migrations no host, com usuário proprietário. |
| `PROCESSING_CONTAINER_DATABASE_URL` | API em container, usuário de aplicação, host `postgres:5432`. |
| `PROCESSING_CONTAINER_MIGRATION_URL` | Migrations em container, usuário proprietário, host `postgres:5432`. |
| `PROCESSING_TEST_DATABASE_URL` | Banco separado cujo nome termina em `_test`; nunca o banco operacional. |

`PROCESSING_DATABASE_URL` é obrigatória ao configurar persistência/migrations.
URL ausente, vazia, inválida ou diferente de `postgresql+psycopg` falha
explicitamente. Não há default com senha nem fallback SQLite. O Engine conecta
sob demanda; falhas operacionais do repository produzem
`PersistenceUnavailableError`, sem imprimir a URL. O processo que cria o Engine
é responsável por encerrá-lo com `dispose()`.

Inicie somente o banco e aplique as migrations em processo separado:

```bash
docker compose config --quiet
docker compose up -d --wait postgres
docker compose build migrate
docker compose run --rm migrate
```

O banco lógico e os usuários pertencem ao Processing. O script de inicialização
cria um proprietário sem privilégios globais e um usuário de aplicação com
permissões de dados, sem DDL. O superusuário serve apenas ao bootstrap.
O volume nomeado preserva os dados; o script roda somente em volume novo.
Alterar variáveis não modifica senhas ou permissões de um volume existente.
Não remova volumes para corrigir configuração sem avaliar os dados.

PostgreSQL é publicado somente em loopback, na porta configurável
`PROCESSING_POSTGRES_PORT` (5432 por padrão). Há healthcheck, limite de 1 CPU e
512 MiB. Esses limites locais não constituem metas de desempenho.

Para executar migrations no host, forneça a URL do proprietário explicitamente:

```bash
PROCESSING_DATABASE_URL='postgresql+psycopg://processing:<encoded-migration-password>@127.0.0.1:5432/processing' uv run alembic upgrade head
```

Alembic controla o schema. Não há `create_all`, migrations automáticas no startup
ou dependência do Compose do Ingestion.

## Execução da API

```bash
uv sync --locked
uv run --env-file .env python main.py
```

A API escuta em `127.0.0.1:8000`; `PROCESSING_HOST` e `PROCESSING_PORT` permitem
alterar esse endereço. Alternativamente, após aplicar migrations:

```bash
docker compose --profile app up -d --build api
curl --fail http://127.0.0.1:8000/health
curl --fail http://127.0.0.1:8000/metrics
```

A imagem usa usuário não privilegiado e Python/uv fixados. `/health` retorna
`{"status":"ok"}` e indica somente que a API responde; não verifica o banco.
A API operacional pode responder sem PostgreSQL. Consultas de domínio ainda não
existem. Os logs são JSON, as métricas têm baixa cardinalidade e o tracing extrai
W3C Trace Context. Exportação OTLP é opcional por
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`; não há stack centralizada neste serviço.

## Entrada durável e compatibilidade

`ProcessingInput` reutiliza a validação do contrato e é imutável. O domínio
`ProcessingJob` permanece independente de Pydantic, SQLAlchemy e transporte.

`ProcessingJobRepository.create(job, input=entrada)` exige os quatro metadados
validados: `storage_key`, `media_type`, `size_bytes` e `sha256`. Job e entrada
ocupam a mesma linha e são confirmados em uma transação. A criação não retorna
sucesso antes do commit. `save(job)` preserva os metadados aceitos.
As consultas `get_input_by_processing_id` e `get_input_by_source_event_id`
recuperam a entrada sem RabbitMQ ou produtor. Os identificadores externos
continuam strings opacas, e `correlation_id` continua opcional/anulável.

`0001_processing_jobs` foi preservada; `0002_processing_input` adiciona campos
anuláveis para manter registros legados. Um CHECK permite somente os quatro
campos preenchidos ou os quatro ausentes. O repository nunca cria uma nova
aceitação sem entrada. Escritas SQL fora dessa interface ainda podem inserir
linhas sem metadados; não representam aceitação durável. Consultar a entrada de
um job legado retorna `None`; esse job não pode ser reconhecido como recuperável.
Um futuro `NOT NULL` exige regularização aprovada dos dados, sem valores fictícios.

`size_bytes` usa NUMERIC integral, não BIGINT, preservando valores superiores a
64 bits. Checks rejeitam negativos, frações, NaN e infinitos; SHA-256 é lowercase.
MIME e chave opaca são preservados conforme o contrato.

A deduplicação usa exclusivamente a UNIQUE de `source_event_id`. Somente
SQLSTATE 23505 com a constraint correspondente vira `DuplicateSourceEventError`.
Colisão de PK e outras violações são distintas. Duplicatas, inclusive conflitantes,
sempre geram erro: não são automaticamente consideradas aceitas/equivalentes.
A Etapa 6 deverá comparar identidade, correlação e os metadados persistidos e
fechar a política do envelope: `occurred_at` não é persistido nesta entrega.
Resultado de commit incerto por perda de conexão também exige reconciliação por
`source_event_id`, nunca ACK antecipado. Não há promessa de exatamente uma vez.

Os limites físicos do PostgreSQL continuam relevantes: TEXT não aceita U+0000,
e IDs muito longos podem exceder o limite do índice B-tree. O contrato público
não foi restringido; esses casos precisam de avaliação antes de declarar suporte
operacional irrestrito. Nenhum truncamento ou alteração silenciosa é aplicado.

## SQLite e dados legados

SQLite saiu do fluxo operacional. Arquivos locais existentes não devem ser
apagados nem sobrescritos. A estratégia é PostgreSQL novo, sem transferência
automática. Alembic migra schema, não copia dados entre bancos.
Jobs antigos não contêm os metadados necessários à recuperação: sua transferência
ou regularização exige inventário, backup e decisão própria. Downgrade da revisão
0002 remove os metadados; execute somente em banco descartável ou com aprovação
específica e backup. Nenhuma migration deve ser aplicada em produção por estes
comandos de desenvolvimento.

## Testes e validações

Testes de domínio, contrato e HTTP não exigem PostgreSQL:

```bash
uv run pytest -m 'not persistence'
```

Para persistência, provisione um banco exclusivo com nome terminado em `_test`
e um proprietário com permissão de criar schemas. A suíte exige configuração
explícita e falha se ela estiver ausente. As fixtures criam schemas aleatórios,
aplicam migrations e removem somente os schemas criados por elas. Não há cleanup
do banco operacional nem transação externa mascarando commits do repository.

```bash
PROCESSING_TEST_DATABASE_URL='postgresql+psycopg://<test-user>:<encoded-test-password>@127.0.0.1:5432/processing_test' uv run pytest -m persistence
```

Configure essa variável no ambiente para executar a suíte completa:

```bash
uv lock --check
uv sync --locked
uv run task lint
uv run ruff format --check
uv run task typecheck
uv run pytest
uv run task test
uv run typos
git diff --check
docker compose config --quiet
docker build -t docpipe-processing:local .
```

`task test` exige cobertura mínima de 80% e gera HTML em `htmlcov/`. A CI preserva
`quality`, `tests` e `container-smoke`: MyPy no primeiro, PostgreSQL real no segundo,
e migrations da imagem, execução não privilegiada e saúde/métricas no terceiro.
As credenciais da CI são sintéticas e seus bancos são descartáveis.

O [contrato público](docs/contracts/document_received_v1.md) usa fixtures próprias.
O [protocolo experimental](experiments/README.md) permanece futuro para os cenários
que exigem worker, armazenamento e extração. Não há dependência de Ingestion,
Triage ou Registry para as capacidades implementadas.
