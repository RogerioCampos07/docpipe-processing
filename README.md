# DocPipe Processing

Microserviço de processamento de documentos do DocPipe. O repositório inclui
uma aplicação HTTP mínima e persistência própria para o estado dos trabalhos.
A extração e o processamento dos documentos ainda não estão disponíveis.

## Autonomia do serviço

A responsabilidade de negócio prevista é transformar documentos referenciados
por eventos compatíveis em resultado e estado próprios, com utilidade para
qualquer produtor autorizado que cumpra o contrato. Isso deve funcionar sem
Ingestion ou consumidores posteriores em execução. A diretriz obrigatória,
os limites e as lacunas atuais estão no [DESIGN](docs/DESIGN.md), seção 2.

Hoje, o repositório oferece validação de contrato, domínio e persistência
próprios, além da API operacional. Instalação, build, inicialização e testes
usam este repositório, sem checkout ou runtime de outro microsserviço. O fluxo
completo ainda depende das etapas futuras de consumidor, recuperação do objeto,
extração e publicação; sua autonomia de negócio ainda precisa ser demonstrada.

Operar isoladamente permite a infraestrutura necessária ao serviço: SQLite
local está disponível; RabbitMQ e armazenamento de objetos são integrações
previstas, ainda não implementadas. Compartilhar infraestrutura não permite
acessar tabelas, modelos ou filesystem privado de outro serviço. Não é
necessário inventar outra API ou CLI para cumprir essa diretriz.

## Requisitos

- Python 3.13 ou superior (a versão local indicada em `.python-version` é 3.14.4)
- `uv`

## Execução local

```bash
uv sync --locked
uv run python main.py
```

A aplicação escuta em `127.0.0.1:8000` por padrão. Configure `PROCESSING_HOST` e
`PROCESSING_PORT` no ambiente para alterar o endereço e a porta, respectivamente.
Não há serviços externos necessários para esta etapa.

## Contrato de entrada

O Processing valida o contrato público versionado `document.received.v1` sem
depender do runtime ou do código de qualquer produtor. A especificação está em
[`docs/contracts/document_received_v1.md`](docs/contracts/document_received_v1.md).
Execute os testes sintéticos do contrato com:

```bash
uv run pytest -m contract
```

## Persistência local

O banco local inicial é SQLite. Por padrão, o Processing usa
`sqlite:///./processing.db`; configure `PROCESSING_DATABASE_URL` para indicar
outra URL de conexão. O arquivo padrão é local e não deve ser compartilhado
com o Ingestion.

Prepare ou atualize o schema aplicando as migrations versionadas:

```bash
uv run alembic upgrade head
```

Os testes de persistência usam bancos SQLite temporários e podem ser executados
com:

```bash
uv run pytest -m persistence
```

Em outro terminal, confira a resposta HTTP:

```bash
curl --fail-with-body http://127.0.0.1:8000/health
```

Resposta esperada: `{"status":"ok"}`. A rota indica apenas que a aplicação está
respondendo; ela não verifica dependências futuras.

## Verificações

```bash
uv sync --locked
uv run task lint
uv run ruff format --check
uv run pytest
uv run task test
uv run typos
docker build -t docpipe-processing:local .
```

`uv run task test` aplica o limite inicial de 80% de cobertura para
`docpipe_processing` e gera um relatório HTML em `htmlcov/`. A CI executa lint,
formatação, typos, testes com cobertura e o smoke test do container em Pull
Requests.

Para validar a imagem localmente, inicie-a e consulte a saúde e as métricas:

```bash
docker run --rm -d --name docpipe-processing-smoke -p 127.0.0.1:18000:8000 docpipe-processing:local
curl --fail http://127.0.0.1:18000/health
curl --fail http://127.0.0.1:18000/metrics
docker stop docpipe-processing-smoke
```

O container escuta em `0.0.0.0:8000`; a execução local continua usando
`127.0.0.1:8000` por padrão. `/health` verifica apenas que a API responde.
`/metrics` expõe métricas HTTP e do processo com rótulos de baixa cardinalidade.
Os logs da API são JSON e não incluem corpos de requisição. A API extrai o
contexto W3C `traceparent`; a exportação OTLP fica desativada por padrão e pode
ser habilitada definindo `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`.

O protocolo experimental inicial e suas dependências estão em
[`experiments/README.md`](experiments/README.md). Os cenários que dependem de
worker, RabbitMQ, storage ou extração ainda não são executáveis; a persistência
própria já possui testes isolados.

## Estrutura atual

- `docpipe_processing/app.py`: aplicação HTTP e rota de saúde.
- `docpipe_processing/observability.py`: logs JSON da aplicação.
- `docpipe_processing/domain.py`: modelo de domínio imutável do trabalho.
- `docpipe_processing/persistence.py`: ORM, configuração de banco e repository.
- `migrations/`: migrations versionadas do banco próprio do Processing.
- `main.py`: inicializador local.
- `tests/`: testes automatizados do comportamento implementado.
- `.github/workflows/ci.yml`: checks de qualidade, testes e smoke do container.
- `pyproject.toml` e `uv.lock`: dependências runtime e ferramentas de desenvolvimento.

O `docker-compose.yml` permanece sem serviços auxiliares nesta etapa.
