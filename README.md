# DocPipe Processing

Microserviço de processamento de documentos do DocPipe. Nesta primeira etapa, o
repositório oferece apenas uma aplicação HTTP mínima para validar a execução
isolada e o ambiente de desenvolvimento. O processamento de documentos ainda
não está disponível.

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

Em outro terminal, confira a resposta HTTP:

```bash
curl --fail-with-body http://127.0.0.1:8000/health
```

Resposta esperada: `{"status":"ok"}`. A rota indica apenas que a aplicação está
respondendo; ela não verifica dependências futuras.

## Verificações

```bash
uv run task lint
uv run ruff format --check
uv run pytest
uv run task test
uv run typos
```

Os testes atuais verificam somente a rota de saúde. A tarefa `task test` gera um
relatório de cobertura em `htmlcov/`.

## Estrutura atual

- `docpipe_processing/app.py`: aplicação HTTP e rota de saúde.
- `main.py`: inicializador local.
- `tests/`: testes automatizados do comportamento implementado.
- `pyproject.toml` e `uv.lock`: dependências e ferramentas de desenvolvimento.

O `Dockerfile` e o `docker-compose.yml` vêm do template. A configuração e a
validação da execução em container pertencem a uma etapa posterior.
