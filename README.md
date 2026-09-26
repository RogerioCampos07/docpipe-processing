# Python Project Template

Template base para iniciar projetos Python da organização **Campos Lab** com uma configuração mínima, padronizada e pronta para evolução.

## Visão geral

Este repositório serve como base para novos projetos Python e inclui:

- estrutura simples de aplicação
- gerenciamento do projeto com `pyproject.toml`
- testes automatizados com `pytest`
- lint e formatação com `ruff`
- automação de tarefas com `taskipy`
- containerização com `Docker`
- arquivo `docker-compose.yml` como base para futura configuração de serviços locais

## Estrutura do projeto

- `src/` (opcional/recomendado): diretório para o código da aplicação.
- `main.py`: ponto de entrada da aplicação. Atualmente funciona como placeholder e deve ser substituído pela lógica principal do projeto.
- `tests/`: diretório para testes automatizados.
- `pyproject.toml`: configuração do projeto, dependências e tarefas.
- `Dockerfile`: definição da imagem para executar a aplicação em container.
- `docker-compose.yml`: placeholder para orquestração local, a ser configurado conforme as necessidades de cada projeto.

## Pré-requisitos

- Python 3.13+ para execução local (`Dockerfile` usa Python 3.14 por padrão)
- `uv` instalado
- opcionalmente: Docker e Docker Compose

> Para manter maior paridade entre o ambiente local e o container, prefira utilizar Python 3.14 durante o desenvolvimento.

## Criando um projeto a partir do template

1. No GitHub, clique em **Use this template** e crie um novo repositório para o projeto.

2. Clone o novo repositório:

   ```bash
   git clone <url-do-novo-repositorio>
   cd <nome-do-repositorio>
