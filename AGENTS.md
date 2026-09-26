# Instruções para agentes de código

## Escopo e fontes de verdade

Estas instruções valem para todo o repositório `docpipe-processing`.
Instruções mais específicas em subdiretórios prevalecem apenas no respectivo
escopo. A solicitação atual do usuário tem prioridade sobre este guia.

Antes de alterar qualquer arquivo:

1. inspecione o estado atual do repositório e as mudanças preexistentes;
2. leia `docs/PLAN.md` e identifique a etapa solicitada;
3. consulte no `docs/DESIGN.md` as decisões e os limites relacionados;
4. examine código, testes, contratos e configuração existentes;
5. implemente apenas o menor incremento necessário para a etapa atual.

Use esta hierarquia documental:

1. `AGENTS.md`: regras operacionais de trabalho;
2. `docs/DESIGN.md`: arquitetura, responsabilidades, limites e decisões;
3. `docs/PLAN.md`: ordem planejada de implementação.

O DESIGN define o que construir; o PLAN governa quando construir. Não avance
automaticamente para etapas posteriores. Propostas, exemplos, estados
conceituais e nomes provisórios não são contratos definitivos.

## Responsabilidade e limites do serviço

O Processing é um microserviço independente. Ele consome informações sobre
documentos aceitos pelo Ingestion, recupera o original pela abstração de
armazenamento definida para o projeto, processa-o, persiste estado e resultado
próprios e, quando o contrato estiver definido, publica o resultado.

- Mantenha banco, migrations, imagem, testes e CI próprios do Processing.
- Nunca acesse diretamente o banco, tabelas ou filesystem privado do
  Ingestion. A integração ocorre por contratos/eventos e armazenamento
  explicitamente compartilhado.
- Trate a chave de armazenamento recebida como opaca; não pressuponha caminhos
  internos do Ingestion.
- Preserve entrega pelo menos uma vez, tolerância a duplicatas e idempotência.
  Não prometa processamento exatamente uma vez.
- Modele processamento assíncrono, confirmação, retomada e falhas de forma
  explícita e testável.
- Não altere o registro do Ingestion para representar estado do Processing.
- Não assuma que consumidores ou microserviços posteriores já existam.
- Emita apenas sinais de observabilidade do próprio serviço. A stack
  centralizada de coleta e visualização não pertence a este repositório.
- Não introduza Kubernetes, AKS, recursos Azure reais ou infraestrutura de
  produção na implementação inicial. A `v1.0.0` deve permanecer local e
  reproduzível.

## Decisões abertas e contratos externos

Consulte sempre a seção de decisões pendentes do DESIGN. Não invente nem fixe
sem evidência:

- topologia RabbitMQ, exchange, routing keys, filas ou bindings;
- política de ACK, retry, DLQ, headers ou reprocessamento;
- contratos finais dos eventos de entrada, saída ou falha;
- motor, idiomas ou política de OCR e extração;
- formato, retenção ou estratégia definitiva para resultados;
- detalhes de consumidores futuros.

Antes de depender desses dados, procure evidência na documentação, no código,
nos testes e nos contratos disponíveis. Quando o DESIGN exigir confronto com
o `docpipe-ingestion`, não presuma acesso ao outro repositório. Se os artefatos
não estiverem disponíveis localmente, registre exatamente o que precisa ser
validado externamente. Não recrie nem altere contratos do Ingestion por
conveniência.

Se surgir evidência contra uma decisão do DESIGN, não a altere silenciosamente:
descreva o conflito, a proposta e os impactos e aguarde decisão quando a
mudança for arquiteturalmente relevante. Só então atualize a documentação
correspondente.

## Desenvolvimento incremental

Para cada etapa solicitada:

1. confirme o estado atual e o critério de conclusão da etapa;
2. identifique o menor slice implementável;
3. implemente somente o necessário para esse slice;
4. adicione ou atualize os testes correspondentes;
5. execute as validações aplicáveis;
6. documente apenas decisões que tenham sido efetivamente tomadas;
7. reporte entregas, pendências, riscos e limitações.

Evite abstrações sem uso, grandes implementações antecipadas e refactors não
relacionados. Não trate placeholders do template como decisões arquiteturais.
Confirme no repositório antes de adicionar frameworks, dependências, serviços,
filas, bancos ou uma estrutura de pacotes.

## Qualidade e validações

Amplie a suíte à medida que as capacidades existirem: testes unitários, de
integração, contrato, persistência, consumidor, armazenamento e fluxo; lint,
cobertura, build, smoke tests e GitHub Actions. Não crie testes artificiais
para aumentar cobertura. Testes devem ser determinísticos, usar dados
sintéticos e isolar fronteiras externas quando apropriado.

O estado real de `pyproject.toml`, `uv.lock`, Dockerfile e CI é a fonte de
verdade. Verifique os comandos antes de declará-los ou executá-los. Atualmente,
as ferramentas básicas configuradas incluem:

```bash
uv sync --locked
uv run task lint
uv run ruff format --check
uv run pytest
uv run typos
```

`uv run task format` altera arquivos. Use-o apenas quando formatação fizer
parte da tarefa. O alvo atual de `task test` pressupõe o pacote
`docpipe_processing`; enquanto ele não existir, valide com `uv run pytest` e
não declare cobertura válida para um alvo inexistente. Execute build de imagem,
integrações e smoke tests somente quando seus artefatos e dependências já
existirem. Se uma validação não puder ser executada, informe o motivo.

Ao adicionar dependências, use `uv add` (e `uv add --dev` para desenvolvimento),
não edite `uv.lock` manualmente e mantenha lockfile e `pyproject.toml`
coerentes.

## Segurança, dados e observabilidade

- Trate documentos e metadados como entrada não confiável e aplique limites
  configuráveis de tamanho, páginas, pixels, tempo e recursos quando a etapa
  correspondente for implementada.
- Nunca registre conteúdo do documento ou texto extraído, nem os inclua em
  métricas, traces, eventos ou respostas operacionais.
- Não inclua credenciais, tokens, `.env` ou segredos no código, no repositório
  ou na imagem; use configuração externa e privilégio mínimo.
- Mantenha originais e resultados privados, em responsabilidades de
  armazenamento explicitamente definidas; resultados nunca sobrescrevem o
  original.
- Evite identificadores de alta cardinalidade como labels de métricas.
- Diferencie falhas temporárias, permanentes e entradas inválidas sem criar
  ciclos infinitos de reentrega.

## Experimentos

Os ensaios do TCC devem começar pequenos, reproduzíveis, seguros e baseados em
documentos sintéticos. Registre parâmetros e ambiente. Não invente metas de
desempenho antes do baseline nem converta resultados experimentais em
requisitos arquiteturais sem evidência. Carga pesada e cenários caros de
resiliência não devem rodar em toda pull request.

## Disciplina de Git e entrega

- Preserve alterações preexistentes e não reformate arquivos sem relação com
  a tarefa.
- Prefira mudanças pequenas e revisáveis.
- Não faça commit, merge, push, release ou mudança de branch sem solicitação
  explícita.
- Não modifique arquivos apenas por preferência estética.

Ao concluir, informe:

1. arquivos alterados;
2. testes e validações executados, com resultados;
3. decisões tomadas;
4. decisões ainda pendentes;
5. riscos ou limitações encontrados.
