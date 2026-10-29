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
conceituais e nomes provisórios não são contratos definitivos. Preserve também
planejamento aprovado ainda não commitado. Diferencie IMPLEMENTADO (código),
DECIDIDO (requisito aprovado), PLANEJADO (execução futura) e PENDENTE (escolha
que exige avaliação). Autorização documental não autoriza implementação técnica
ou alteração de schema.

## Responsabilidade e limites do serviço

O Processing é um microserviço independente. Ele consome informações sobre
documentos de qualquer produtor autorizado e compatível com o contrato público
versionado, recupera o original pela abstração de
armazenamento definida para o projeto, processa-o, persiste estado e resultado
próprios, disponibiliza recursos por API HTTP e, quando o contrato estiver
definido, publica o resultado. Consulte o estado implementado no DESIGN 2.2;
essa responsabilidade não significa que todo o fluxo já exista.

- Respeite a diretriz comum de autonomia em `docs/DESIGN.md`, seção 2.1:
  Ingestion, Processing, Triage e Registry devem possuir API HTTP, produto de domínio,
  dados próprios e utilidade sem os demais em execução. RabbitMQ não substitui
  HTTP. Não dependa de testes de outro repositório; use produtores sintéticos.
- Mantenha repositório, domínio, banco, migrations, configuração, imagem,
  testes e CI próprios do Processing.
- Não importe código interno, modelos ORM ou classes de domínio de outro
  serviço; não acesse seu banco, tabelas ou filesystem privado. A integração
  ocorre exclusivamente por contratos públicos versionados, com representação
  própria em cada serviço. Referências a objetos devem estar no contrato e
  ser acessadas por interfaces públicas e adaptadores próprios.
- Não exija checkout ou execução de outro microsserviço para instalar,
  construir, iniciar, testar ou implantar o Processing. Infraestrutura própria
  necessária é permitida; compartilhar uma instância não permite compartilhar
  estado interno. A API de domínio é obrigatória; implemente somente o escopo
  autorizado no DESIGN 7.2, com consultas independentes do consumer/broker.
  Não crie interfaces adicionais sem utilidade funcional.
- Antes de alterar uma fronteira, verifique os impactos sobre produtores e
  consumidores, compatibilidade e implantação independente. Não exija mudança
  coordenada de domínio em outros serviços; preserve a possibilidade de
  substituí-los por sistemas autorizados e compatíveis com o contrato.
- Preserve os tipos, formatos e regras dos identificadores vigentes, incluindo
  `correlation_id` opcional e anulável no contrato de entrada. A especificação
  está em `docs/contracts/document_received_v1.md`.
- Trate a chave de armazenamento recebida como opaca; não pressuponha caminhos
  internos do Ingestion.
- Preserve entrega pelo menos uma vez, tolerância a duplicatas e idempotência.
  Não prometa processamento exatamente uma vez.
- Modele processamento assíncrono, confirmação, retomada e falhas de forma
  explícita e testável.
- Preserve estados e invariantes do `ProcessingJob` implementado. Diferencie-o
  de `ProcessingResult`, produto técnico futuro da Etapa 8, sem schema completo
  antecipado. Não classifique documentos ou aplique validações de negócio do
  Triage; o `DocumentRecord` consolidado pertence ao Registry.
- Mantenha domínio independente de HTTP, ORM e AMQP; application handler sem
  objetos RabbitMQ e worker separado da API, conforme DESIGN 2.3.
- Na Etapa 6, crie job RECEIVED com attempts=0, UUID próprio e deduplicação por
  source_event_id. Job e entrada necessária à continuidade devem ser gravados
  atomicamente no PostgreSQL próprio antes do ACK. ACK confirma aceitação
  durável, não conclusão; também cabe para duplicata equivalente já persistida
  e recuperável. Não incremente tentativas por redelivery nem confirme sucesso
  em falhas transitórias. Rejeite inválidas permanentes sem requeue, com DLQ
  simples; preserve W3C Trace Context nos headers.
- Não implemente o consumer antes da persistência durável necessária. Modelo
  físico, constraints, índices, migrations, dados existentes e recuperação
  exigem planejamento e autorização posteriores (DESIGN 5.3).
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
- parâmetros de retry/DLQ, headers exatos, equivalência de duplicatas,
  conflitos e recuperação/reprocessamento, preservando o ACK durável já decidido;
- modelo físico da entrada durável, índices e estratégia de migrations;
- mudanças no contrato público de entrada vigente e contratos ainda abertos
  de saída ou falha;
- motor, idiomas ou política de OCR e extração;
- formato, retenção ou estratégia definitiva para resultados;
- detalhes de consumidores futuros.

Antes de depender desses dados, procure evidência na documentação, no código,
nos testes e nos contratos disponíveis. A consulta ao `docpipe-ingestion` serve
somente como evidência de compatibilidade; não presuma acesso ao repositório
nem torne essa consulta requisito de build ou execução. Se os artefatos
não estiverem disponíveis localmente, registre exatamente o que precisa ser
validado externamente. Não recrie nem altere contratos do Ingestion por
conveniência.

Se surgir evidência contra uma decisão do DESIGN, não a altere silenciosamente:
descreva o conflito, a proposta e os impactos e aguarde decisão quando a
mudança for arquiteturalmente relevante. Só então atualize a documentação
correspondente.

## Stack e persistência

Siga a política homologada no DESIGN 10.1: Python 3.14, PostgreSQL e RabbitMQ
nas imagens ali registradas. Não atualize versões sem escopo autorizado.
Bibliotecas compartilhadas seguem a referência do Ingestion ou superior
compatível; preserve lockfile próprio e valide por testes e CI. Não adicione
bibliotecas específicas de outros serviços sem necessidade funcional.

A migração PostgreSQL precede a Etapa 6. Preserve SQLAlchemy ORM, Alembic e seu
histórico, integridade referencial, constraints, UUID, timestamps UTC e unicidade
de source_event_id. Mantenha banco lógico e credenciais próprios, configuração
externa e sessões/transações com rollback correto. Valide a migration inicial
no PostgreSQL; não apague ou reescreva histórico indiscriminadamente, nem
presuma descarte de dados SQLite sem inventário e decisão explícita.

## Desenvolvimento incremental

Para cada etapa solicitada:

1. confirme o estado atual e o critério de conclusão da etapa;
2. identifique o menor slice implementável;
3. implemente somente o necessário para esse slice;
4. adicione ou atualize os testes correspondentes;
5. execute as validações aplicáveis;
6. documente apenas decisões que tenham sido efetivamente tomadas;
7. reporte entregas, pendências, riscos e limitações.

Preserve as entregas das Etapas 1 a 5. O PLAN posiciona alinhamento de stack e
PostgreSQL, depois API HTTP de domínio, antes da Etapa 6. Esta aceita entradas;
não antecipe recuperação (7), pipeline/ProcessingResult (8) ou saída/outbox (9),
nem implemente reprocessamento manual sem autorização.

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
uv run task typecheck
uv run pytest
uv run task test
uv run typos
```

`uv run task format` altera arquivos. Use-o apenas quando formatação fizer
parte da tarefa. `task test` cobre o pacote existente `docpipe_processing`,
exige 80% e gera relatório HTML; não declare cobertura sem executar a validação.
MyPy estrito está configurado, mas sua inclusão na CI ainda é planejada.
Preserve os checks `quality`, `tests` e `container-smoke`; verifique o ruleset
antes de alterações, sem renomear checks por conveniência.

Testes de domínio e contrato não devem exigir broker ou banco. Na futura
migração, valide persistência em PostgreSQL real isolado; no consumer, cubra
atomicidade, concorrência e reinicialização com produtor sintético. Execute
build, integrações e smoke tests somente com artefatos e dependências
existentes. Informe validações não executadas e o motivo. Em tarefas somente
documentais, confira diff, whitespace e escopo, sem instalar dependências ou
executar testes técnicos desnecessários.

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
