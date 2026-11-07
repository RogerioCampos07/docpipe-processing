# Design do DocPipe Processing

## 1. Contexto e premissas

O `docpipe-processing` é um microserviço independente cuja responsabilidade é aceitar duravelmente entradas compatíveis com `document.received.v1`, localizar o original por adaptador próprio de armazenamento, executar processamento técnico, registrar estado e resultado próprios, disponibilizar recursos por API HTTP e publicar resultados por contratos públicos versionados. A entrada pode vir de qualquer produtor autorizado e compatível, incluindo o `docpipe-ingestion`. O aceite HTTP do Ingestion não depende da conclusão desta etapa.

Este documento define arquitetura, fronteiras, componentes, fluxos, decisões e trade-offs da `v1.0.0`. A integração tem como fonte o contrato público versionado; código e testes do Ingestion, quando disponíveis, servem apenas como evidência de compatibilidade daquele produtor. Sua inspeção não cria dependência entre repositórios. O [PLAN](PLAN.md) governa etapas, entregas, dependências, ordem e critérios de conclusão; o [AGENTS](../AGENTS.md) define regras operacionais de desenvolvimento. A preparação de qualidade, observabilidade e experimentos da Etapa 2 será ampliada conforme os componentes existirem, sem reabrir as entregas anteriores nem antecipar conclusões experimentais.

Neste documento, **IMPLEMENTADO** indica evidência no código atual; **DECIDIDO**, requisito arquitetural aprovado; **PLANEJADO**, execução técnica futura; e **PENDENTE**, escolha ainda sujeita a avaliação e aprovação. Decisão documentada não significa funcionalidade implementada.

## 2. Responsabilidades e limites

| Responsabilidade do Processing | Limite |
| --- | --- |
| Receber e validar `document.received.v1` do RabbitMQ | Não receber uploads de clientes nem modificar o evento do produtor |
| Registrar atomicamente o job aceito e a entrada durável necessária à continuidade | Não depender da mensagem na fila ou de dados privados do produtor após o ACK |
| Recuperar e validar o original referenciado pelo evento | Não gravar no banco privado do Ingestion |
| Extrair texto de PDF, PNG e JPEG dentro de limites configuráveis | Não classificar documentos nem aplicar validações de negócio pertencentes ao Triage |
| Persistir estado, tentativas, diagnóstico técnico e referência ao resultado | Não tratar o storage original como banco de estado |
| Disponibilizar API HTTP de domínio e sinais operacionais próprios | Não hospedar a stack central de observabilidade nem exigir broker para consultas ao banco |
| Emitir um evento de conclusão após persistir o resultado | Não pressupor que consumidores posteriores já existam |

O resultado de processamento não altera o registro de ingestão. O Triage será responsável por classificação, validação de negócio e encaminhamento; o Registry, pelo `DocumentRecord` consolidado. Essas responsabilidades não pertencem ao Processing e não autorizam acoplamento aos modelos internos dos demais serviços.

### 2.1. Decisão obrigatória: autonomia dos microsserviços

**DECIDIDO:** o DocPipe v1.0.0 possui quatro microsserviços: `docpipe-ingestion`, `docpipe-processing`, `docpipe-triage` e `docpipe-registry`. Todos devem ser desacoplados, independentes e possuir utilidade própria. Cada um deve executar sua responsabilidade de negócio delimitada sem exigir outros microsserviços em execução.

- Cada serviço deve possuir repositório, API HTTP, produto de domínio, banco de dados, migrations, configuração, imagem, testes e CI próprios, com responsabilidade de domínio identificável e contratos públicos versionados.
- Instalação, build, inicialização, testes e implantação não podem exigir checkout nem execução de outro microsserviço. A evolução e a implantação são independentes, respeitando a compatibilidade dos contratos públicos.
- A integração ocorre exclusivamente por contratos públicos e versionados. Cada serviço mantém sua própria representação; produtores e consumidores autorizados e compatíveis podem ser substituídos sem mudanças no domínio do serviço.
- É proibido importar código interno, classes de domínio ou modelos ORM de outro serviço, consultar suas tabelas ou depender de seu filesystem privado. Por exemplo: importar `DocumentReceivedEvent` do pacote do Ingestion, consultar o banco de ingestão para completar um evento ou montar seu diretório privado de documentos no Processing.
- Referências externas a documentos e objetos devem estar explícitas no contrato. O acesso ocorre por interfaces públicas de armazenamento e adaptadores próprios, usando referências opacas, sem descobrir caminhos ou consultar estruturas internas do produtor.
- Banco de dados, RabbitMQ e armazenamento de objetos continuam sendo dependências legítimas. Operação isolada pressupõe a infraestrutura necessária ao próprio serviço; compartilhar uma instância física não autoriza compartilhar bancos lógicos, tabelas privadas, modelos internos ou código de domínio.
- A colaboração assíncrona preserva entrega pelo menos uma vez, idempotência e rastreabilidade conforme os contratos e políticas aplicáveis. Autonomia não promete processamento exatamente uma vez nem altera identificadores ou regras de correlação.
- RabbitMQ integra os serviços de forma assíncrona e não substitui suas APIs HTTP. A API de domínio do Processing atende à consulta e rastreabilidade de recursos próprios; interfaces adicionais exigem utilidade funcional, sem criar modos artificiais apenas para alegar autonomia.

**Aplicação ao DocPipe:** receber, registrar e armazenar documentos dá utilidade própria ao Ingestion, sem exigir Processing nem conclusão das etapas posteriores. O Processing deve transformar documentos referenciados por entradas compatíveis em resultado e estado próprios, consultáveis por HTTP, sem código, banco, testes ou runtime do Ingestion, Triage ou Registry. Não se exige consumidor posterior em execução para concluir sua responsabilidade; a publicação segue as garantias de entrega definidas. Testes com produtores sintéticos e infraestrutura própria devem demonstrar essa independência.

### 2.2. Estado observado e evidências ainda necessárias

O estado implementado deve ser distinguido do fluxo previsto nas seções seguintes:

- `docpipe_processing/contracts.py` valida o contrato com modelos próprios; `tests/test_contracts.py` usa payloads e fixture JSON locais, sem produtor ou broker.
- `docpipe_processing/domain.py` define `ProcessingJob` sem importar o contrato externo. `persistence.py` e `migrations/` usam PostgreSQL próprio, mantêm entrada durável na linha de job e unicidade de `source_event_id`; os testes de persistência usam schemas isolados.
- `application.py` e `api.py` implementam a consulta HTTP versionada. `acceptance.py` coordena validação, equivalência e criação; `consumer.py` trata transporte e ACK; `worker.py` compõe processo AMQP separado da API.
- `pyproject.toml`, `Dockerfile`, Compose e CI usam dependências, build e testes do Processing. A API expõe `/health`, `/metrics` e `GET /v1/processing-jobs/{processing_id}`. O worker aceita eventos, mas não executa o pipeline documental.

Na inspeção desses artefatos não foi encontrada dependência técnica do código, banco ou runtime do Ingestion. A aceitação durável foi exercitada com produtor sintético e PostgreSQL/RabbitMQ próprios. Recuperação do objeto, extração e publicação ainda não estão implementadas. Autorização de produtores não é garantida pela validação estrutural do payload.

O repositório do Ingestion não estava disponível nesta revisão; sua compatibilidade e operação independente não foram verificadas. Esta decisão registra a obrigação arquitetural, sem afirmar adequações técnicas concluídas em outros serviços.

**IMPLEMENTADO:** PostgreSQL próprio, entrada durável, consulta HTTP e consumer da Etapa 6. `ProcessingJob` e o contrato público foram preservados. `ProcessingResult`, recuperação e publicação continuam futuros.

### 2.3. Componentes e direção das dependências

- **Domínio:** `ProcessingJob` e suas invariantes, sem objetos FastAPI, SQLAlchemy ou RabbitMQ e sem dependência do contrato externo. `ProcessingResult` será definido na Etapa 8.
- **Aplicação:** coordena os casos de uso e a persistência necessária à aceitação; o application handler será independente de objetos de transporte.
- **Transporte HTTP:** valida parâmetros e projeta respostas de recursos persistidos por meio do repository próprio, sem chamar o consumer.
- **Transporte AMQP:** usa `aio-pika` em worker separado da API, valida a entrada e traduz o resultado da aceitação em ACK ou rejeição.
- **Infraestrutura:** SQLAlchemy/repository, PostgreSQL e adaptador próprio de armazenamento implementam o acesso aos recursos externos; não acessam dados privados de outros serviços.

Esses limites orientam as futuras implementações sem exigir reorganização antecipada de pacotes, frameworks adicionais ou abstrações genéricas sem uso.

## 3. Contrato de entrada

O contrato de entrada implementado tem nome lógico `document.received.v1` e envelope com `event_id`, `event_type: document.received`, `event_version: 1`, `occurred_at`, `correlation_id`, `document_id` e `data.storage_key`, `data.media_type`, `data.size_bytes`, `data.sha256`. As regras vigentes estão na [especificação pública](contracts/document_received_v1.md): IDs externos são strings opacas não vazias nem compostas apenas de whitespace; `correlation_id` pode estar ausente, ser `null` ou uma string válida. O `processing_id` interno continua sendo UUID, distinto dos IDs externos. O corpo não contém arquivo, URL pública ou credenciais. W3C Trace Context pertence aos headers de transporte e o worker o extrai dos headers disponíveis.

O exchange `docpipe.events`, tipo `direct`, e a routing key `document.received.v1` são defaults configuráveis da referência aprovada pelo usuário, não um contrato definitivo do produtor. O checkout do Ingestion não estava disponível; sua compatibilidade precisa de confirmação externa. O Processing usa fila própria e valida a estrutura do evento. Conferência de tamanho e checksum do objeto pertence à recuperação futura.

A garantia arquitetural de entrega permanece **pelo menos uma vez**, inclusive na integração prevista com o Ingestion. `event_id` é a chave para deduplicar a entrega; `document_id` identifica o documento. O recebimento repetido do mesmo evento não pode disparar duas conclusões lógicas nem duplicar resultados. Uma nova versão ou solicitação explícita de reprocessamento exigirá contrato próprio, sem reutilizar silenciosamente a mesma chave de idempotência.

## 4. Fluxo de processamento

### 4.1. Aceitação durável — Etapa 6

**IMPLEMENTADO:** worker `aio-pika` separado da API, application handler independente dos objetos RabbitMQ, validação pelo contrato público e extração de Trace Context.

```text
evento válido → validação → job RECEIVED + entrada durável → commit → ACK
```

Criar `ProcessingJob.RECEIVED`, com `attempts=0`, UUID próprio em `processing_id` e `event_id` em `source_event_id`. Job e informações necessárias à continuidade serão gravados atomicamente no PostgreSQL próprio. **ACK significa aceitação durável da entrada, não conclusão do processamento documental.** Após reinicialização, não depender da mensagem original nem do runtime do produtor.

ACK somente após commit da entrada nova ou confirmação de duplicata equivalente já persistida e recuperável. A equivalência compara `event_id`, `document_id`, `correlation_id`, `storage_key`, `media_type`, `size_bytes` e `sha256`; `occurred_at` é excluído. Redelivery não cria outro job nem incrementa tentativas. Conflitos e jobs legados sem entrada completa seguem para quarentena. A constraint UNIQUE de `source_event_id` protege concorrência.

O consumer usa ACK manual, duas tentativas locais adicionais (1 e 5 segundos) e, após falhas do PostgreSQL, mantém a entrega sem ACK, pausa novas entregas por `prefetch=1` e sonda o banco com backoff e jitter. Resultado incerto de commit é reconciliado pela chave no banco primário. Erros inesperados suspendem o worker; não são tratados como ausência nem sucesso.

A fila de entrada é quorum, com dead-lettering `at-least-once`, `reject-publish` e `x-delivery-limit=-1`. Testes no RabbitMQ 4.3.6 observaram 26 redeliveries sem descarte por delivery limit, preservação de mensagem quando a DLQ cheia recuperou capacidade e mensagens persistentes após reinício do broker. Antes de rejeitar, o worker verifica/recria a topologia de quarentena. Na v1.0.0 local, RabbitMQ roda em nó único: fila quorum não oferece alta disponibilidade e a recuperação observada depende da preservação dos dados persistentes. Perda de volume, perda definitiva do nó e comportamento de cluster não foram testados nem são cobertos; também não há prova contra corrida com exclusão administrativa. Proteja os recursos do Processing e acompanhe quorum e filas.

Queda antes do commit não permite ACK; depois do commit e antes do ACK pode produzir redelivery reconhecida pela chave. Testes de resiliência descartáveis cobrem indisponibilidade prolongada antes do consumo, reinício do worker durante a falha e interrupção do PostgreSQL quando um trigger de teste observa o INSERT ativo; após a restauração, o job é aceito uma vez e não aparece na DLQ. Teste PostgreSQL/RabbitMQ fecha o canal depois do commit e antes do ACK, confirmando redelivery equivalente sem alteração do job. Fault injection com PostgreSQL real cobre resposta perdida após commit, falha antes da escrita e reconciliação indisponível; não simula perda de rede exatamente durante o protocolo COMMIT.

O aceite não recupera documentos, executa OCR ou extração, cria `ProcessingResult`, publica saída, implementa outbox ou reprocessamento manual.

### 4.2. Execução posterior e publicação

- **Etapa 7:** recuperar o original pela referência persistida e adaptador próprio; verificar tamanho, SHA-256 e limites de recursos.
- **Etapa 8:** executar o pipeline técnico, definir `ProcessingResult` e persistir resultados privados. Motores, OCR, páginas mistas e ausência de texto exigem decisões próprias.
- **Etapa 9:** registrar evento em outbox própria atomicamente com a conclusão persistida; publicador separado envia e registra confirmação do broker, sem exigir consumidor posterior em execução.

Descoberta e recuperação de trabalhos interrompidos, concorrência, lease e retry ainda serão planejados e testados. O aceite não mantém uma entrega AMQP aberta durante OCR longo. Queda entre publicação e registro da confirmação pode causar publicação duplicada; contratos e idempotência precisam contemplá-la.

## 5. Dados e estados próprios

### 5.1. ProcessingJob — IMPLEMENTADO

Recurso de identidade, ciclo de vida, tentativas, timestamps, falhas e rastreabilidade. O modelo imutável contém `processing_id`, `source_event_id`, `document_id`, `correlation_id`, `status`, `attempts`, `created_at`, `updated_at`, `started_at`, `finished_at` e `failure_reason`.

Preservar `RECEIVED`, `PROCESSING`, `COMPLETED`, `FAILED` e suas invariantes: criação com zero tentativas; início incrementa tentativas; conclusão ou falha exige início; timestamps conscientes de timezone são normalizados para UTC e respeitam ordem temporal. IDs externos são strings opacas, correlação é opcional/anulável e a identidade interna é UUID. `RETRY_WAIT`, `next_attempt_at`, lease e reprocessamento não estão implementados; sua eventual adoção exige avaliação posterior, sem alterar estados pela troca do banco.

### 5.2. ProcessingResult — PLANEJADO para a Etapa 8

Produto técnico reutilizável vinculado ao trabalho, distinto do seu ciclo de vida. Não está implementado nem possui schema completo aprovado. Metadados, artefatos privados, vínculo, persistência e acesso serão definidos pelas necessidades reais do pipeline. Não classifica documentos, aplica validações de negócio do Triage ou consolida o `DocumentRecord` do Registry; não depende de seus modelos internos. A política para ausência de texto permanece pendente. Outbox é infraestrutura futura da Etapa 9, não produto de domínio.

### 5.3. Entrada durável — IMPLEMENTADO

Os quatro campos `storage_key`, `media_type`, `size_bytes` e `sha256` estão persistidos como colunas anuláveis na linha do job e validados como todos preenchidos ou todos ausentes. Jobs legados continuam representáveis; não provam aceitação recuperável.

Job e metadados da entrada são criados no mesmo commit PostgreSQL. `occurred_at` não é persistido nem substituído por `created_at`. A constraint UNIQUE de `source_event_id` protege concorrência.

## 6. Persistência e armazenamento

**IMPLEMENTADO:** SQLAlchemy ORM síncrono, repository, sessões, Alembic, PostgreSQL homologado (seção 10.1), Psycopg 3 e configuração externa `PROCESSING_DATABASE_URL`, sem fallback SQLite.

Preservar ORM, histórico Alembic, integridade referencial, constraints, UUID, timestamps UTC e unicidade de `source_event_id`. Cada serviço mantém banco lógico, credenciais, schema sob sua responsabilidade e acesso exclusivo aos próprios dados. API e worker gerenciarão sessões e transações próprias, com rollback correto; distinguir conflito da constraint de evento de outros `IntegrityError`, sem tratar todo erro como duplicata.

As migrations `0001_processing_jobs` e `0002_processing_input` foram validadas em PostgreSQL real de teste. O histórico foi preservado. Alembic migra schema, não transfere dados de SQLite; a migração de dados locais permanece uma decisão operacional separada. Testes de persistência usam schemas isolados para round-trip, UTC, unicidade, rollback e concorrência.

Para integração ponta a ponta, o produtor deve disponibilizar o original ao Processing por interface pública de armazenamento privado, como Azurite no laboratório local, sem exigir seu próprio runtime para a recuperação. A referência anterior ao caminho `dataset/documents/` do Ingestion não constitui contrato nem autoriza montá-lo como volume compartilhado; detalhes internos daquele repositório não foram verificados nesta revisão. O Processing recebe a chave opaca do evento e configura o acesso por seu próprio adaptador e credenciais de mínimo privilégio. Resultados derivados usam namespace/container próprio e nunca sobrescrevem o original.

Se a escrita do artefato derivado for confirmada, mas a transação do banco falhar, pode restar um órfão. A reconciliação deve identificar esse caso antes de qualquer exclusão. O conteúdo extraído não deve aparecer em eventos, logs, métricas ou traces.

## 7. Contratos de saída e API HTTP

### 7.1. Evento de saída — PLANEJADO para a Etapa 9

Nome provisório: `document.processed.v1`. Proposta não definitiva: envelope com identificação, tipo, versão, ocorrência, correlação e documento; dados com `processing_id`, status e referência lógica ao resultado, possivelmente páginas e modo de extração. Contrato e eventual evento de falha precisam ser definidos considerando consumidores compatíveis, sem exigir que estejam implementados ou em execução. Não transportar texto, URL pública ou credenciais; o evento não fixa o schema de `ProcessingResult`.

### 7.2. API HTTP própria — IMPLEMENTADA

Além de `/health` e `/metrics`, está implementado `GET /v1/processing-jobs/{processing_id}`.

A fronteira HTTP consultará `ProcessingJob` persistido pelo repository próprio, com contrato versionado, schema explícito de resposta, tratamento de identificador inválido e recurso inexistente, testes automatizados e documentação OpenAPI. A projeção não expõe modelos ORM; diagnósticos devem ser seguros, sem conteúdo documental ou segredos.

Consultas aos dados persistidos não exigem consumer, RabbitMQ ou Ingestion em execução. Não estão aprovados endpoints adicionais, recuperação do original ou exposição do texto completo. `/health` continua sendo liveness e não implica readiness do worker ou broker.

## 8. Falhas, segurança e privacidade

- Diferenciar erro temporário de broker, banco ou storage, documento ausente/inconsistente, formato inválido e erro do motor de extração.
- Aplicar timeout, backoff com limite, estado terminal e reprocessamento controlado; impedir fila de reentregas sem fim.
- Proteger o worker contra documentos malformados, PDFs grandes ou comprimidos de forma adversarial e custo excessivo de OCR; processar dados sintéticos nos ensaios.
- Nunca incluir conteúdo do documento ou texto extraído em logs, traces, métricas, mensagens ou respostas operacionais.
- Configurar segredos fora da imagem; restringir acesso a originais e resultados e documentar retenção e exclusão antes de dados pessoais reais.
- Fazer o Processing tolerar duplicatas e interrupções sem exigir que o produtor esteja em execução após disponibilizar o evento e o objeto pela infraestrutura contratada.

## 9. Observabilidade e experimentos desde a Etapa 2

**IMPLEMENTADO:** observabilidade HTTP e do worker com logs estruturados, métricas de baixa cardinalidade e tracing. O worker registra entregas, resultados de aceitação, falhas por categoria, suspensão/recuperação, tentativas de ACK, rejeições e duração. Identificadores não são labels; payloads e credenciais não são registrados. A stack centralizada não pertence ao serviço.

A Etapa 2 preparou o protocolo em `experiments/README.md`; isso não comprova baseline do pipeline ainda inexistente. Ensaios pequenos e sintéticos serão ampliados com reentrega, reinício e indisponibilidade quando os componentes existirem. Registrar ambiente, parâmetros, conclusão, latências p50/p95/p99, backlog, tentativas, CPU e memória. Começar com uma instância antes de comparar concorrência; não inventar metas antes do baseline. A campanha de carga e resiliência pertence à Etapa 10, sem carga pesada em toda pull request.

## 10. Execução local e evolução

A arquitetura local planejada usa FastAPI, worker separado, PostgreSQL, RabbitMQ, armazenamento por adaptador próprio, `uv` e Docker Compose próprio. O laboratório integrado exige configuração explícita, sem dependência entre runtimes dos microsserviços. O template não decide a arquitetura de processamento.

### 10.1. Política tecnológica — DECIDIDA

| Tecnologia | Política aprovada |
| --- | --- |
| Python | Linha minor 3.14 comum; selecionar patch por compatibilidade e reprodutibilidade |
| PostgreSQL | Imagem `postgres:17.11-bookworm` para todos os serviços que o utilizarem; dados e migrations independentes |
| RabbitMQ | Imagem `rabbitmq:4.3.6-management` para todos os serviços que o utilizarem; filas, configurações e responsabilidades próprias |
| Bibliotecas compartilhadas | Referência do Ingestion ou superior compatível, sem exigir versões idênticas; respeitar compatibilidade |
| Lockfile | `uv.lock` próprio, resolvido e testado; nenhuma dependência específica de outro serviço sem uso real |

A política abrange FastAPI, Uvicorn, Pydantic, Pydantic Settings, SQLAlchemy, Alembic, OpenTelemetry, Prometheus Client, pytest, pytest-cov, Ruff, MyPy, Typos e Taskipy, condicionadas ao uso real. Consultar referências do Ingestion não o torna dependência de build ou execução.

**IMPLEMENTADO:** alinhamento Python 3.14.4, Docker, Compose, PostgreSQL e MyPy no fluxo de qualidade. As dependências AMQP foram resolvidas e verificadas com Python 3.14.4.

Preservar checks `quality`, `tests` e `container-smoke`, cobertura mínima configurada de 80% e CI independente. O job `quality` já executa MyPy. Confirmar requisitos do ruleset antes de mudanças futuras, sem renomear checks por conveniência. Não executar carga pesada em toda pull request.

AKS, recursos Azure reais, deployment cloud e Kubernetes ficam para `v1.1.0` ou para decisão futura do repositório integrador. A adoção de Azure Service Bus não está decidida.

## 11. Decisões pendentes antes de fixar contratos

1. Confirmar a compatibilidade do exchange `docpipe.events` (direct), routing key `document.received.v1` e headers com artefatos públicos do Ingestion. O checkout não estava disponível nesta implementação; defaults permanecem configuráveis e não são contrato definitivo do produtor.
2. Escolher motor(es) de extração e OCR, idiomas, dependências, licenças e limites de recursos para o ambiente do TCC.
3. Definir política para PDF com texto parcial, ausência de texto, páginas ilegíveis e formatos corrompidos.
4. Fixar formato e retenção do artefato derivado e regras de acesso ao texto.
5. Fechar contrato de `document.processed.v1` e eventual evento de falha considerando consumidores compatíveis, sem exigir sua implementação.
6. Avaliar operação em cluster, proteção dos recursos contra exclusão, monitoramento e procedimentos de recuperação da fila/DLQ. Os testes atuais são evidência de nó único, não garantia de alta disponibilidade.

As etapas futuras preservam os limites da seção 4. O PLAN distingue as entregas concluídas das capacidades futuras.
