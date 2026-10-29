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
- `docpipe_processing/domain.py` define `ProcessingJob` sem importar o contrato externo. `persistence.py` e `migrations/` mantêm estado próprio, SQLite por padrão e unicidade de `source_event_id`; os testes de persistência usam bancos temporários próprios.
- `pyproject.toml`, `Dockerfile` e `.github/workflows/ci.yml` usam dependências, checkout, build e testes do Processing. A aplicação atual expõe saúde e métricas; não executa o pipeline de documentos.

Na inspeção desses artefatos não foi encontrada dependência técnica do código, banco ou runtime do Ingestion. Isso não comprova autonomia do fluxo completo: consumidor, recuperação do objeto, extração e publicação ainda não estão implementados. Nas etapas correspondentes, será necessário demonstrar o fluxo com entradas sintéticas compatíveis e infraestrutura própria, sem outros microsserviços, incluindo substituição do produtor e ausência de consumidor posterior. Autorização de produtores também não é garantida pela validação estrutural do payload.

O repositório do Ingestion não estava disponível nesta revisão; sua compatibilidade e operação independente não foram verificadas. Esta decisão registra a obrigação arquitetural, sem afirmar adequações técnicas concluídas em outros serviços.

**DECIDIDO, ainda PLANEJADO tecnicamente:** migrar para PostgreSQL e evoluir a consulta HTTP antes da Etapa 6. O consumidor, a entrada durável e `ProcessingResult` ainda não estão implementados. `ProcessingJob` e o contrato de entrada já estão implementados e devem ser preservados.

### 2.3. Componentes e direção das dependências

- **Domínio:** `ProcessingJob` e suas invariantes, sem objetos FastAPI, SQLAlchemy ou RabbitMQ e sem dependência do contrato externo. `ProcessingResult` será definido na Etapa 8.
- **Aplicação:** coordena os casos de uso e a persistência necessária à aceitação; o application handler será independente de objetos de transporte.
- **Transporte HTTP:** valida parâmetros e projeta respostas de recursos persistidos por meio do repository próprio, sem chamar o consumer.
- **Transporte AMQP:** usa `aio-pika` em worker separado da API, valida a entrada e traduz o resultado da aceitação em ACK ou rejeição.
- **Infraestrutura:** SQLAlchemy/repository, PostgreSQL e adaptador próprio de armazenamento implementam o acesso aos recursos externos; não acessam dados privados de outros serviços.

Esses limites orientam as futuras implementações sem exigir reorganização antecipada de pacotes, frameworks adicionais ou abstrações genéricas sem uso.

## 3. Contrato de entrada

O contrato de entrada implementado tem nome lógico `document.received.v1` e envelope com `event_id`, `event_type: document.received`, `event_version: 1`, `occurred_at`, `correlation_id`, `document_id` e `data.storage_key`, `data.media_type`, `data.size_bytes`, `data.sha256`. As regras vigentes estão na [especificação pública](contracts/document_received_v1.md): IDs externos são strings opacas não vazias nem compostas apenas de whitespace; `correlation_id` pode estar ausente, ser `null` ou uma string válida. O `processing_id` interno continua sendo UUID, distinto dos IDs externos. O corpo não contém arquivo, URL pública ou credenciais. W3C Trace Context pertence aos headers de transporte; sua integração AMQP ainda será implementada.

Antes de implementar o consumidor, verificar a especificação pública de transporte e a compatibilidade do produtor quanto a exchange, routing key, fila, bindings e nomes exatos dos headers. O código do Ingestion, se disponível, é evidência complementar, sem se tornar requisito de instalação ou execução. Esses detalhes não são definidos pelo envelope e não devem ser inventados como contratos existentes. As políticas básicas de ACK durável, rejeição sem requeue e DLQ simples estão decididas na seção 4; seus parâmetros operacionais ainda precisam ser definidos. O Processing já valida a estrutura do evento; a conferência de tamanho e checksum do arquivo pertence à recuperação futura. Rejeições permanentes devem ficar diagnosticáveis, sem repetição infinita.

A garantia arquitetural de entrega permanece **pelo menos uma vez**, inclusive na integração prevista com o Ingestion. `event_id` é a chave para deduplicar a entrega; `document_id` identifica o documento. O recebimento repetido do mesmo evento não pode disparar duas conclusões lógicas nem duplicar resultados. Uma nova versão ou solicitação explícita de reprocessamento exigirá contrato próprio, sem reutilizar silenciosamente a mesma chave de idempotência.

## 4. Fluxo de processamento

### 4.1. Aceitação durável — Etapa 6

**DECIDIDO, implementação PLANEJADA:** worker `aio-pika` separado da API, com application handler independente dos objetos RabbitMQ, validação pelo contrato da Etapa 5 e W3C Trace Context nos headers.

```text
evento válido → validação → job RECEIVED + entrada durável → commit → ACK
```

Criar `ProcessingJob.RECEIVED`, com `attempts=0`, UUID próprio em `processing_id` e `event_id` em `source_event_id`. Job e informações necessárias à continuidade serão gravados atomicamente no PostgreSQL próprio. **ACK significa aceitação durável da entrada, não conclusão do processamento documental.** Após reinicialização, não depender da mensagem original nem do runtime do produtor.

ACK somente após commit da entrada nova ou confirmação de duplicata equivalente já persistida e recuperável. Redelivery não cria outro job nem incrementa tentativas. O mesmo identificador com dados incompatíveis não é automaticamente equivalente. Preservar unicidade de `source_event_id`, idempotência e entrega pelo menos uma vez.

Mensagens permanentemente inválidas serão rejeitadas sem requeue, com DLQ simples. Falhas transitórias não receberão ACK de sucesso. Parâmetros de retry limitado, equivalência, conflitos e prevenção de ciclos infinitos ainda precisam ser detalhados.

Queda antes do commit não permite ACK; depois do commit e antes do ACK pode produzir redelivery. Testes com produtor sintético deverão cobrir essas janelas, rollback atômico, concorrência, duplicatas, rejeição, falhas transitórias e continuidade após reinicialização.

O consumer depende da conclusão da persistência durável necessária, mediante planejamento e autorização posteriores do modelo físico. A Etapa 6 não recupera documentos, executa OCR ou extração, cria `ProcessingResult`, publica saída, implementa outbox ou reprocessamento manual.

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

### 5.3. Entrada durável — requisito DECIDIDO, modelo físico PENDENTE

O job persiste as correspondências de `event_id` em `source_event_id`, `document_id` e `correlation_id`, mas não `storage_key`, `media_type`, `size_bytes` e `sha256`, necessários à recuperação e verificação posterior. Memória do worker e mensagem confirmada não são fontes duráveis para retomada.

Está aprovada uma representação mínima da entrada vinculada ao job, exclusivamente no PostgreSQL do Processing, gravada atomicamente com sua criação. Deve conservar informações necessárias à continuidade após reinicialização e preservar idempotência. Não exige armazenar o documento nem payload JSON integral. Avaliar outros dados somente quando necessários; `occurred_at` do evento não equivale a `created_at` do job.

Alternativas ainda não selecionadas:

| Alternativa | Trade-off a avaliar |
| --- | --- |
| Estender `processing_jobs` | Leitura direta, com maior mistura entre ciclo de vida e entrada |
| Tabela própria vinculada ao job | Separação da entrada, com vínculo e transação entre tabelas |

**PENDENTE de planejamento e autorização:** modelo físico, campos finais, constraints, índices, estratégia de migrations, tratamento de dados existentes e recuperação de trabalhos pendentes. O repository atual confirma a criação internamente; a futura coordenação deverá garantir um único commit para job e entrada, evitando commits separados. Aprovar o requisito não autoriza schema, migrations ou implementação técnica.

## 6. Persistência e armazenamento

**IMPLEMENTADO:** SQLAlchemy ORM síncrono, repository, sessões, Alembic e SQLite padrão. **DECIDIDO, migração PLANEJADA antes da Etapa 6:** PostgreSQL homologado (seção 10.1), Psycopg 3 e configuração externa `PROCESSING_DATABASE_URL`, sem fallback silencioso para SQLite após a migração.

Preservar ORM, histórico Alembic, integridade referencial, constraints, UUID, timestamps UTC e unicidade de `source_event_id`. Cada serviço mantém banco lógico, credenciais, schema sob sua responsabilidade e acesso exclusivo aos próprios dados. API e worker gerenciarão sessões e transações próprias, com rollback correto; distinguir conflito da constraint de evento de outros `IntegrityError`, sem tratar todo erro como duplicata.

Validar `0001_processing_jobs` no PostgreSQL; não apagar nem reescrever indiscriminadamente o histórico. Avaliar novas revisões justificadas para evoluções, incluindo entrada durável. Inventariar SQLite local antes de decidir entre transferência e banco novo: dados relevantes exigem backup, transferência validada e possibilidade de retorno. Alembic migra schema, não transfere automaticamente dados entre bancos. Testes futuros usarão PostgreSQL real isolado para migrations, round-trip, UTC, unicidade, rollback e concorrência.

Para integração ponta a ponta, o produtor deve disponibilizar o original ao Processing por interface pública de armazenamento privado, como Azurite no laboratório local, sem exigir seu próprio runtime para a recuperação. A referência anterior ao caminho `dataset/documents/` do Ingestion não constitui contrato nem autoriza montá-lo como volume compartilhado; detalhes internos daquele repositório não foram verificados nesta revisão. O Processing recebe a chave opaca do evento e configura o acesso por seu próprio adaptador e credenciais de mínimo privilégio. Resultados derivados usam namespace/container próprio e nunca sobrescrevem o original.

Se a escrita do artefato derivado for confirmada, mas a transação do banco falhar, pode restar um órfão. A reconciliação deve identificar esse caso antes de qualquer exclusão. O conteúdo extraído não deve aparecer em eventos, logs, métricas ou traces.

## 7. Contratos de saída e API HTTP

### 7.1. Evento de saída — PLANEJADO para a Etapa 9

Nome provisório: `document.processed.v1`. Proposta não definitiva: envelope com identificação, tipo, versão, ocorrência, correlação e documento; dados com `processing_id`, status e referência lógica ao resultado, possivelmente páginas e modo de extração. Contrato e eventual evento de falha precisam ser definidos considerando consumidores compatíveis, sem exigir que estejam implementados ou em execução. Não transportar texto, URL pública ou credenciais; o evento não fixa o schema de `ProcessingResult`.

### 7.2. API HTTP própria — DECIDIDA, implementação PLANEJADA

Além de `/health` e `/metrics` implementados, a primeira entrega será `GET /v1/processing-jobs/{processing_id}`, após alinhamento de stack/PostgreSQL e antes da Etapa 6, conforme o PLAN.

A fronteira HTTP consultará `ProcessingJob` persistido pelo repository próprio, com contrato versionado, schema explícito de resposta, tratamento de identificador inválido e recurso inexistente, testes automatizados e documentação OpenAPI. A projeção não expõe modelos ORM; diagnósticos devem ser seguros, sem conteúdo documental ou segredos.

Consultas aos dados persistidos não exigirão consumer, RabbitMQ ou Ingestion em execução. Não estão aprovados endpoints adicionais, recuperação do original ou exposição do texto completo. `/health` não implica readiness de componentes ainda não implementados.

## 8. Falhas, segurança e privacidade

- Diferenciar erro temporário de broker, banco ou storage, documento ausente/inconsistente, formato inválido e erro do motor de extração.
- Aplicar timeout, backoff com limite, estado terminal e reprocessamento controlado; impedir fila de reentregas sem fim.
- Proteger o worker contra documentos malformados, PDFs grandes ou comprimidos de forma adversarial e custo excessivo de OCR; processar dados sintéticos nos ensaios.
- Nunca incluir conteúdo do documento ou texto extraído em logs, traces, métricas, mensagens ou respostas operacionais.
- Configurar segredos fora da imagem; restringir acesso a originais e resultados e documentar retenção e exclusão antes de dados pessoais reais.
- Fazer o Processing tolerar duplicatas e interrupções sem exigir que o produtor esteja em execução após disponibilizar o evento e o objeto pela infraestrutura contratada.

## 9. Observabilidade e experimentos desde a Etapa 2

**IMPLEMENTADO:** observabilidade HTTP com logs estruturados, métricas e tracing. Sinais do worker serão acrescentados conforme os componentes existirem, preservando correlação e W3C Trace Context. Métricas de baixa cardinalidade poderão representar recebimentos, duplicatas, tentativas, falhas, espera e duração de recuperação, extração e publicação. Identificadores não serão labels; conteúdo documental não será registrado. A stack centralizada não pertence ao serviço.

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

**PLANEJADO:** alinhar `requires-python`, `.python-version`, Dockerfile, GitHub Actions e MyPy; revisar compatibilidade, patches e fixações necessárias à reprodutibilidade. Atualmente o projeto declara Python `>=3.13`, usa SQLite padrão e possui Compose vazio; MyPy já está configurado para 3.14. A decisão não representa atualização técnica concluída.

Preservar checks `quality`, `tests` e `container-smoke`, cobertura mínima configurada de 80% e CI independente. O comando MyPy existe; sua inclusão na CI é planejada. Confirmar requisitos do ruleset antes de mudanças futuras, sem renomear checks por conveniência. Não executar carga pesada em toda pull request.

AKS, recursos Azure reais, deployment cloud e Kubernetes ficam para `v1.1.0` ou para decisão futura do repositório integrador. A adoção de Azure Service Bus não está decidida.

## 11. Decisões pendentes antes de fixar contratos

1. Definir a integração AMQP pública e verificar compatibilidade dos produtores, incluindo o Ingestion quando seus artefatos estiverem disponíveis; registrar evidências de mensagem e headers sem alterar o contrato de entrada já implementado nem exigir checkout de outro serviço.
2. Escolher motor(es) de extração e OCR, idiomas, dependências, licenças e limites de recursos para o ambiente do TCC.
3. Definir política para PDF com texto parcial, ausência de texto, páginas ilegíveis e formatos corrompidos.
4. Fixar formato e retenção do artefato derivado e regras de acesso ao texto.
5. Fechar contrato de `document.processed.v1` e eventual evento de falha considerando consumidores compatíveis, sem exigir sua implementação.
6. Detalhar parâmetros operacionais de tratamento transitório e DLQ, equivalência e conflitos, recuperação/lease e futuro reprocessamento manual. ACK durável, rejeição permanente sem requeue, DLQ simples e Trace Context já estão decididos.
7. Planejar e aprovar o modelo físico da entrada durável: extensão da tabela ou tabela vinculada, constraints, índices, migrations, dados existentes e recuperação. Persistência própria e atômica é requisito aprovado, mas essas escolhas não estão.

Essas escolhas exigem avaliação e aprovação posteriores, sem reabrir requisitos decididos nem declarar integrações implementadas. A Etapa 6 depende da persistência durável; as Etapas 7, 8 e 9 preservam os limites da seção 4. O PLAN governa a ordem e permanece preservado nesta revisão.
