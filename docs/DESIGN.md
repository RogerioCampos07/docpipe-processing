# Design do DocPipe Processing

## 1. Contexto e premissas

O `docpipe-processing` é um microserviço independente cuja responsabilidade é processar documentos referenciados por `document.received.v1`: localizar o original no armazenamento, extrair seu texto e registrar o resultado e o estado do processamento. A entrada pode vir de qualquer produtor autorizado e compatível com o contrato público, incluindo o `docpipe-ingestion`. O aceite HTTP do Ingestion não depende da conclusão desta etapa.

Este documento orienta a implementação inicial da `v1.0.0`. A integração tem como fonte o contrato público versionado; código e testes do Ingestion, quando disponíveis, servem apenas como evidência de compatibilidade daquele produtor. Sua inspeção não cria dependência entre repositórios. O `PLAN.md` específico do Processing governa a ordem das etapas. Conforme a decisão do projeto, as capacidades mínimas de carga e resiliência previstas para uma etapa posterior devem entrar já na Etapa 2, sem antecipar conclusões experimentais.

## 2. Responsabilidades e limites

| Responsabilidade do Processing | Limite |
| --- | --- |
| Consumir `document.received.v1` do RabbitMQ | Não receber uploads de clientes nem modificar o evento do produtor |
| Recuperar e validar o original referenciado pelo evento | Não gravar no banco privado do Ingestion |
| Extrair texto de PDF, PNG e JPEG dentro de limites configuráveis | Não classificar documentos nem interpretar campos de negócio nesta versão |
| Persistir estado, tentativas, diagnóstico técnico e referência ao resultado | Não tratar o storage original como banco de estado |
| Disponibilizar consulta e sinais operacionais próprios | Não hospedar a stack central de observabilidade |
| Emitir um evento de conclusão após persistir o resultado | Não pressupor que consumidores posteriores já existam |

O resultado de processamento não altera o registro de ingestão; uma visão agregada do documento pertencerá a uma composição futura entre serviços.

### 2.1. Decisão obrigatória: autonomia dos microsserviços

Todos os microsserviços do DocPipe devem ser desacoplados, independentes e possuir utilidade própria. Cada um deve executar sua responsabilidade de negócio delimitada sem exigir outros microsserviços em execução.

- Cada serviço possui repositório, domínio, banco de dados, migrations, configuração, imagem, testes e CI próprios.
- Instalação, build, inicialização, testes e implantação não podem exigir checkout nem execução de outro microsserviço. A evolução e a implantação são independentes, respeitando a compatibilidade dos contratos públicos.
- A integração ocorre exclusivamente por contratos públicos e versionados. Cada serviço mantém sua própria representação; produtores e consumidores autorizados e compatíveis podem ser substituídos sem mudanças no domínio do serviço.
- É proibido importar código interno, classes de domínio ou modelos ORM de outro serviço, consultar suas tabelas ou depender de seu filesystem privado. Por exemplo: importar `DocumentReceivedEvent` do pacote do Ingestion, consultar o banco de ingestão para completar um evento ou montar seu diretório privado de documentos no Processing.
- Referências externas a documentos e objetos devem estar explícitas no contrato. O acesso ocorre por interfaces públicas de armazenamento e adaptadores próprios, usando referências opacas, sem descobrir caminhos ou consultar estruturas internas do produtor.
- Banco de dados, RabbitMQ e armazenamento de objetos continuam sendo dependências legítimas. Operação isolada pressupõe a infraestrutura necessária ao próprio serviço; compartilhar uma instância física não autoriza compartilhar tabelas, modelos ou estado interno.
- A colaboração assíncrona preserva entrega pelo menos uma vez, idempotência e rastreabilidade conforme os contratos e políticas aplicáveis. Autonomia não promete processamento exatamente uma vez nem altera identificadores ou regras de correlação.
- Autonomia não exige criar API HTTP, CLI ou novo modo de execução. As interfaces devem atender à responsabilidade existente de cada serviço.

**Aplicação ao DocPipe:** receber, registrar e armazenar documentos dá utilidade própria ao Ingestion, sem exigir Processing nem conclusão das etapas posteriores. O Processing deve transformar documentos referenciados por entradas compatíveis em resultado e estado próprios, sem código, banco ou runtime do Ingestion. Não se exige consumidor posterior em execução para concluir sua responsabilidade; a publicação segue as garantias de entrega definidas. Não há responsabilidades de outros serviços suficientemente definidas neste repositório para atribuir novas funcionalidades.

### 2.2. Estado observado e evidências ainda necessárias

O estado implementado deve ser distinguido do fluxo previsto nas seções seguintes:

- `docpipe_processing/contracts.py` valida o contrato com modelos próprios; `tests/test_contracts.py` usa payloads e fixture JSON locais, sem produtor ou broker.
- `docpipe_processing/domain.py` define `ProcessingJob` sem importar o contrato externo. `persistence.py` e `migrations/` mantêm estado próprio, SQLite por padrão e unicidade de `source_event_id`; os testes de persistência usam bancos temporários próprios.
- `pyproject.toml`, `Dockerfile` e `.github/workflows/ci.yml` usam dependências, checkout, build e testes do Processing. A aplicação atual expõe saúde e métricas; não executa o pipeline de documentos.

Na inspeção desses artefatos não foi encontrada dependência técnica do código, banco ou runtime do Ingestion. Isso não comprova autonomia do fluxo completo: consumidor, recuperação do objeto, extração e publicação ainda não estão implementados. Nas etapas correspondentes, será necessário demonstrar o fluxo com entradas sintéticas compatíveis e infraestrutura própria, sem outros microsserviços, incluindo substituição do produtor e ausência de consumidor posterior. Autorização de produtores também não é garantida pela validação estrutural do payload.

O repositório do Ingestion não estava disponível nesta revisão; sua compatibilidade e operação independente não foram verificadas. Esta decisão registra a obrigação arquitetural, sem afirmar adequações técnicas concluídas em outros serviços.

## 3. Contrato de entrada

O contrato de entrada implementado tem nome lógico `document.received.v1` e envelope com `event_id`, `event_type: document.received`, `event_version: 1`, `occurred_at`, `correlation_id`, `document_id` e `data.storage_key`, `data.media_type`, `data.size_bytes`, `data.sha256`. As regras vigentes estão na [especificação pública](contracts/document_received_v1.md): IDs externos são strings opacas não vazias nem compostas apenas de whitespace; `correlation_id` pode estar ausente, ser `null` ou uma string válida. O `processing_id` interno continua sendo UUID, distinto dos IDs externos. O corpo não contém arquivo, URL pública ou credenciais. W3C Trace Context pertence aos headers de transporte; sua integração AMQP ainda será implementada.

Antes de implementar o consumidor, verificar a especificação pública de transporte e a compatibilidade do produtor quanto a exchange, routing key, fila, bindings, política de confirmação e nomes exatos dos headers. O código do Ingestion, se disponível, é evidência complementar, sem se tornar requisito de instalação ou execução. Esses detalhes não são definidos pelo envelope e não devem ser inventados como contratos existentes. O Processing já valida a estrutura do evento; a conferência de tamanho e checksum do arquivo pertence à recuperação futura. Rejeições permanentes devem ficar diagnosticáveis, sem repetição infinita.

A garantia arquitetural de entrega permanece **pelo menos uma vez**, inclusive na integração prevista com o Ingestion. `event_id` é a chave para deduplicar a entrega; `document_id` identifica o documento. O recebimento repetido do mesmo evento não pode disparar duas conclusões lógicas nem duplicar resultados. Uma nova versão ou solicitação explícita de reprocessamento exigirá contrato próprio, sem reutilizar silenciosamente a mesma chave de idempotência.

## 4. Fluxo de processamento

1. O worker recebe o evento, recupera o contexto de correlação e de trace e valida o envelope.
2. Em transação no banco próprio, cria ou localiza o trabalho pelo `event_id` e registra seu estado; duplicatas já concluídas são reconhecidas.
3. Lê o original por adaptador de armazenamento, confere tamanho e SHA-256 e impõe limites de bytes, páginas, pixels, tempo e consumo de recursos.
4. Extrai texto do PDF quando houver camada textual; aplica OCR quando necessário e para imagens suportadas. A política para páginas mistas e documentos sem texto deve ser explicitada e testada antes de fechar o contrato de resultado.
5. Persiste o texto ou um artefato derivado em área privada própria, conforme a estratégia de armazenamento escolhida, e grava metadados e estado no banco do Processing.
6. Na mesma transação do estado concluído, registra o evento de conclusão em outbox própria. Um publicador separado envia o evento e registra a confirmação do broker.
7. A mensagem de entrada é confirmada somente quando o trabalho estiver durável para retomada. Falhas temporárias seguem retry limitado com backoff; falhas permanentes vão a um estado terminal inspecionável.

O passo 7 não significa manter uma entrega AMQP aberta durante OCR longo: a implementação deve escolher e testar uma estratégia explícita de confirmação e retomada de trabalhos persistidos após quedas. Queda entre a confirmação do broker e o registro local, e queda entre publicação e confirmação da outbox, podem causar reentrega; a idempotência precisa cobrir ambas.

## 5. Dados e estados próprios

Modelo inicial proposto, sujeito à revisão no primeiro slice implementável:

| Entidade | Dados mínimos |
| --- | --- |
| `processing_jobs` | `id`, `source_event_id` único, `document_id`, `correlation_id`, `status`, `attempts`, `next_attempt_at`, timestamps UTC, motivo técnico resumido |
| `processing_results` | `job_id`, motor e versão, modo de extração, referência privada ao texto, tamanho do resultado, checksum, páginas processadas, timestamps |
| `outbox_events` | `id`, `aggregate_id`, tipo e versão, payload, tentativas, próxima tentativa, publicação confirmada |

Estados conceituais: `RECEIVED`, `PROCESSING`, `COMPLETED`, `RETRY_WAIT` e `FAILED`. Uma queda pode deixar um trabalho em `PROCESSING`; uma rotina com lease/timeout deve recuperá-lo de forma segura. Trabalho sem texto extraível precisa ter resultado explícito, sem fingir sucesso com conteúdo inexistente. Os estados externos e nomes de colunas finais serão definidos com testes de contrato.

## 6. Persistência e armazenamento

O banco do Processing é privado e possui migrations próprias. SQLite pode atender à execução simples em instância única; PostgreSQL é a opção para experimentos com workers concorrentes. Não compartilhar tabelas ou schema lógico com o Ingestion, mesmo quando a infraestrutura PostgreSQL física for comum no laboratório.

Para integração ponta a ponta, o produtor deve disponibilizar o original ao Processing por interface pública de armazenamento privado, como Azurite no laboratório local, sem exigir seu próprio runtime para a recuperação. A referência anterior ao caminho `dataset/documents/` do Ingestion não constitui contrato nem autoriza montá-lo como volume compartilhado; detalhes internos daquele repositório não foram verificados nesta revisão. O Processing recebe a chave opaca do evento e configura o acesso por seu próprio adaptador e credenciais de mínimo privilégio. Resultados derivados usam namespace/container próprio e nunca sobrescrevem o original.

Se a escrita do artefato derivado for confirmada, mas a transação do banco falhar, pode restar um órfão. A reconciliação deve identificar esse caso antes de qualquer exclusão. O conteúdo extraído não deve aparecer em eventos, logs, métricas ou traces.

## 7. Evento de saída e API

O nome provisório do evento é `document.processed.v1`. Proposta de campos: `event_id`, `event_type`, `event_version`, `occurred_at`, `correlation_id`, `document_id` e, em `data`, `processing_id`, `status`, referência lógica ao resultado, contagem de páginas e modo de extração. O contrato final depende do consumidor seguinte; uma falha terminal pode exigir evento separado. Nenhum deles deve carregar texto, URL pública ou credenciais.

Uma API administrativa mínima pode expor liveness, readiness, métricas e consulta do estado e metadados do processamento por `document_id`. A consulta não deve servir o original ou o texto completo sem um contrato de autorização e acesso definido. Rotas e payloads finais serão versionados quando a necessidade do consumidor estiver clara.

## 8. Falhas, segurança e privacidade

- Diferenciar erro temporário de broker, banco ou storage, documento ausente/inconsistente, formato inválido e erro do motor de extração.
- Aplicar timeout, backoff com limite, estado terminal e reprocessamento controlado; impedir fila de reentregas sem fim.
- Proteger o worker contra documentos malformados, PDFs grandes ou comprimidos de forma adversarial e custo excessivo de OCR; processar dados sintéticos nos ensaios.
- Nunca incluir conteúdo do documento ou texto extraído em logs, traces, métricas, mensagens ou respostas operacionais.
- Configurar segredos fora da imagem; restringir acesso a originais e resultados e documentar retenção e exclusão antes de dados pessoais reais.
- Fazer o Processing tolerar duplicatas e interrupções sem exigir que o produtor esteja em execução após disponibilizar o evento e o objeto pela infraestrutura contratada.

## 9. Observabilidade e experimentos desde a Etapa 2

A API e o worker emitem logs JSON com `service`, `correlation_id`, `document_id` quando apropriado, `event_id`, `trace_id`, operação, estado e duração. Métricas de baixo cardinalidade incluem mensagens recebidas, deduplicações, tentativas, falhas por categoria, trabalhos em espera, duração de download, extração/OCR e publicação, consumo de CPU e memória. Expor métricas para scrape e permitir exportação configurável de traces, preservando W3C Trace Context. A stack de coleta e visualização central pertence a um eventual repositório integrador.

Na **Etapa 2**, criar um ensaio pequeno e repetível com documentos sintéticos PDF, PNG e JPEG, teste de reentrega, reinício do worker e indisponibilidade temporária de RabbitMQ/storage. Registrar parâmetros, ambiente, taxa de conclusão, latências p50/p95/p99, backlog, tentativas, CPU, memória e limites observados. Começar por baseline de uma instância e só depois comparar concorrência sob orçamento fixo. O ensaio deve impor limites de recursos, permitir parada segura e preservar evidências; não deve presumir ganho de escala nem definir meta numérica antes do baseline. Esse trabalho inicial não substitui uma campanha experimental posterior mais completa.

## 10. Execução local e evolução

A `v1.0.0` usa Python, `uv`, FastAPI onde houver API, worker separado, Docker Compose, RabbitMQ e dependências locais. O template de repositório fornece convenções iniciais, mas não decide a arquitetura de processamento. A CI valida qualidade, contratos, testes e build sem executar carga pesada em toda pull request. O laboratório integrado entre repositórios exigirá configuração explícita de broker e armazenamento comuns.

AKS, recursos Azure reais, deployment cloud e Kubernetes ficam para `v1.1.0` ou para decisão futura do repositório integrador. A adoção de Azure Service Bus não está decidida.

## 11. Decisões pendentes antes de fixar contratos

1. Definir a integração AMQP pública e verificar compatibilidade dos produtores, incluindo o Ingestion quando seus artefatos estiverem disponíveis; registrar evidências de mensagem e headers sem alterar o contrato de entrada já implementado nem exigir checkout de outro serviço.
2. Escolher motor(es) de extração e OCR, idiomas, dependências, licenças e limites de recursos para o ambiente do TCC.
3. Definir política para PDF com texto parcial, ausência de texto, páginas ilegíveis e formatos corrompidos.
4. Fixar formato e retenção do artefato derivado e regras de acesso ao texto.
5. Fechar contrato de `document.processed.v1` e eventual evento de falha junto ao consumidor posterior.
6. Definir política de confirmação AMQP, fila de falhas, lease de recuperação e reprocessamento manual com testes de queda.

Essas decisões devem ser incorporadas ao design conforme forem verificadas, sem inventar integração já existente.
