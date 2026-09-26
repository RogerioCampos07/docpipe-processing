# Design do DocPipe Processing

## 1. Contexto e premissas

O `docpipe-processing` é um microserviço independente que recebe documentos já aceitos pelo `docpipe-ingestion`. Seu trabalho começa após o evento `document.received.v1`: localizar o original no armazenamento compartilhado, extrair seu texto e registrar o resultado e o estado do processamento. O aceite HTTP do Ingestion não depende da conclusão desta etapa.

Este documento orienta a implementação inicial da `v1.0.0`. Ele parte dos contratos documentados no Ingestion e deve ser confrontado com o código e os testes atuais daquele repositório antes de fixar a integração. O `PLAN.md` específico do Processing governa a ordem das etapas. Conforme a decisão do projeto, as capacidades mínimas de carga e resiliência previstas para uma etapa posterior devem entrar já na Etapa 2, sem antecipar conclusões experimentais.

## 2. Responsabilidades e limites

| Responsabilidade do Processing | Limite |
| --- | --- |
| Consumir `document.received.v1` do RabbitMQ | Não receber uploads de clientes nem modificar o evento do produtor |
| Recuperar e validar o original referenciado pelo evento | Não gravar no banco privado do Ingestion |
| Extrair texto de PDF, PNG e JPEG dentro de limites configuráveis | Não classificar documentos nem interpretar campos de negócio nesta versão |
| Persistir estado, tentativas, diagnóstico técnico e referência ao resultado | Não tratar o storage original como banco de estado |
| Disponibilizar consulta e sinais operacionais próprios | Não hospedar a stack central de observabilidade |
| Emitir um evento de conclusão após persistir o resultado | Não pressupor que consumidores posteriores já existam |

Cada serviço mantém seu próprio repositório, banco, migrations, imagem, testes e CI. O resultado de processamento não altera o registro de ingestão; uma visão agregada do documento pertencerá a uma composição futura entre serviços.

## 3. Contrato de entrada

O evento existente tem nome lógico `document.received.v1` e envelope com `event_id`, `event_type: document.received`, `event_version: 1`, `occurred_at`, `correlation_id`, `document_id` e `data.storage_key`, `data.media_type`, `data.size_bytes`, `data.sha256`. O corpo não contém arquivo, URL pública ou credenciais. O carrier W3C Trace Context é propagado pelos headers AMQP, sem mudança no corpo do evento.

Antes de implementar o consumidor, conferir no código do Ingestion o exchange, routing key, fila, bindings, política de confirmação e nomes exatos dos headers. Esses detalhes não são definidos pelo envelope e não devem ser inventados como contratos existentes. O Processing valida versão, identificadores, tipo, tamanho e checksum; rejeições permanentes devem ficar diagnosticáveis, sem repetição infinita.

A entrega do Ingestion é **pelo menos uma vez**. `event_id` é a chave para deduplicar a entrega; `document_id` identifica o documento. O recebimento repetido do mesmo evento não pode disparar duas conclusões lógicas nem duplicar resultados. Uma nova versão ou solicitação explícita de reprocessamento exigirá contrato próprio, sem reutilizar silenciosamente a mesma chave de idempotência.

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

Para integração ponta a ponta, o original precisa estar acessível aos dois serviços por meio de armazenamento compartilhado e privado, como Azurite no laboratório local. O caminho `dataset/documents/` do modo simples do Ingestion pertence àquele serviço e não deve ser assumido como volume implicitamente compartilhado. O Processing recebe a chave opaca do evento e configura o acesso por seu próprio adaptador e credenciais de mínimo privilégio. Resultados derivados usam namespace/container próprio e nunca sobrescrevem o original.

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
- Fazer o Processing tolerar duplicatas, interrupções e indisponibilidade temporária do Ingestion após a publicação do evento.

## 9. Observabilidade e experimentos desde a Etapa 2

A API e o worker emitem logs JSON com `service`, `correlation_id`, `document_id` quando apropriado, `event_id`, `trace_id`, operação, estado e duração. Métricas de baixo cardinalidade incluem mensagens recebidas, deduplicações, tentativas, falhas por categoria, trabalhos em espera, duração de download, extração/OCR e publicação, consumo de CPU e memória. Expor métricas para scrape e permitir exportação configurável de traces, preservando W3C Trace Context. A stack de coleta e visualização central pertence a um eventual repositório integrador.

Na **Etapa 2**, criar um ensaio pequeno e repetível com documentos sintéticos PDF, PNG e JPEG, teste de reentrega, reinício do worker e indisponibilidade temporária de RabbitMQ/storage. Registrar parâmetros, ambiente, taxa de conclusão, latências p50/p95/p99, backlog, tentativas, CPU, memória e limites observados. Começar por baseline de uma instância e só depois comparar concorrência sob orçamento fixo. O ensaio deve impor limites de recursos, permitir parada segura e preservar evidências; não deve presumir ganho de escala nem definir meta numérica antes do baseline. Esse trabalho inicial não substitui uma campanha experimental posterior mais completa.

## 10. Execução local e evolução

A `v1.0.0` usa Python, `uv`, FastAPI onde houver API, worker separado, Docker Compose, RabbitMQ e dependências locais. O template de repositório fornece convenções iniciais, mas não decide a arquitetura de processamento. A CI valida qualidade, contratos, testes e build sem executar carga pesada em toda pull request. O laboratório integrado entre repositórios exigirá configuração explícita de broker e armazenamento comuns.

AKS, recursos Azure reais, deployment cloud e Kubernetes ficam para `v1.1.0` ou para decisão futura do repositório integrador. A adoção de Azure Service Bus não está decidida.

## 11. Decisões pendentes antes de fixar contratos

1. Validar a topologia AMQP e o envelope efetivo no Ingestion; registrar exemplo real de mensagem e headers.
2. Escolher motor(es) de extração e OCR, idiomas, dependências, licenças e limites de recursos para o ambiente do TCC.
3. Definir política para PDF com texto parcial, ausência de texto, páginas ilegíveis e formatos corrompidos.
4. Fixar formato e retenção do artefato derivado e regras de acesso ao texto.
5. Fechar contrato de `document.processed.v1` e eventual evento de falha junto ao consumidor posterior.
6. Definir política de confirmação AMQP, fila de falhas, lease de recuperação e reprocessamento manual com testes de queda.

Essas decisões devem ser incorporadas ao design conforme forem verificadas, sem inventar integração já existente.
