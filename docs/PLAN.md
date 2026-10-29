# DocPipe Processing — Plano de Desenvolvimento

## 1. Objetivo

O **Processing** é o microserviço responsável pelo processamento dos documentos recebidos pelo DocPipe após a etapa de ingestão.

Ele deverá operar sem exigir outros microsserviços em execução, recebendo entradas de qualquer produtor autorizado e compatível por contratos públicos versionados e processamento assíncrono.

O serviço deve ser desenvolvido, instalado, construído, iniciado, testado e implantado sem checkout ou runtime de outros microsserviços. A diretriz de autonomia e as evidências anteriores estão nas seções 2.1 e 2.2 do [DESIGN](DESIGN.md). As decisões recentes aprovadas estão incorporadas neste plano; as divergências com os demais documentos deverão ser avaliadas em revisão posterior. Esta atualização é exclusivamente documental, preserva as entregas anteriores e não declara adequações técnicas concluídas.

---

## 2. Responsabilidade do microserviço

O Processing deverá:

- receber eventos informando que um documento está disponível para processamento;
- identificar o documento e suas informações necessárias;
- recuperar o documento do armazenamento;
- executar o pipeline de processamento definido para o DocPipe;
- registrar o estado e o resultado do processamento;
- publicar um novo evento indicando conclusão e, quando definido pelo contrato de saída, falha;
- permitir rastrear cada processamento por identificadores de correlação;
- disponibilizar consulta HTTP versionada dos recursos de domínio próprios.

### Produtos de domínio

- **IMPLEMENTADO — `ProcessingJob`:** representa identidade do processamento, estado, tentativas, timestamps, falhas e rastreabilidade. Seu modelo e suas invariantes deverão ser preservados.
- **PLANEJADO — `ProcessingResult`:** resultado técnico reutilizável do processamento documental, a ser definido e implementado na Etapa 8 conforme as necessidades reais do pipeline. Seu formato, armazenamento e política de acesso ainda não constituem contrato definitivo.

O Processing não deverá assumir responsabilidades do Ingestion, de classificação, validação e encaminhamento do Triage, nem de consolidação do Registry. O Registry será responsável pelo `DocumentRecord` consolidado. Não compartilhar modelos internos entre esses serviços.

---

## 3. Relação com o Ingestion

Um fluxo de colaboração esperado é:

**Documento → Ingestion → armazenamento → evento → Processing**

O Ingestion é responsável por receber, registrar e armazenar documentos, com utilidade própria sem Processing ou conclusão de etapas posteriores. Essa é uma obrigação arquitetural; a implementação do Ingestion não foi verificada neste repositório.

Depois que o documento estiver disponível, o Ingestion publica um evento no broker.

O Processing consome esse evento e inicia seu trabalho.

O Processing deve executar sua responsabilidade sem código, banco, filesystem privado ou runtime do Ingestion. Um produtor autorizado e compatível pode substituí-lo sem mudar o domínio do Processing.

A integração deve ocorrer através de:

- contratos públicos versionados de mensagens e eventos;
- identificadores e referências opacas definidos nesses contratos;
- interfaces públicas de armazenamento com adaptadores próprios, quando necessário.

**DECIDIDO:** o DocPipe v1.0.0 será composto por `docpipe-ingestion`, `docpipe-processing`, `docpipe-triage` e `docpipe-registry`. Cada serviço deverá possuir API HTTP, produto de domínio, banco de dados, migrations, testes e CI próprios, com autonomia operacional e integração por contratos públicos versionados.

Banco, RabbitMQ e armazenamento são infraestrutura permitida para operação isolada. Compartilhar instâncias não autoriza compartilhar bancos lógicos, tabelas privadas, modelos internos ou código de domínio. O Processing deverá continuar funcional e testável sem executar Ingestion, Triage ou Registry.

RabbitMQ não substitui a API HTTP. A consulta de processamentos é uma capacidade de domínio aprovada, com utilidade para acompanhamento e rastreabilidade. Não criar interfaces adicionais sem necessidade funcional.

---

## 4. Arquitetura inicial

O Processing seguirá a stack e os princípios comuns aprovados para o DocPipe, mantendo configuração, dependências e implementação próprias:

- Python 3.14 como linha minor comum;
- FastAPI;
- gerenciamento de dependências com `uv`;
- configuração por variáveis de ambiente;
- PostgreSQL com banco lógico, credenciais, migrations e dados próprios;
- RabbitMQ para comunicação assíncrona;
- armazenamento compatível com o ambiente local do DocPipe;
- Docker;
- Docker Compose;
- testes automatizados;
- lint e validações de qualidade;
- GitHub Actions.

Para a versão local do DocPipe, serviços externos de cloud deverão ser substituídos por alternativas locais quando aplicável.

Integrações específicas com provedores de cloud ficam reservadas para uma evolução posterior.

### Política híbrida de stack e versões

**DECIDIDO:** todos os serviços que utilizarem cada componente de infraestrutura deverão adotar a respectiva imagem homologada:

| Tecnologia | Referência aprovada | Responsabilidade do Processing |
| --- | --- | --- |
| PostgreSQL | `postgres:17.11-bookworm` | Banco lógico, credenciais, schema, migrations e acesso exclusivo aos próprios dados. |
| RabbitMQ | `rabbitmq:4.3.6-management` | Filas, configuração e responsabilidades próprias; broker compartilhado em ambiente integrado não pode exigir outro microsserviço em execução. |
| Python | Linha minor 3.14 | Alinhar `requires-python`, `.python-version`, Dockerfile, GitHub Actions, MyPy e documentação. |

**PLANEJADO:** selecionar versões patch do Python considerando compatibilidade e reprodutibilidade e alinhá-las nos ambientes utilizados. Avaliar fixação por digest das imagens e uniformização da versão de `uv` entre Docker e CI, sem presumir que uma tag garante um build imutável.

Para bibliotecas compartilhadas, adotar a versão de referência do Ingestion ou uma versão superior compatível: FastAPI, Uvicorn, Pydantic, Pydantic Settings, SQLAlchemy, Alembic, OpenTelemetry, Prometheus Client, pytest, pytest-cov, Ruff, MyPy, Typos e Taskipy.

Não exigir versões idênticas de todas as bibliotecas Python nem forçar versões superiores com risco de incompatibilidade. Cada microsserviço manterá seu próprio `uv.lock`, com dependências resolvidas e testadas. Bibliotecas específicas do Ingestion e dependências ainda sem uso funcional no Processing não deverão ser instaladas apenas para padronização; isso também vale para Pydantic Settings caso sua adoção não seja necessária.

**PENDENTE:** confirmar as versões de referência das bibliotecas e sua compatibilidade na futura revisão técnica. Os artefatos do Ingestion não estão disponíveis no ambiente desta revisão; registrar a evidência externa necessária, sem tornar seu checkout requisito de instalação, build, testes ou execução. Nenhuma dependência é atualizada por esta revisão documental.

---

## 5. Princípios de desenvolvimento

O serviço deverá seguir:

- responsabilidade única;
- baixo acoplamento;
- contratos explícitos;
- processamento idempotente;
- tratamento previsível de falhas;
- possibilidade de retry;
- rastreabilidade;
- configuração externa;
- testes automatizados;
- documentação das decisões relevantes;
- integração contínua desde o início do desenvolvimento.

---

# Etapas de desenvolvimento

### Estado atual e leitura do plano

- **IMPLEMENTADO:** comprovado no código atual, sem significar nova execução das validações nesta revisão documental.
- **PLANEJADO:** previsto para implementação futura, ainda não entregue tecnicamente.
- **DECIDIDO:** requisito arquitetural aprovado, que pode continuar aguardando implementação.
- **PENDENTE:** decisão ou avaliação ainda necessária antes de fixar a solução correspondente.

As entregas das Etapas 1 a 5 estão preservadas e não serão reabertas. Suas seções mantêm o histórico dos objetivos; as adequações serão realizadas nos blocos futuros apropriados.

| Entrega | Estado observado |
| --- | --- |
| Etapas 1 e 2 | **IMPLEMENTADO:** bootstrap, API operacional com `/health` e `/metrics`, observabilidade HTTP, testes, cobertura mínima configurada em 80% e CI com `quality`, `tests` e `container-smoke`. MyPy possui configuração e task, mas ainda não é executado pelo workflow. |
| Etapa 3 | **IMPLEMENTADO:** `ProcessingJob` imutável, UUID próprio, estados e invariantes com testes unitários. |
| Etapa 4 | **IMPLEMENTADO:** SQLAlchemy ORM, repository, sessões, migration Alembic `0001_processing_jobs`, SQLite padrão e testes de persistência. A unicidade no banco não comprova ainda idempotência de um consumidor. |
| Etapa 5 | **IMPLEMENTADO:** contrato `document.received.v1`, representação própria, especificação, fixture e testes sintéticos sem broker. |
| Blocos intermediários e Etapas 6 a 10 | **PLANEJADO:** alinhamento técnico, consulta HTTP de domínio, consumidor, recuperação do objeto, pipeline, resultado, publicação e experimentos do fluxo completo. |

A autonomia do fluxo completo de negócio ainda deverá ser demonstrada nas etapas correspondentes. A preparação experimental existente não significa que os cenários dependentes de worker, broker, storage ou extração já sejam executáveis.

### Ordem de execução futura aprovada

1. Realinhamento documental deste `PLAN.md`, sem implementação técnica.
2. Alinhamento técnico pré-Etapa 6 — Stack e PostgreSQL.
3. API HTTP de domínio do Processing.
4. Etapa 6 — Integração com RabbitMQ.
5. Etapa 7 — Recuperação do documento.
6. Etapa 8 — Pipeline de processamento e `ProcessingResult`.
7. Etapa 9 — Evento de saída e fluxo ponta a ponta.
8. Etapa 10 — Carga e resiliência.

Os dois blocos intermediários serão executados após as entregas da Etapa 5 e antes da Etapa 6, sem renumerar as Etapas 1 a 10. Cada implementação futura depende de autorização explícita.

## Etapa 1 — Bootstrap do Processing

Preparar o novo repositório utilizando o template adotado no projeto DocPipe.

Objetivos:

- validar estrutura do projeto;
- configurar `uv`;
- configurar ferramentas de desenvolvimento;
- configurar testes;
- configurar lint;
- validar execução local;
- criar documentação inicial.

Resultado esperado:

**Processing executando isoladamente com ambiente de desenvolvimento funcional.**

---

## Etapa 2 — Qualidade, testes e CI

Estabelecer a infraestrutura de qualidade antes da implementação das funcionalidades principais.

A partir desta etapa, todas as funcionalidades adicionadas ao Processing deverão passar pelas validações automatizadas aplicáveis.

### Testes

Preparar a estrutura para:

- testes unitários;
- testes de integração;
- testes de contratos;
- testes de persistência;
- testes do consumidor;
- testes de armazenamento;
- testes do fluxo completo;
- cobertura automatizada.

Nem todas essas categorias precisarão possuir testes funcionais nesta etapa, pois algumas dependem de funcionalidades que ainda serão implementadas.

O objetivo é deixar a **estrutura preparada desde o início** e expandir a suíte progressivamente.

### Qualidade

Configurar as validações adotadas pelo projeto, incluindo:

- lint;
- formatação quando aplicável;
- análise estática;
- verificação de tipos quando adotada pelo projeto;
- cobertura mínima;
- validações de arquivos e configuração.

### GitHub Actions

Criar o pipeline de integração contínua próprio seguindo, quando aplicável, as convenções do projeto, sem depender do checkout, testes ou runtime do Ingestion.

O CI deverá executar automaticamente as validações relevantes para cada mudança.

Preparar checks para:

- qualidade;
- testes;
- cobertura;
- build;
- integração;
- smoke test quando a infraestrutura necessária estiver disponível.

### Docker

Validar desde esta etapa:

- construção da imagem;
- inicialização do container;
- configuração por variáveis de ambiente;
- execução mínima da aplicação.

### Proteção da `main`

Preparar o repositório para utilizar o mesmo modelo de governança adotado no Ingestion, incluindo o ruleset reutilizável do projeto.

O merge deverá depender dos checks obrigatórios definidos para o Processing.

Preservar os nomes existentes `quality`, `tests` e `container-smoke` nas futuras adaptações, conferindo sua correspondência com o ruleset da `main` antes de qualquer mudança de governança. Esta revisão não altera nem comprova a configuração remota do ruleset.

### Evolução contínua

As etapas posteriores não deverão criar uma grande fase de testes somente no final.

Cada nova funcionalidade deverá adicionar ou atualizar seus respectivos testes.

Preservar a observabilidade e a preparação experimental já entregues. Habilitar os cenários pequenos de reentrega, reinício e indisponibilidade conforme seus componentes forem implementados; a campanha de carga e resiliência permanece na Etapa 10, fora da CI normal de Pull Requests.

Exemplo:

**implementação → testes → CI → revisão → merge**

Resultado esperado:

**pipeline de qualidade e integração contínua funcionando antes da implementação do domínio principal.**

---

## Etapa 3 — Domínio e modelo de processamento

Definir os conceitos internos necessários para representar um processamento.

Exemplos:

- identificador do processamento;
- identificador do documento;
- status;
- timestamps;
- tentativas;
- erro;
- metadados necessários.

Estados implementados, substituindo o exemplo inicial com `PENDING`:

`RECEIVED → PROCESSING → COMPLETED`

e, em caso de erro:

`PROCESSING → FAILED`

`ProcessingJob.create` produz `RECEIVED` com `attempts=0`; iniciar o processamento incrementa a tentativa. Preservar UUID próprio, identificadores externos opacos, `correlation_id` anulável, timestamps UTC e invariantes das transições.

Estratégias de retry e retomada continuam futuras; estados conceituais como `RETRY_WAIT` não estão implementados. `ProcessingResult` pertence à Etapa 8. A mudança de banco não deverá alterar o ciclo de vida existente.

Os testes unitários correspondentes deverão ser adicionados nesta mesma etapa.

---

## Etapa 4 — Persistência própria

Implementar a persistência dos dados pertencentes ao Processing.

O banco deverá armazenar somente informações necessárias ao próprio serviço.

O Processing não deverá acessar diretamente o banco do Ingestion.

Nesta etapa deverão ser definidos:

- modelo persistente;
- criação e atualização dos registros;
- consultas necessárias;
- transições de estado;
- prevenção de processamento duplicado.

Adicionar os respectivos testes unitários e de integração.

Resultado esperado:

**estado do processamento persistido independentemente do Ingestion.**

---

## Etapa 5 — Contrato de entrada

Definir formalmente o contrato público versionado aceito pelo Processing, implementado localmente e utilizável por qualquer produtor autorizado e compatível.

O contrato deverá fornecer informações suficientes para localizar e identificar o documento sem expor detalhes internos do produtor. A especificação vigente é [document.received.v1](contracts/document_received_v1.md); esta diretriz preserva suas regras, incluindo identificadores opacos e `correlation_id` opcional e anulável.

Deverão ser considerados:

- versão do evento;
- identificador do evento;
- identificador do documento;
- localização/referência do objeto;
- timestamp;
- correlation ID;
- metadados estritamente necessários.

O contrato deverá possuir testes próprios com payloads e fixtures sintéticos locais, sem código, fixtures privadas ou runtime de outro serviço e sem broker para validar o schema.

---

## Alinhamento técnico pré-Etapa 6 — Stack e PostgreSQL

**PLANEJADO:** adequação técnica posterior às entregas das Etapas 1 a 5, sem refazê-las. Concluir este bloco antes da API HTTP de domínio e da Etapa 6.

Escopo da futura implementação:

1. Alinhar Python 3.14 em `requires-python`, `.python-version`, Dockerfile, GitHub Actions, MyPy e documentação, selecionando patches compatíveis e reproduzíveis.
2. Revisar bibliotecas compartilhadas conforme a política híbrida, preservando versões compatíveis e lockfile próprio; adicionar dependências somente por necessidade real e via `uv`.
3. Migrar a persistência operacional SQLite para PostgreSQL, mantendo domínio, estados e invariantes de `ProcessingJob`.
4. Adotar `postgres:17.11-bookworm`, banco lógico e credenciais exclusivos do Processing.
5. Utilizar Psycopg 3 em versão compatível com Python 3.14 e com as plataformas utilizadas, validando a disponibilidade dos artefatos necessários.
6. Preservar SQLAlchemy ORM e a abordagem síncrona existente, sem conversão assíncrona apenas pela troca de banco.
7. Preservar Alembic e avaliar a compatibilidade da migration inicial com PostgreSQL.
8. Adaptar `PROCESSING_DATABASE_URL` para o dialeto `postgresql+psycopg`, com configuração externa segura e sem credenciais no código ou na imagem; explicitar a configuração PostgreSQL sem fallback silencioso para SQLite.
9. Revisar repository, limites de transação, commit, rollback, ciclo de vida de engines e sessões, mantendo sessões isoladas por operação e por processo.
10. Adaptar `IntegrityError` para Psycopg 3: reconhecer duplicata de evento por SQLSTATE e constraint correspondente, sem confundir colisão de chave primária ou outras violações com deduplicação.
11. Validar UUID nativo e round-trip de timestamps com timezone, normalizados para UTC, preservando os identificadores externos como strings opacas e a correlação anulável.
12. Preservar constraints existentes, incluindo tentativas não negativas e unicidade de `source_event_id`.
13. Adaptar os testes de persistência para PostgreSQL real, com isolamento dos dados, migrations próprias, round-trip dos estados, rollback e criações concorrentes do mesmo evento.
14. Configurar PostgreSQL no Docker Compose próprio, com volume e healthcheck, sem depender de outro microsserviço.
15. Ajustar CI e smoke tests à infraestrutura necessária, mantendo os nomes dos checks existentes e a cobertura mínima; incluir a task MyPy no check de qualidade e validar instalação pelo lockfile.
16. Atualizar a documentação operacional conforme as mudanças forem efetivamente entregues, incluindo configuração, inicialização do banco, migrations e execução dos testes.

### Histórico de migrations e dados locais

Preservar o histórico Alembic. A migration `0001_processing_jobs` utiliza tipos portáveis de UUID e timestamps com timezone e constraints nomeadas; sua execução em PostgreSQL ainda deverá ser validada. Não excluir nem reescrever migrations indiscriminadamente. Se houver evolução necessária do schema, preferir nova revisão com justificativa e validação dos impactos.

**PENDENTE:** inventariar eventuais bancos SQLite locais na futura implementação e decidir entre transferência dos dados e inicialização de PostgreSQL vazio. Não presumir que dados existentes podem ser descartados. Havendo dados relevantes, planejar backup, transferência validada, conferência de identificadores, timestamps e unicidade e possibilidade de retorno. Migração de schema via Alembic não transfere automaticamente dados entre os dois bancos.

Critério de conclusão: persistência e migrations verificadas em PostgreSQL homologado, domínio e contrato preservados, testes e checks aplicáveis aprovados e operação documentada com apenas este checkout e a infraestrutura própria. Nenhuma migration, dependência ou infraestrutura é executada ou alterada nesta revisão documental.

---

## API HTTP de domínio do Processing

**PLANEJADO:** implementar após o alinhamento técnico e antes da Etapa 6. A API atual possui apenas capacidades operacionais, como saúde e métricas; a consulta de domínio ainda não existe.

Endpoint inicial aprovado:

```http
GET /v1/processing-jobs/{processing_id}
```

A futura implementação deverá:

- consultar `ProcessingJob` pelo UUID próprio usando o repository e somente o banco do Processing;
- definir schema HTTP explícito e versionado, com identidade, estado, tentativas, timestamps, correlação e diagnóstico técnico seguro quando aplicável;
- tratar identificadores inválidos e retornar resposta adequada para recurso inexistente;
- documentar respostas e erros no OpenAPI, sem serializar diretamente o ORM;
- preservar privacidade, sem expor conteúdo documental, texto extraído, credenciais ou detalhes internos de exceções;
- adicionar testes automatizados de contrato HTTP e integração com a persistência, incluindo consulta existente, inexistente, identificador inválido e falhas de banco;
- operar sem consumer ou RabbitMQ quando a consulta depender somente de recursos persistidos, sem exigir Ingestion, Triage ou Registry.

Preservar `/health` e `/metrics`. Não adicionar outros endpoints sem justificativa funcional, nem criar upload, reprocessamento ou capacidades de Triage e Registry para demonstrar autonomia.

Critério de conclusão: consulta versionada documentada e testada sobre persistência própria, com erros previsíveis e execução independente do broker. Nenhum endpoint é implementado por esta revisão documental.

---

## Etapa 6 — Integração com RabbitMQ

**PLANEJADO:** implementar a aceitação durável dos eventos após concluir os dois blocos intermediários. Esta etapa não executará processamento documental real.

Decisões aprovadas para a futura implementação:

- usar o cliente `aio-pika` e a imagem `rabbitmq:4.3.6-management`;
- executar worker separado da API, com configuração e ciclo de vida próprios;
- manter application handler independente de objetos RabbitMQ, permitindo testes sem broker;
- consumir `document.received.v1` e validar pelo contrato implementado na Etapa 5, sem alterar seus identificadores ou a regra de `correlation_id` opcional e anulável;
- criar `ProcessingJob` no estado `RECEIVED` (`ProcessingStatus.RECEIVED` no código atual), com `attempts=0` e UUID próprio para `processing_id`;
- mapear `event_id` para `source_event_id` e deduplicar por essa chave, com a unicidade garantida pelo banco;
- enviar ACK somente após commit durável ou confirmação de duplicata equivalente já persistida;
- rejeitar mensagens permanentemente inválidas com reject sem requeue e encaminhamento para DLQ simples;
- tratar falhas transitórias sem ACK prematuro e sem ciclos infinitos de reentrega, com comportamento de espera e recuperação explícito e testado;
- propagar W3C Trace Context nos headers de transporte, sem incluí-lo no corpo do contrato;
- testar com produtor sintético compatível e infraestrutura própria, sem executar ou importar código do Ingestion.

Fluxo previsto para um evento válido novo:

**Evento válido → validação → criação do ProcessingJob em RECEIVED → commit → ACK.**

O ACK representa aceitação durável do evento, não conclusão do processamento documental. Preservar entrega pelo menos uma vez; não prometer processamento exatamente uma vez. Duplicata equivalente não deverá criar outro job nem incrementar tentativas; o mesmo identificador com dados incompatíveis não deverá ser reconhecido automaticamente como equivalente.

A aceitação durável deverá preservar também os dados validados da entrada necessários à recuperação posterior, pois o `ProcessingJob` atual não armazena a referência ao objeto, MIME, tamanho e checksum. **PENDENTE:** definir a representação persistente mínima e sua gravação atômica com o job antes de fechar os testes de ACK; isso não inclui recuperar o documento ou criar `ProcessingResult`.

**PENDENTE:** documentar a integração pública AMQP, incluindo exchange, routing keys, filas, bindings, permissões, headers exatos e parâmetros de tratamento transitório e DLQ. Não inventar esses valores como contratos existentes; verificar evidências de compatibilidade sem exigir checkout ou runtime do produtor. As políticas básicas de ACK, reject e DLQ descritas acima já estão decididas.

Adicionar testes do handler, consumidor e integração com broker e PostgreSQL: entrada válida, duplicata equivalente, conflito de conteúdo, rejeição permanente, encaminhamento à DLQ, falha transitória, correlação, Trace Context e queda entre commit e ACK. A API deverá permanecer consultável sem o worker ou broker quando seu banco estiver disponível.

Ficam fora da Etapa 6: recuperação de documentos, OCR, extração de conteúdo, `ProcessingResult`, evento de saída, outbox e reprocessamento manual. Mensagens inválidas não deverão forçar transições que violem as invariantes do domínio.

---

## Etapa 7 — Recuperação do documento

Integrar o Processing ao armazenamento utilizado pelo DocPipe.

No ambiente local, deverá ser utilizada a solução definida para simular o armazenamento de objetos.

O Processing deverá:

- localizar o objeto através das informações recebidas;
- recuperar o documento;
- validar sua disponibilidade;
- identificar falhas de acesso;
- diferenciar erros temporários de erros permanentes quando possível.

O Processing deverá usar a referência explícita no contrato por interface pública de armazenamento e adaptador próprio, sem depender do filesystem interno ou runtime de qualquer produtor.

Adicionar testes da integração com armazenamento.

---

## Etapa 8 — Pipeline de processamento

Implementar o pipeline principal do serviço.

Inicialmente, o pipeline deverá ser simples, determinístico e facilmente testável.

O processamento deverá possuir limites claros entre:

**entrada → validação → processamento → resultado**

A arquitetura deverá permitir que novas etapas sejam adicionadas futuramente sem transformar o serviço em um conjunto fortemente acoplado.

**PLANEJADO:** definir e implementar `ProcessingResult` nesta etapa, junto ao primeiro pipeline real. Avaliar metadados técnicos, vínculo com `ProcessingJob`, formato do artefato privado, persistência e migrations necessárias. Motor, idiomas, política de OCR, ausência de texto, limites, retenção e acesso ao resultado continuam **PENDENTES** de decisão conforme as necessidades verificadas.

O resultado deverá ser técnico e reutilizável, sem classificar documentos, executar validações de negócio do Triage ou produzir o `DocumentRecord` consolidado do Registry. Não acoplar seus modelos internos ao Processing. Originais e resultados permanecerão privados e resultados nunca sobrescreverão o original.

Adicionar testes unitários, de integração e do fluxo de processamento.

---

## Etapa 9 — Evento de saída e fluxo ponta a ponta

Após o processamento, o serviço deverá produzir um evento representando seu resultado.

Fluxo esperado:

**Produtor autorizado e compatível → evento de entrada → Processing → evento de resultado**

O evento deverá permitir que um consumidor autorizado e compatível continue o fluxo sem conhecer detalhes internos do Processing. Não exigir que esse consumidor esteja em execução para concluir o trabalho do Processing; preservar as garantias de publicação definidas.

**PLANEJADO:** implementar outbox própria e publicação com confirmação do broker nesta etapa, preservando atomicidade entre a conclusão persistida e o registro do evento a publicar. O contrato de saída e eventual evento de falha continuam **PENDENTES**; não fixar payloads ou assumir consumidores implementados. Não incluir texto extraído, URLs públicas ou credenciais nos eventos.

Devem existir contratos apropriados para situações como:

- processamento concluído;
- processamento rejeitado;
- processamento que terminou com erro, quando necessário ao fluxo.

Nesta etapa deverá ser validado também o fluxo integrado do Processing:

**evento → consumo → recuperação do documento → processamento → persistência → publicação do resultado**

Os testes end-to-end aplicáveis deverão ser incorporados ao CI criado na Etapa 2. Demonstrar o fluxo com infraestrutura necessária, entradas sintéticas e sem outros microsserviços; ensaios entre serviços podem complementar essa evidência, sem se tornarem requisito de execução isolada.

Resultado esperado:

**Processing funcional de ponta a ponta e protegido pelo pipeline de CI estabelecido desde o início do projeto.**

---

## Etapa 10 — Carga e resiliência

Preparar o Processing para os experimentos do TCC.

Avaliar cenários como:

- sequência elevada de eventos;
- múltiplos documentos aguardando processamento;
- indisponibilidade temporária do RabbitMQ;
- indisponibilidade temporária do armazenamento;
- falha durante processamento;
- mensagens duplicadas;
- reinicialização do serviço;
- recuperação após falhas.

Registrar métricas úteis para comparação experimental, como:

- throughput;
- latência;
- taxa de sucesso;
- taxa de erro;
- tempo de recuperação;
- quantidade de retries;
- comportamento durante indisponibilidade.

Os ensaios pesados poderão ser executados em infraestrutura externa ao notebook de desenvolvimento.

---

## Fluxo esperado

O fluxo abaixo ilustra a composição possível do DocPipe. Não exige executar todos os serviços juntos; produtores e consumidores compatíveis podem ser substituídos nas fronteiras públicas. As setas posteriores ao Processing não fixam contratos ou transportes ainda pendentes entre Triage e Registry. Cada um dos quatro serviços também terá API HTTP e banco próprios.

```text
Documento
    │
    ▼
Ingestion
    │
    ├──► armazenamento
    │
    ▼
RabbitMQ
    │
    ▼
Processing
    │
    ├──► recupera documento
    │
    ├──► processa
    │
    ├──► persiste resultado
    │
    ▼
RabbitMQ
    │
    ▼
Triage — classificação, validação e encaminhamento
    │
    ▼
Registry — DocumentRecord consolidado
```

---

## Fora do escopo inicial

Não incluir prematuramente:

- Kubernetes;
- AKS;
- serviços específicos de Azure;
- infraestrutura de produção;
- observabilidade centralizada do DocPipe;
- dependências diretas dos bancos dos demais microserviços;
- funcionalidades pertencentes aos microserviços posteriores.

Classificação, validação e encaminhamento do Triage e consolidação de `DocumentRecord` pelo Registry permanecem fora da responsabilidade do Processing.

As evoluções permitidas deverão ser introduzidas nas etapas apropriadas do projeto global. Dependências diretas de bancos ou estruturas internas de outros serviços permanecem proibidas, inclusive em evoluções futuras.

---

## Critério de conclusão

O Processing estará funcional quando conseguir executar autonomamente:

```text
receber evento → validar → localizar documento → processar → persistir resultado → publicar evento
```

Esse fluxo deverá possuir testes automatizados e ser reproduzível no ambiente local do DocPipe.

Critérios verificáveis de autonomia, a comprovar nas etapas pertinentes:

- instalar, construir, iniciar, testar e implantar com apenas este checkout e a infraestrutura necessária ao Processing;
- validar entradas de produtores sintéticos compatíveis usando modelos e fixtures próprios, preservando o contrato público vigente;
- executar o fluxo de negócio sem Ingestion e sem consumidor posterior, mantendo entrega, idempotência e rastreabilidade;
- acessar somente banco próprio e objetos explicitamente referenciados pelo contrato, através de interfaces públicas;
- demonstrar que a substituição de produtor ou consumidor compatível e a implantação independente não exigem mudanças no domínio;
- utilizar PostgreSQL homologado, banco lógico e migrations próprios, sem compartilhar estado privado de outros serviços;
- consultar `ProcessingJob` pela API HTTP versionada mesmo sem consumer ou RabbitMQ em execução;
- produzir e persistir `ProcessingResult` técnico conforme o contrato definido na Etapa 8, preservando os limites de Triage e Registry.

Esses critérios não significam que o fluxo completo já esteja implementado ou validado. A execução respeitará os blocos intermediários e as Etapas 6 a 9, sem antecipar suas funcionalidades. A API de domínio atende à necessidade aprovada de consulta e rastreabilidade.

Além disso, o repositório deverá possuir integração contínua capaz de impedir que alterações que violem os critérios de qualidade definidos sejam incorporadas à `main`.

---

## Evolução

Após a conclusão da implementação local, o Processing deverá estar preparado conceitualmente para:

- execução em containers;
- múltiplas instâncias;
- escalabilidade horizontal;
- execução em Kubernetes;
- integração com serviços de cloud;
- observabilidade centralizada;
- experimentos controlados de carga e resiliência.

Essas capacidades devem ser habilitadas posteriormente sem alterar a responsabilidade fundamental do microserviço.
