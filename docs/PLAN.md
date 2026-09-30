# DocPipe Processing — Plano de Desenvolvimento

## 1. Objetivo

O **Processing** é o microserviço responsável pelo processamento dos documentos recebidos pelo DocPipe após a etapa de ingestão.

Ele deverá operar sem exigir outros microsserviços em execução, recebendo entradas de qualquer produtor autorizado e compatível por contratos públicos versionados e processamento assíncrono.

O serviço deve ser desenvolvido, instalado, construído, iniciado, testado e implantado sem checkout ou runtime de outros microsserviços. A diretriz obrigatória e as evidências atuais estão nas seções 2.1 e 2.2 do [DESIGN](DESIGN.md). Esta atualização acrescenta critérios de autonomia, preserva a sequência das etapas e não declara adequações técnicas concluídas.

---

## 2. Responsabilidade do microserviço

O Processing deverá:

- receber eventos informando que um documento está disponível para processamento;
- identificar o documento e suas informações necessárias;
- recuperar o documento do armazenamento;
- executar o pipeline de processamento definido para o DocPipe;
- registrar o estado e o resultado do processamento;
- publicar um novo evento indicando sucesso ou falha;
- permitir rastrear cada processamento por identificadores de correlação.

O Processing não deverá assumir responsabilidades pertencentes ao Ingestion.

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

Cada microserviço mantém seus próprios dados e responsabilidades. Banco, RabbitMQ e armazenamento são infraestrutura permitida para operação isolada. Compartilhar instâncias não autoriza compartilhar tabelas, modelos ou estado interno. Autonomia não exige nova API HTTP, CLI ou modo de execução.

---

## 4. Arquitetura inicial

O Processing seguirá os mesmos princípios gerais utilizados no Ingestion:

- Python moderno;
- FastAPI;
- gerenciamento de dependências com `uv`;
- configuração por variáveis de ambiente;
- banco de dados próprio;
- RabbitMQ para comunicação assíncrona;
- armazenamento compatível com o ambiente local do DocPipe;
- Docker;
- Docker Compose;
- testes automatizados;
- lint e validações de qualidade;
- GitHub Actions.

Para a versão local do DocPipe, serviços externos de cloud deverão ser substituídos por alternativas locais quando aplicável.

Integrações específicas com provedores de cloud ficam reservadas para uma evolução posterior.

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

### Evolução contínua

As etapas posteriores não deverão criar uma grande fase de testes somente no final.

Cada nova funcionalidade deverá adicionar ou atualizar seus respectivos testes.

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
- resultado;
- erro;
- metadados necessários.

Estados iniciais sugeridos:

`PENDING → PROCESSING → COMPLETED`

e, em caso de erro:

`PROCESSING → FAILED`

O modelo deverá permitir futuras estratégias de retry sem exigir alteração significativa do domínio.

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

## Etapa 6 — Integração com RabbitMQ

Implementar o consumidor responsável por receber eventos destinados ao Processing.

O consumidor deverá:

- receber a mensagem;
- validar o contrato;
- identificar mensagens inválidas;
- iniciar o processamento;
- confirmar mensagens corretamente processadas;
- tratar falhas de maneira previsível;
- evitar processamento duplicado quando possível.

A política de ACK, retry e mensagens problemáticas deverá ser explicitamente documentada.

Adicionar testes do consumidor e testes de integração com o broker, usando produtores sintéticos compatíveis sem exigir Ingestion. Preservar as garantias definidas de entrega, idempotência e rastreabilidade.

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

Adicionar testes unitários, de integração e do fluxo de processamento.

---

## Etapa 9 — Evento de saída e fluxo ponta a ponta

Após o processamento, o serviço deverá produzir um evento representando seu resultado.

Fluxo esperado:

**Produtor autorizado e compatível → evento de entrada → Processing → evento de resultado**

O evento deverá permitir que um consumidor autorizado e compatível continue o fluxo sem conhecer detalhes internos do Processing. Não exigir que esse consumidor esteja em execução para concluir o trabalho do Processing; preservar as garantias de publicação definidas.

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

O fluxo abaixo ilustra a composição possível do DocPipe. Não exige executar todos os serviços juntos; produtores e consumidores compatíveis podem ser substituídos nas fronteiras públicas.

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
Próximo microserviço
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
- demonstrar que a substituição de produtor ou consumidor compatível e a implantação independente não exigem mudanças no domínio.

Esses critérios não significam que o fluxo completo já esteja implementado ou validado. Não antecipam as Etapas 6 a 9 nem exigem novas interfaces apenas para demonstrar autonomia.

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
