# DocPipe Processing — Plano de Desenvolvimento

## 1. Objetivo

O **Processing** é o microserviço responsável pelo processamento dos documentos recebidos pelo DocPipe após a etapa de ingestão.

Ele deverá operar de forma independente do microserviço **Ingestion**, comunicando-se por contratos bem definidos e processamento assíncrono.

O serviço deve ser desenvolvido e testado isoladamente, mantendo baixo acoplamento com os demais componentes do DocPipe.

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

O fluxo esperado é:

**Documento → Ingestion → armazenamento → evento → Processing**

O Ingestion é responsável por receber e registrar a entrada do documento.

Depois que o documento estiver disponível, o Ingestion publica um evento no broker.

O Processing consome esse evento e inicia seu trabalho.

O Processing não deve depender diretamente do banco de dados interno do Ingestion.

A integração deve ocorrer através de:

- mensagens;
- contratos de eventos;
- identificadores;
- armazenamento compartilhado quando necessário.

Cada microserviço mantém seus próprios dados e responsabilidades.

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

Criar o pipeline de integração contínua seguindo, quando aplicável, o padrão já consolidado no Ingestion.

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

Definir formalmente o evento produzido pelo Ingestion e consumido pelo Processing.

O contrato deverá fornecer informações suficientes para localizar e identificar o documento sem expor detalhes internos desnecessários do Ingestion.

Deverão ser considerados:

- versão do evento;
- identificador do evento;
- identificador do documento;
- localização/referência do objeto;
- timestamp;
- correlation ID;
- metadados estritamente necessários.

O contrato deverá possuir testes próprios.

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

Adicionar testes do consumidor e testes de integração com o broker.

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

O Processing não deverá depender do filesystem interno do Ingestion.

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

**Ingestion → evento de entrada → Processing → evento de resultado**

O evento deverá permitir que o próximo microserviço do DocPipe continue o fluxo sem conhecer detalhes internos do Processing.

Devem existir contratos apropriados para situações como:

- processamento concluído;
- processamento rejeitado;
- processamento que terminou com erro, quando necessário ao fluxo.

Nesta etapa deverá ser validado também o fluxo integrado do Processing:

**evento → consumo → recuperação do documento → processamento → persistência → publicação do resultado**

Os testes end-to-end aplicáveis deverão ser incorporados ao CI criado na Etapa 2.

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

Esses componentes deverão ser introduzidos nas etapas apropriadas do projeto global.

---

## Critério de conclusão

O Processing estará funcional quando conseguir executar autonomamente:

```text
receber evento → validar → localizar documento → processar → persistir resultado → publicar evento
```

Esse fluxo deverá possuir testes automatizados e ser reproduzível no ambiente local do DocPipe.

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