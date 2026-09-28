# Baseline experimental inicial

## Estado nesta etapa

Este documento define o protocolo e as dependências do ensaio. Nenhum baseline
de processamento foi executado: o repositório ainda não tem worker, persistência,
consumidor RabbitMQ, adaptador de storage ou extração de documentos. Não há
resultados medidos nesta etapa.

O ensaio é manual e separado da CI de Pull Requests. Não use documentos reais.

## Protocolo quando os componentes estiverem disponíveis

1. Gerar e registrar um corpus sintético pequeno com PDF, PNG e JPEG, incluindo
   os parâmetros usados para criá-lo e seus hashes.
2. Registrar commit, versões de Python e dependências, sistema operacional,
   recursos alocados, configuração do worker e limites de execução.
3. Executar primeiro com uma instância e orçamento fixo. Repetir o mesmo corpus
   e configuração, preservando a saída bruta e sem estabelecer uma meta antes
   da primeira medição.
4. Registrar taxa de conclusão, latências p50/p95/p99, backlog, tentativas,
   CPU, memória e erros observados. Não registrar conteúdo extraído.
5. Parar de forma segura ao atingir o limite de tempo ou recursos definido para
   a execução e preservar a configuração e os resultados para reprodução.

## Cenários condicionados

Reentrega e reinício exigem consumidor, estado durável e comportamento de
retomada. Indisponibilidade temporária exige RabbitMQ e storage disponíveis em
ambiente local controlado. Esses cenários só devem ser habilitados depois que
as respectivas integrações e políticas forem implementadas e testadas nas
etapas correspondentes. Este protocolo não fixa topologia, ACK, retry, OCR,
limites numéricos ou formato final de resultados.

Comparações de concorrência e carga pesada ficam fora da CI normal e pertencem
a uma campanha experimental posterior.
