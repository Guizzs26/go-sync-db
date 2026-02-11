Esse projeto é uma prova de conceito (PoC) de um serviço de missão crítica focado na sincronização de dados entre ecossistemas distintos. O objetivo principal é garantir a sincronização BIDIRECIONAL de databases modernos (PostgreSQL) com servidores legados (Firebird) distribuídos em múltiplas unidades físicas, garantindo integridade, resiliência e sem comprometer a performance da API de origem.

OBS:
- Postgres 16.0
- Firebird 2.5
- O projeto (11/02/2026 contém por enquanto APENAS o fluxo postgres -> firebird) - ainda não 100% finalizado.

- Esse projeto vai simular 1 aplicação Go conectado ao Postgres que precisa estar em sincronização absoluta entre 50 bancos de dados firebird que estariam em diferentes servidores (todos com o mesmo DDL e mesma versão).

A arquitetura:

Evitamos o antipadrão de Dual-Write (escrever em dois bancos simultaneamente), que inevitavelmente causa inconsistências em caso de falhas parciais. Em vez disso, adotamos o 'Transactional Outbox Pattern'.

O fluxo PG -> FB

1. Toda operação de escrita no Postgres que exige replicação ocorre dentro de uma transação única. O dado de negócio e o evento de sincronização (na tabela pg_sync_outbox) são salvos juntos. Ou ambos persistem, ou nada acontece.

CREATE TABLE pg_sync_outbox (
    id BIGSERIAL PRIMARY KEY,
    correlation_id UUID NOT NULL DEFAULT gen_random_uuid(),
    unit_id INT NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    operation CHAR(1) NOT NULL, 
    payload JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'pending', 
    attempts INT DEFAULT 0 CHECK (attempts >= 0),
    error_log TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


Agora vamos falar do serviço principal desse fluxo:

2. O relay service é um serviço intermediário na arquitetura, escrito em Go e tem como objetivo monitorar a tabela de outbox. É um componente robusto com as seguintes funcionalidades:

- Suporte a várias instâncias rodando simultaneamente, sem problema de race condition (SKIP LOCKED).
- At-Least-Once Delivery. A mensagem só é considerada processada no Postgres após o ACK explícito do RabbitMQ.
- Auto Reconnect e hot swap. Possui um sistema de monitoramento de saúde do broker. Se a conexão com o rabbitmq cair, o serviço não morre. Ele entra em 'modo de recuperação', restabelece o link em background e faz o hot swap da instância do cliente sem interromper o ciclo de vida do processo principal.
- Exponential backoff inteligente. Para evitar 'thundering herd' no db, temos um algoritmo de backoff exponencial. O intervalo de retry cresce dinamicamente (de 1s até 60s), dando fôlego para a infra se recuperar.
- Janitor (self-healing-service). Temos um serviço que roda em background, se uma instância do relay crashar, o janitor identifica mensagens que ficaram presas no status 'processing' por muito tempo e as resgata automaticamente, garantindo que o fluxo nunca estagne.
- graceful shutdown atômico. Respeito completo a todos sinais importantes do sistema (sigterm/sigint). Ao ser encerrado, o relay interrompe o processamento, executa um plano de contingência de 5seg para devolver a mensagens ao banco e garante que zero mensagens fiquem 'orfas' ou em um estado inconsistente.
- Whitelist de segunça. O relay valida o nome da tabela e o tipo da operação antes de publicar. Isso impede que metadados malformados ou tabelas não autorizadas cheguem na mensageria.
- Memory Guard para prevenção de OOM durante o processamento em batch
- Temos controle de I/O com context e timeout.
- Sem panic
- Separação de responsabilidades claras, seguindo boas prátiacs de engenharia de software (documentação, clean code, solid quando necessário, design pattern simples de acordo com o Go idiomático).

3. O RabbitMQ garante a seguranças das mensagens em filas persistentes.
- Exchange Topic, isso permite que um único  serviço postgres alimente N servidores Firebird diferentes.
- Routing key: pax.unit.{id}.{table}.{op}. Cada filial (customer) escuta apeans as mensagens da sua própria unit_id.

4. O consumer é um worker feito em Go. Ele escuta a fila do rabbitmq, traduz o JSON para o SQL do Firebird de maneira dinâmica. Possui as seguintes funcionalidades:
- Idempotencia absoluta. Antes de processar, o consumer checa a tabela sync_control. Se o correlation_id já foi aplicado, ele ignora a mensagem. Isso permite reprocessar filas sem duplicar dados.
- Emulei o comportamento de aplicações legadas. Para cada insert o woerker gerencia uma tabela INDICE dentro da mesma tx, garantindo que os IDs gerados sejam consistentes com o sistema local
- QoS 1 configurado com prefetch(1) para garantir que as mensagens sejam processadas na ordem exata que ocorreram, evitando que, por exemplo, um UPDATE chegue antes de um INSERT.
- SQL builder dinâmico. Ele traduz e trata N possíveis problemas do JSON recebimento para o SQL do firebird 2.5

Outros pontos:

Até agora, temos um bom parâmetro de observabilidade com logs estruturados e métricas com o Prometheus. Algumas métricas são:
- Taxa de transferência
- Latência de batch
- Monitoramento de Backlog 
- Pressão na memória
- Probes

O banco de dados possui indices compostos parciais.


---


FLUXO FB -> PG

Em andamento...

Apenas a arquitetura foi definida.