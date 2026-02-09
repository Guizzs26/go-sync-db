#!/bin/bash

echo "Aguardando o Broker estabilizar (passar de unhealthy)..."
sleep 20

docker exec sync-broker rabbitmqadmin declare exchange --name orders.v1.dlx --type direct

docker exec sync-broker rabbitmqadmin declare queue --name orders.sync.firebird.dlq --durable true

docker exec sync-broker rabbitmqadmin declare binding \
    --source orders.v1.dlx \
    --destination-type queue \
    --destination orders.sync.firebird.dlq \
    --routing-key sync.fail

docker exec sync-broker rabbitmqctl set_policy sync-resilience "^orders.sync" \
  '{"dead-letter-exchange":"orders.v1.dlx", "dead-letter-routing-key":"sync.fail"}' \
  --apply-to queues

echo "Configuração finalizada com sucesso!"
