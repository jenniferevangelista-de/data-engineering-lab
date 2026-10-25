# Produção e consumo de eventos

[Envio, leitura e retomada](day_001_recovery/README.md): experimento aprovado em
21/10/2026 com broker Kafka 3.9.1 real local, quatro entregas confirmadas e três
eventos únicos após a retomada. O checkpoint foi de 1 para 4; duas reentregas não
duplicaram os efeitos no SQLite. Não valida queda do broker, réplicas ou produção.

## Requisitos da próxima entrega
Broker de teste; tópicos isolados; declarar versão do cliente; testar duplicação, chave e confirmação de entrega.

Usar `day_NNN_assunto/` com README, código, dados sintéticos e testes.
Documentar no README do exemplo os comandos, versões, resultados esperados e limites de validação.
Separar testes locais ou simulados da validação real no serviço.


## Artigo relacionado

[Your Kafka Consumer Lag Is Not Your SLA — Jennifer Evangelista, Medium](https://medium.com/@jenniferevangelista.code/your-kafka-consumer-lag-is-not-your-sla-offset-math-retention-and-the-missing-commit-trap-0bab431214bc) discute offsets ausentes, retenção e a diferença entre posição de consumo e resultado de negócio. O exemplo Python no artigo classifica snapshots sintéticos; o experimento acima valida retomada local, mas ainda não constitui um coletor de lag em produção.

O recorte de [métricas opcionais](../databricks/community_lab/queries/optional_operation_metrics.sql) demonstra o princípio de preservar NULL em vez de inventar zero em mapas sintéticos do Spark. Não calcula lag Kafka.
