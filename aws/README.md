# Pipelines e armazenamento S3

[Replay de lote imutável](day_001_replay/README.md): exemplo executável com boto3
e sete testes locais com Moto aprovados em 21/10/2026. Integração AWS real pendente
de um sandbox autenticado; os testes simulados não comprovam execução no S3.

## Requisitos da próxima entrega
Conta de sandbox; região e recursos explícitos; credenciais fora do código; estimar custos e documentar limpeza.

Usar `day_NNN_assunto/` com README, código, dados sintéticos e testes.
Documentar no README do exemplo os comandos, versões, resultados esperados e limites de validação.
Separar testes locais ou simulados da validação real no serviço.


## Artigo relacionado

[Your S3 Pipeline Needs a Replay Plan, Not More Optimism — Jennifer Evangelista, Medium](https://medium.com/@jenniferevangelista.code/your-s3-pipeline-needs-a-replay-plan-not-more-optimism-a62342b93e89) trata de replay, checkpoints e idempotência. O [laboratório de preparação de CDC](../pyspark/day_001_cdc/README.md) testa ordenação, retries e conflitos em um lote PySpark local. Esse teste não executa S3, Auto Loader nem um pipeline AWS.
