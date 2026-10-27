# Correções de consumo, sem perder o sinal negativo

Uma correção pode chegar em registros separados. Neste exemplo fictício, o consumo inicial é 10 DBUs. Uma reversão de -10 cancela esse valor, e o registro corrigido informa 7,5 DBUs.

| Registro | Quantidade |
|---|---:|
| ORIGINAL | 10 |
| RETRACTION | -10 |
| RESTATEMENT | 7,5 |
| Total com sinal | 7,5 |

Somar apenas as quantidades positivas daria 17,5. Filtrar apenas ORIGINAL manteria 10. A documentação de system.billing.usage descreve as correções com RETRACTION e RESTATEMENT; um cancelamento também pode vir sem substituição.

## O que foi conferido

A referência local em Python 3.12.10 passou em 19 testes em 24/10/2026. Eles cobrem correção tardia, cancelamento, separação de SKUs e unidades, valores decimais e repetição de eventos sintéticos. O corte de chegada é exclusivo e separado da data do consumo.

Este guia não inclui arquivos executáveis. A referência usa dados fictícios, sem consultar Databricks, calcular preços, aplicar franquias ou validar faturas. Seu event_id identifica um evento do transporte sintético e não equivale ao record_id da Databricks.

## Antes de aplicar em dados reais

Defina a identidade dos registros, a completude da exportação e o tratamento das correções tardias. Confira as unidades e SKUs antes de somar. Valide esses contratos no ambiente real antes de interpretar o resultado como custo ou cobrança.

Fonte oficial:
https://docs.databricks.com/aws/en/admin/system-tables/billing
