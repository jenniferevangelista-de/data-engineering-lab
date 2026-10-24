# Dia 002 — comparar payloads de CDC por hash canônico

Dados sintéticos; Spark local real, sem credenciais AWS ou workspace Databricks.
PySpark 3.5.8. Não pressupõe equivalência com DBR.

## Artigo e ideia relacionados

[Your Delta MERGE Is Not a Mind Reader](https://dev.to/jenniferevangelistade/your-delta-merge-is-not-a-mind-reader-2ppp)
([versão no Medium](https://medium.com/the-pipeline-ai-ml-and-cloud/your-delta-merge-is-not-a-mind-reader-16e81b6c788b))
rejeita lotes em que a mesma chave e sequência trazem payloads diferentes.
A sugestão de comparar um hash das colunas de valor, em vez das colunas cruas, veio de
[Karan Pulyala, nos comentários](https://dev.to/karanpulyala/comment/3ink7).
Este laboratório testa essa ideia e mede o que ela resolve e o que não resolve.
Complementa o [Dia 001](../day_001_cdc/README.md); não o substitui.

## Executar
```sh
python -m pip install -r pyspark/day_002_payload_hash/requirements.txt
python -m unittest discover -s pyspark/day_002_payload_hash -p 'test_*.py' -v
```
No Windows, defina JAVA_HOME e SPARK_LOCAL_IP=127.0.0.1 se o hostname não resolver.

## Contrato
`payload_hash(values, scales=None)` devolve `sha2(to_json(struct(values...)), 256)`.
`conflict_report(batch, key, sequence, values, scales)` lista, para cada chave e
sequência com payloads diferentes, uma linha por hash distinto, com os valores crus
ao lado do hash. Resultado vazio significa ausência de conflito.
A chave deve estar normalizada e validada antes (veja o Dia 001).

## O que os testes mostram
- O hash é igual ao SHA-256 do JSON calculado em Python, então a forma canônica é conhecida.
- Retry exato não é conflito; payload diferente é reportado com valores e hashes.
- Float com ruído (`0.1 + 0.2` contra `0.3`) **gera falso conflito** se hasheado cru.
  Arredondar antes do hash (`scales={"amount": 2}`) remove o falso conflito, e uma
  mudança real (`0.30` contra `0.31`) continua detectada. O hash não absorve ruído
  sozinho; a normalização antes dele é que absorve.
- Colunas fora de `values` não influenciam o hash, então uma coluna nova não vira
  conflito. Com `distinct()` na linha inteira, ela viraria.
- NULL e string vazia continuam diferentes.
- **A ordem das colunas muda o hash.** Mantenha a lista fixa no código; não dependa
  da ordem das colunas do DataFrame.

## Limites
Não há mock. Os dez testes usam poucas linhas e não são benchmark. O hash serve para
comparar e reportar; não substitui o contrato de identidade e versão do Dia 001.
Colisões de SHA-256 são desprezíveis aqui, mas o relatório mantém os valores crus
para quem investiga. Não foram executados MERGE Delta, Databricks, S3 nem streaming.
O comportamento de `to_json` com tipos complexos (structs aninhados, mapas) não foi coberto.

Fontes: [to_json](https://spark.apache.org/docs/3.5.8/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html),
[sha2](https://spark.apache.org/docs/3.5.8/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sha2.html).
