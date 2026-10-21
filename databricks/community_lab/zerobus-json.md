# Preservar o tipo de um valor JSON antes do envio

Um produtor recebe valores Python nativos. Se uma coluna VARIANT requer JSON
serializado, a string `"5"` deve continuar sendo string; não pode virar o número 5.
O exemplo codifica apenas as colunas designadas. Usa somente dados sintéticos.

Origem: [correção pública de Jennifer na Community, 19/10/2026](https://community.databricks.com/t5/data-engineering/zerobus-variant-data-not-processed-as-expected-json-data/m-p/173977#M56790).
A resposta anterior 173667 está marcada como aceita no fórum; essa marca não
comprova a execução desta correção no serviço.

## Resultado esperado

```python
from databricks.community_lab.zerobus_json_encoding import encode_variant_fields
# Execute a partir da raiz do repositório, sem outro pacote databricks instalado.
record = {"id": 7, "ValueJson": "5"}
encoded = encode_variant_fields(record)
# encoded == {"id": 7, "ValueJson": '"5"'}
```

O texto interno contém aspas JSON. Depois de `json.loads(encoded["ValueJson"])`,
o resultado é a string `"5"`. Objetos, listas, booleanos, números e `None` também
preservam seu valor JSON. `None` vira o texto JSON `null`, não SQL NULL. NaN e
Infinity são recusados, inclusive dentro de listas e objetos. Campos não
selecionados não são validados nem copiados profundamente.

## Executar as oito verificações

Python 3.12, biblioteca padrão, sem instalação adicional:

```sh
python -m unittest discover -s tests -p 'test_variant_encoding.py' -v
```

Os testes carregam o arquivo diretamente, sem depender do namespace `databricks`.
O comando completo `python -m unittest discover -s tests -v` inclui esses testes
mais os seis de vendas. Os oito testes contam verificações, não projetos.

**Limite:** validação local de serialização. Nenhum endpoint Zerobus, workspace,
VARIANT SQL, broker Kafka ou conta AWS foi acessado. JSON previamente serializado
deve ser decodificado explicitamente pelo chamador; o helper não tenta adivinhar
isso pelo tipo `str`. Esta publicação não comprova integração no serviço.
