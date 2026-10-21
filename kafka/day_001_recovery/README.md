# Kafka: entrega, retomada e efeitos idempotentes

Experimento executado em **21/10/2026**, com Apache Kafka **3.9.1 real**, um nó KRaft
local, Java 17 e confluent-kafka 2.16.0. Dados sintéticos; não acessa um cluster externo.

Quatro mensagens são confirmadas pelo broker, com chave correspondente ao evento.
O consumidor grava os offsets 0 e 1 no SQLite, mas confirma somente o primeiro.
Uma segunda instância abre o mesmo banco e grupo, retoma no offset 1 e continua.
O payload repetido de um evento é aceito somente se for idêntico ao armazenado.

Resultado observado:

| Evidência | Resultado |
|---|---|
| Offsets confirmados pelo produtor | 0, 1, 2, 3 |
| Sequência de processamento | 0, 1, 1, 2, 3 |
| Checkpoint antes/depois da retomada | 1 → 4 |
| Eventos únicos no destino durável | 3 |
| Entregas repetidas sem novo efeito | 2 |

É uma interrupção **simulada entre a escrita do destino e o commit do offset**:
o primeiro consumidor é fechado com auto-commit desativado. Não é um teste de
queda do broker, perda de disco, réplica, concorrência ou garantia geral de exactly-once.

## Reproduzir

Baixe a distribuição oficial Kafka 3.9.1 e confira seu SHA-512. Com Java 17, use
o [quickstart oficial](https://kafka.apache.org/39/getting-started/quickstart/) e a
configuração `server.properties` desta pasta. Os scripts oficiais também têm
equivalentes em `bin/windows/`. O classpath Java direto evita limitações de tamanho
do comando dos wrappers Windows:

```powershell
$env:KAFKA_HOME = 'CAMINHO/DA/DISTRIBUICAO'
java -cp "$env:KAFKA_HOME/libs/*" kafka.tools.StorageTool random-uuid
# Copie o ID gerado para CLUSTER_ID; formate apenas um diretório novo do laboratório.
java -cp "$env:KAFKA_HOME/libs/*" kafka.tools.StorageTool format -t CLUSTER_ID -c kafka/day_001_recovery/server.properties
java -cp "$env:KAFKA_HOME/libs/*" kafka.Kafka kafka/day_001_recovery/server.properties
```

Em outro terminal, na raiz deste repositório:

```sh
python -m pip install -r kafka/day_001_recovery/requirements.txt
python kafka/day_001_recovery/recovery.py
```

O teste cria tópico/grupo com sufixo UUID e banco SQLite em `work/kafka-state/`.
Aceita somente `127.0.0.1:19092` ou `localhost:19092` para evitar cluster de produção.
O broker deve ficar ligado apenas no loopback; interrompa com Ctrl+C ao terminar.
Os arquivos de log e SQLite ficam disponíveis para inspeção. Remova apenas os
diretórios descartáveis do laboratório quando não forem mais necessários.
