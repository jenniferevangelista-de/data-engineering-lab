# Replay de lote imutável em S3

O mesmo lote pode ser reenviado sem sobrescrever seu conteúdo. A chave é estável,
os eventos são serializados em ordem determinística e `IfNoneMatch="*"` protege
a criação. Se a chave já existe, o corpo e o SHA-256 precisam coincidir; divergências
interrompem o processamento. Eventos idênticos são deduplicados por `(id, version)`;
versões diferentes são preservadas, não transformadas em um estado CDC final.

## Verificação local — 21/10/2026

Sete testes passaram com boto3 1.43.116 e Moto 5.2.4. São chamadas interceptadas
pelo simulador, **não integração validada na AWS**. Cobrem retry, alteração de lote,
conflito, duplicação, ordenação, valores inválidos e integridade do objeto.

```sh
python -m pip install -r aws/day_001_replay/requirements.txt
python -m unittest discover -s aws/day_001_replay -v
```

## Executar em um sandbox AWS existente

Use um perfil local autenticado e um bucket de teste já criado. Não coloque chaves
no código ou no Git. O comando grava somente sob `lab-replay/<run-id>/`; não cria
conta, bucket, políticas, permissões ou recursos de computação.

```sh
python aws/day_001_replay/replay.py --profile SEU_PERFIL --region SUA_REGIAO --bucket SEU_BUCKET --run-id teste-20261021 --events aws/day_001_replay/events.json
```

A primeira execução deve retornar CREATED e REPLAYED. Uma repetição pode retornar
REPLAYED duas vezes. Preserve a saída como evidência, com região, versão e horário.
Há custos usuais de armazenamento e operações S3: um objeto pequeno persistido,
duas tentativas de escrita e duas leituras por execução bem-sucedida. Sem teste
de IAM, concorrência distribuída, falha de
rede, versionamento ou retenção. O prefixo de teste pode ser removido posteriormente
pelo proprietário do sandbox; o programa não apaga objetos.

[Documentação oficial de escritas condicionais](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
