# Camada Silver

## Visão geral

A Bronze preserva o evento recebido e os metadados do Kafka sem aplicar regras
de negócio. A Silver interpreta esse JSON com schema explícito, aplica validações
técnicas, remove duplicatas, cria atributos analíticos e calcula sinais
explicáveis de risco.

```text
Bronze Parquet
  -> parsing e validação
  -> deduplicação e enriquecimento
  -> regras de risco
  -> Silver Parquet
```

As saídas ficam no bucket `fraud-data-lake`:

- `silver/transactions`: eventos válidos, enriquecidos e pontuados;
- `silver/invalid-events`: payload bruto, metadados Kafka e todos os erros;
- `silver/fraud-alerts`: subconjunto cuja pontuação atingiu o limite de alerta.

## Validações técnicas

O parsing usa `from_json` e `StructType`, sem Pydantic, UDF Python ou pandas.
São rejeitados JSON corrompido, versão de schema incompatível, UUID inválido,
campos obrigatórios ausentes, tipo desconhecido, valores monetários negativos,
contas fora dos padrões PaySim e timestamps ausentes ou inválidos. A coluna
`validation_errors` acumula todas as falhas encontradas.

Inconsistências de saldo não tornam o evento tecnicamente inválido. Elas são
sinais de negócio mantidos nas transações válidas. Saldos de destino de contas
comerciais (`M...`) não passam pela mesma verificação, pois o PaySim pode não
informá-los.

## Tempo do evento, deduplicação e watermark

O producer deriva `event_time` de forma determinística: a data configurada em
`PAYSIM_SIMULATION_START` mais `step - 1` horas. O ID continua derivado apenas do
registro da fonte; timestamps de processamento não participam dele.

Eventos válidos usam watermark configurável por `event_time` e são deduplicados
por `event_id`. Isso limita o estado mantido pelo Spark. Um evento duplicado que
chegar depois da janela do watermark poderá voltar a aparecer.

Não há promessa de exactly-once ponta a ponta. A estratégia combina checkpoints
do Spark, ID determinístico e deduplicação no destino. Cada sink possui seu
próprio checkpoint, pois compartilhar estado entre queries é incorreto.

## Enriquecimentos e anomalias

A Silver adiciona data/hora do evento, identificação de comerciante, variações
de saldo, saldo esperado da origem, diferença observada, flags de anomalia,
conta esvaziada, latência de processamento e faixa de valor.

## Regras de risco

As regras iniciais detectam valor elevado, `TRANSFER`/`CASH_OUT`, conta de origem
esvaziada e anomalia de saldo da origem. Cada regra possui coluna booleana e peso.
O resultado inclui `triggered_rules`, `risk_score`, `risk_level` e
`predicted_fraud`.

`is_fraud` e `is_flagged_fraud` são rótulos do dataset. Eles não entram em
nenhuma regra ou cálculo, evitando data leakage. Servirão posteriormente para
avaliar a qualidade das previsões.

## Configuração

Todos os caminhos, checkpoints, watermark, micro-batch, limites e pesos estão no
`.env.example`. Caminhos `s3a://` usam `MINIO_ENDPOINT`, `MINIO_ROOT_USER` e
`MINIO_ROOT_PASSWORD` do `.env`, que não deve ser versionado. Para filesystem
local, substitua cada caminho por uma URI `file:///...`; nesse caso as
credenciais MinIO não são necessárias.

## Execução

Suba todo o fluxo:

```bash
docker compose -f infrastructure/streaming/docker-compose.yml up -d
```

Acompanhe a Silver:

```bash
docker compose -f infrastructure/streaming/docker-compose.yml logs -f silver-stream
```

Publique eventos novos:

```bash
poetry run python -m fraud_streaming_pipeline.main \
  --publisher kafka --limit 10 --events-per-second 2
```

No console do MinIO (`http://localhost:9001`), abra o bucket
`fraud-data-lake` e confira os três prefixos em `silver/`.

Também é possível listar os objetos pelo cliente do Compose:

```bash
docker compose -f infrastructure/streaming/docker-compose.yml run --rm \
  --entrypoint /bin/sh minio-init -c \
  'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" \
  "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls --recursive \
  "local/$MINIO_BUCKET/silver"'
```

## Limitações iniciais

- As regras são heurísticas e ainda precisam ser avaliadas contra os rótulos.
- O destino é Parquet, sem transações ACID de Delta Lake ou Iceberg.
- Arquivos pequenos podem exigir compactação periódica.
- Alterar schemas ou regras com estado pode exigir migração de checkpoint.
- A deduplicação é limitada pela janela configurada de watermark.
