# PostgreSQL Analytics

Banco analítico separado do PostgreSQL de metadados do Airflow. A porta externa
padrão é `5434`; internamente, os containers usam `analytics-postgres:5432`.

As configurações e credenciais ficam exclusivamente no `.env` da raiz. Use o
`.env.example` da raiz como referência.

Suba e verifique o banco pela raiz do repositório:

```bash
docker compose --env-file .env -f infrastructure/analytics/docker-compose.yml up -d postgres
docker compose --env-file .env -f infrastructure/analytics/docker-compose.yml ps
docker compose --env-file .env -f infrastructure/analytics/docker-compose.yml exec postgres \
  pg_isready -U analytics -d fraud_analytics
```

O script `init/001_create_schemas.sql` cria os schemas e tabelas somente quando
o volume é inicializado pela primeira vez. Em volume já existente, aplique-o:

```bash
docker compose --env-file .env -f infrastructure/analytics/docker-compose.yml exec -T postgres \
  psql -U analytics -d fraud_analytics \
  < infrastructure/analytics/init/001_create_schemas.sql
```

## Carga Silver

A carga requer que o Compose de streaming esteja ativo, pois acessa o MinIO pela
rede `fraud-streaming-network`. Configure no `.env` da raiz todas as variáveis
`MINIO_*`, `SILVER_*` e `ANALYTICS_DB_*` documentadas no `.env.example`.

```bash
docker compose --env-file .env -f infrastructure/analytics/docker-compose.yml \
  --profile loader run --build --rm silver-loader
```

Execute o mesmo comando novamente para comprovar idempotência. Sem arquivos
novos, a saída terá `records_read: 0` e `records_inserted: 0`. O manifesto
`monitoring.loaded_files`, a PK de `event_id` e `ON CONFLICT` formam três níveis
de proteção contra duplicação.

O loader usa Spark 4.1.2, sem Delta Lake, e fixa o pgJDBC em 42.7.13.

## Isolation Forest

Depois de executar `dbt build`, treine e pontue o modelo:

```bash
docker compose --env-file .env -f infrastructure/analytics/docker-compose.yml \
  --profile ml run --build --rm isolation-forest
```

O job ordena os eventos por `event_time`, usa os primeiros 80% para treino e o
restante para avaliação. Imputação, escala e codificação categórica são ajustadas
somente no treino. Configure `ML_MIN_TRAIN_ROWS`, `ML_TRAIN_FRACTION`,
`ML_CONTAMINATION`, `ML_RANDOM_STATE` e, opcionalmente, `ML_MODEL_VERSION` no
`.env` da raiz.

Os resultados ficam em `analytics.transaction_anomaly_scores`, separados por
versão. A auditoria fica em `monitoring.ml_model_runs`. O modelo não lê nenhum
rótulo de fraude. Depois da pontuação, execute `dbt build` novamente para
atualizar a comparação posterior em `mart_anomaly_evaluation`.

## Gold com dbt em container

A imagem usada pela DAG do Airflow também permite executar a Gold sem instalar
dbt localmente:

```bash
docker compose --env-file .env \
  -f infrastructure/analytics/docker-compose.yml \
  --profile gold run --build --rm dbt-gold
```

Para encerrar sem remover o volume:

```bash
docker compose --env-file .env -f infrastructure/analytics/docker-compose.yml down
```
