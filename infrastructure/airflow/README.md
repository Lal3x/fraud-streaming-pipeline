# Airflow

Ambiente local do Apache Airflow 3.3.1 com `LocalExecutor`, API Server, DAG
Processor, Triggerer e PostgreSQL dedicado para os metadados. Os DAGs devem ser
adicionados em `dags/`.

O ambiente local usa o provider Docker e o socket `/var/run/docker.sock` para
executar os jobs isolados. Ajuste `DOCKER_GID` no `.env` com o resultado de:

```bash
stat -c '%g' /var/run/docker.sock
```

## Inicialização

Revise as credenciais do arquivo `.env` na raiz do projeto e execute, também
pela raiz:

```bash
docker compose --env-file .env -f infrastructure/airflow/docker-compose.yml up airflow-init
docker compose --env-file .env -f infrastructure/airflow/docker-compose.yml up -d
```

A interface fica disponível em <http://localhost:8083>. Por padrão, o usuário e
a senha são `airflow`.

O PostgreSQL é exposto em `localhost:5433`, com banco e usuário `airflow`. Os
dados são mantidos no volume Docker `fraud-airflow-postgres-data`.

## DAG Bronze → Gold

A DAG `fraud_bronze_to_gold` executa uma carga parametrizada nesta ordem:

```text
start
  → run_producer
  → ingest_bronze
  → transform_silver
  → load_postgres
  → dbt_build_gold
  → run_isolation_forest
  → validate_gold
  → finish
```

Antes da primeira execução, suba o streaming e o banco e construa as imagens
efêmeras usadas pela DAG:

```bash
docker compose --env-file .env \
  -f infrastructure/streaming/docker-compose.yml up -d

docker compose --env-file .env \
  -f infrastructure/analytics/docker-compose.yml up -d postgres

docker compose --env-file .env \
  -f infrastructure/analytics/docker-compose.yml \
  --profile loader --profile gold --profile ml build \
  silver-loader dbt-gold isolation-forest
```

Ao disparar a DAG pela interface, o formulário permite configurar:

- `producer_limit`: quantidade de eventos PaySim publicada no Kafka;
- `events_per_second`: limite de publicação do producer;
- `stream_timeout_seconds`: limite para os streams ficarem disponíveis;
- `bronze_settle_seconds`: espera para a Bronze concluir seus micro-batches;
- `silver_settle_seconds`: espera para a Silver concluir seus micro-batches;
- `dbt_select`: seleção executada pelo `dbt build`;
- `full_refresh`: reconstrói os modelos incrementais.

O formulário já vem ajustado para uma execução local de 10 mil eventos:

```text
producer_limit: 10000
events_per_second: 100
bronze_settle_seconds: 20
silver_settle_seconds: 120
full_refresh: false
```

Os jobs efêmeros criados pela DAG também possuem limites de memória: 1200 MB
para o loader Spark, 1200 MB para o Isolation Forest e 768 MB para cada
execução dbt.

O loader recebe `producer_limit` como quantidade mínima esperada. Se o Silver
ainda não tiver produzido o lote completo, a tarefa falha antes de marcar
arquivos como processados e o retry do Airflow tenta novamente com segurança.

Bronze e Silver permanecem como serviços contínuos. As tarefas `ingest_bronze`
e `transform_silver` verificam o respectivo container e aguardam a conclusão dos
micro-batches gerados pelo lote. Depois, a carga PostgreSQL e o dbt são
executados como jobs finitos. Após a materialização Gold, o Isolation Forest
calcula e persiste os scores de anomalia. `validate_gold` roda os testes dbt
separadamente.

Para acompanhar os serviços:

```bash
docker compose --env-file .env -f infrastructure/airflow/docker-compose.yml ps
docker compose --env-file .env -f infrastructure/airflow/docker-compose.yml logs -f airflow-api-server airflow-scheduler airflow-dag-processor
```

Para encerrar sem apagar o banco:

```bash
docker compose --env-file .env -f infrastructure/airflow/docker-compose.yml down
```
