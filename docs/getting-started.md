# Executar localmente

## Pré-requisitos

- Docker com Docker Compose.
- Python 3.12 e Poetry para desenvolvimento e testes.
- Aproximadamente 8 GiB disponíveis para o WSL/Docker.

Copie e revise as configurações:

```bash
cp .env.example .env
```

## 1. Streaming

```bash
docker compose --env-file .env \
  -f infrastructure/streaming/docker-compose.yml up -d
```

## 2. PostgreSQL analytics e imagens dos jobs

```bash
docker compose --env-file .env \
  -f infrastructure/analytics/docker-compose.yml up -d postgres

docker compose --env-file .env \
  -f infrastructure/analytics/docker-compose.yml \
  --profile loader --profile gold --profile ml build \
  silver-loader dbt-gold isolation-forest
```

## 3. Airflow

```bash
docker compose --env-file .env \
  -f infrastructure/airflow/docker-compose.yml up airflow-init

docker compose --env-file .env \
  -f infrastructure/airflow/docker-compose.yml up -d
```

Acesse [http://localhost:8083](http://localhost:8083) e execute a DAG
`fraud_bronze_to_gold`. O formulário vem configurado para 10 mil eventos.

## 4. Interfaces

```bash
docker compose --env-file .env \
  -f infrastructure/grafana/docker-compose.yml up -d

docker compose --env-file .env \
  -f infrastructure/streamlit/docker-compose.yml up -d
```

- Grafana: [http://localhost:3000](http://localhost:3000)
- Streamlit: [http://localhost:8501](http://localhost:8501)

## Acompanhar recursos

```bash
docker stats
```

Os serviços possuem limites de memória. Bronze e Silver usam um core e 512 MiB
por executor para poderem compartilhar o worker Spark local.
