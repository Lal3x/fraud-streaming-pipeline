# Configuração

As configurações locais ficam no `.env`, criado a partir de `.env.example`.
Credenciais reais não devem ser versionadas.

## Producer e Kafka

| Variável | Função |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Endereço do broker |
| `KAFKA_TOPIC` | Tópico de eventos brutos |
| `PAYSIM_FILE` | Caminho do CSV |
| `PRODUCER_CHECKPOINT` | Posição confirmada do producer |
| `PAYSIM_SIMULATION_START` | Início do relógio simulado |

## Silver

| Variável | Padrão | Função |
| --- | ---: | --- |
| `SILVER_WATERMARK` | `24 hours` | Limite do estado de deduplicação |
| `SILVER_TRIGGER_INTERVAL` | `10 seconds` | Intervalo de microbatch |
| `SILVER_HIGH_AMOUNT_THRESHOLD` | `100000` | Corte de valor alto |
| `SILVER_ALERT_SCORE` | `50` | Score mínimo para alerta |
| `SILVER_MEDIUM_RISK_SCORE` | `30` | Início do risco médio |
| `SILVER_HIGH_RISK_SCORE` | `70` | Início do risco alto |

Os pesos das regras também são configuráveis. A soma é limitada a 100.

## Analytics

`ANALYTICS_DB_HOST`, `ANALYTICS_DB_PORT`, `ANALYTICS_DB_NAME`,
`ANALYTICS_DB_USER` e `ANALYTICS_DB_PASSWORD` configuram loader, dbt, ML,
Grafana e Streamlit.

## Machine Learning

| Variável | Padrão | Impacto |
| --- | ---: | --- |
| `ML_TRAIN_FRACTION` | `0.8` | Fração cronológica de treino |
| `ML_CONTAMINATION` | `0.01` | Proporção esperada de anomalias |
| `ML_MAX_ROWS` | `100000` | Teto carregado em memória |
| `ML_N_ESTIMATORS` | `100` | Quantidade de árvores |
| `ML_N_JOBS` | `2` | Paralelismo do treino |
| `ML_PERSIST_BATCH_SIZE` | `5000` | Lote de escrita dos scores |

## Portas locais

| Serviço | Porta |
| --- | ---: |
| Kafka | 9092 |
| Kafka UI | 8082 |
| Spark Master | 8080 |
| Spark Worker | 8081 |
| MinIO API / Console | 9000 / 9001 |
| PostgreSQL analytics | 5434 |
| Airflow | 8083 |
| Grafana | 3000 |
| Streamlit | 8501 |
