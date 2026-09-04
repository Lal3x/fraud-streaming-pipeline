# Decisões técnicas

## Kafka

Separa produção e processamento, permite replay e representa o padrão de eventos
de plataformas financeiras. Um broker e três partições atendem ao ambiente local.

## Spark Structured Streaming

Unifica APIs batch e streaming e oferece checkpoints, watermark e operadores
stateful. A Silver usa funções nativas, evitando o custo de UDFs Python.

## MinIO e Parquet

MinIO oferece uma API compatível com S3 localmente. Parquet reduz armazenamento,
preserva schema e permite leitura colunar, mas não fornece transações ACID.

## PostgreSQL e dbt

PostgreSQL é leve e suficiente para o volume local. dbt torna linhagem,
materialização e qualidade do modelo analítico explícitas em SQL.

## Regras e Isolation Forest

Regras oferecem explicabilidade imediata. Isolation Forest demonstra detecção
não supervisionada. Persistir ambos permite medir concordância e discordância.

## Split temporal e prevenção de leakage

Uma divisão aleatória pode permitir que padrões futuros influenciem o treino. O
corte cronológico aproxima o uso real. `is_fraud` só existe por ser um dataset
rotulado e é proibido nas features por testes dbt.

## Idempotência em camadas

Não existe promessa exatamente uma vez ponta a ponta. O projeto combina:

- producer Kafka idempotente;
- checkpoint após confirmação;
- `event_id` determinístico;
- deduplicação Silver com watermark;
- manifesto de Parquets carregados;
- PK e `ON CONFLICT` no PostgreSQL;
- chaves únicas nos modelos incrementais dbt.

## Limites locais

O worker Spark possui 1 GiB e executa Bronze e Silver com 512 MiB cada. Jobs
finitos têm limites próprios. O trade-off é menor throughput em troca de uma
demonstração segura em notebook ou WSL2.

## Evolução para produção

Uma evolução consideraria Kafka gerenciado, Spark/Kubernetes, Iceberg ou Delta
Lake, object storage real, secrets manager, Prometheus, tracing, CI/CD e
infraestrutura como código.
