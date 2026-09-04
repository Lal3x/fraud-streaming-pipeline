# Desenvolvimento

## Estrutura do repositório

```text
fraud-streaming-pipeline/
├── src/fraud_streaming_pipeline/
│   ├── dashboard/       # Streamlit e consultas analíticas
│   ├── etl/
│   │   ├── bronze/      # Kafka → Bronze
│   │   ├── silver/      # validação, enriquecimento e regras
│   │   └── loading/     # Parquet → PostgreSQL
│   ├── ml/              # treino, score e auditoria do modelo
│   ├── models/          # contrato do evento
│   ├── publishers/      # Kafka e console
│   ├── sources/         # leitura incremental PaySim
│   └── state/           # checkpoint do producer
├── dbt/fraud_analytics/ # staging, intermediate, marts e testes
├── infrastructure/      # Compose, imagens, DAG e provisionamento
├── tests/               # testes unitários e Spark
├── docs/                # documentação MkDocs
└── mkdocs.yml
```

## Ambiente Python

```bash
poetry install --with dev
```

O projeto requer Python 3.12. Spark e os jobs de infraestrutura também possuem
imagens Docker próprias para manter o ambiente reproduzível.

## Ciclo de validação

```bash
poetry run ruff check src tests infrastructure/airflow/dags
poetry run pytest tests/unit -q
poetry run pytest tests/spark -q
poetry run mkdocs build --strict
```

## Testar o producer sem Kafka

```bash
poetry run python -m fraud_streaming_pipeline.main \
  --publisher console --limit 5 --events-per-second 10
```

O modo console preserva leitura, validação e checkpoint, mas imprime os eventos
em vez de enviá-los ao broker.

## Convenções

- Configuração vem do ambiente; segredos não ficam no código.
- Transformações Spark preferem funções nativas a UDFs Python.
- Consultas do dashboard são agregadas ou limitadas.
- Alterações de schema devem incluir migração, testes e documentação.
- Checkpoints stateful não devem ser removidos sem entender o impacto de replay.

## Gerar documentação

```bash
poetry run mkdocs serve
poetry run mkdocs build --strict
```

O diretório `site/` é gerado e ignorado pelo Git.
