# Qualidade e testes

A qualidade é aplicada em várias camadas, pois uma única validação no início não
protege contra falhas de transformação, carga ou modelagem.

## Camadas de proteção

| Ponto | Mecanismo |
| --- | --- |
| Producer | Pydantic, tipos, limites e padrões de conta |
| Kafka | Confirmação de entrega e producer idempotente |
| Silver | Schema Spark, validações acumuladas e saída de inválidos |
| Streaming | Watermark, deduplicação e checkpoints separados |
| PostgreSQL | PKs, FKs, `CHECK`, staging e transação |
| Loader | Manifesto de arquivos, reconciliação e lote mínimo esperado |
| dbt | 41 testes de domínio, unicidade, relacionamento e reconciliação |
| ML | Proibição de labels nas features e split temporal |

## Testes Python

Os testes unitários cobrem checkpoint e retomada, publicação no console e
Kafka, leitura incremental do PaySim, configurações, consultas analíticas,
carga PostgreSQL e ciclo de execução do Isolation Forest. Os testes Spark
cobrem parsing, validação, deduplicação, enriquecimentos e regras Silver.

A cobertura mínima obrigatória é 70%. A medição não inclui a composição visual
do Streamlit nem os processos contínuos Bronze/Silver; esses pontos de entrada
dependem do ambiente integrado. As consultas, transformações, validações e
regras de negócio utilizadas por eles continuam dentro da métrica.

```bash
poetry run pytest tests/unit -q
poetry run pytest tests/spark -q
poetry run task tests
```

Na validação local atual, 52 testes unitários passam e atingem 71,84%. O CI
instala Java 17 e também executa os 16 testes Spark.

## Qualidade estática

```bash
poetry run ruff check src tests infrastructure/airflow/dags
poetry run mypy src
```

## Testes dbt

Os testes dbt são executados pelo `dbt build` da DAG. Entre as verificações estão:

- IDs únicos e não nulos.
- Valores monetários não negativos.
- Tipos, scores e níveis dentro do domínio.
- Relacionamentos entre fatos, dimensões e fontes.
- Reconciliação Silver × Gold e alertas × transações.
- Ausência de `is_fraud`, `is_flagged_fraud` e `predicted_fraud` nas features.

## Falha segura na DAG

Se a DAG publica 10 mil eventos e o loader encontra menos de 10 mil novos
registros, `load_postgres` falha antes de marcar arquivos. O Airflow repete a
tarefa quando o Silver terminar, impedindo Gold e ML com dados parciais.
