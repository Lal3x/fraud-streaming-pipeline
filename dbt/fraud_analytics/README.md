# Camada Gold com dbt

O projeto transforma as tabelas PostgreSQL dos schemas `silver` e `monitoring`
em views de staging/intermediate e marts no schema `analytics`.

## Configuração e execução

Use o ambiente Poetry já existente em `dbt/`:

```bash
cp dbt/fraud_analytics/profiles.yml.example dbt/fraud_analytics/profiles.yml
export ANALYTICS_DB_HOST=localhost
export ANALYTICS_DB_PORT=5434
export ANALYTICS_DB_NAME=fraud_analytics
export ANALYTICS_DB_USER=analytics
export ANALYTICS_DB_PASSWORD='sua-senha-local'
cd dbt
poetry install
poetry run dbt debug --project-dir fraud_analytics --profiles-dir fraud_analytics
poetry run dbt build --project-dir fraud_analytics --profiles-dir fraud_analytics
poetry run dbt test --project-dir fraud_analytics --profiles-dir fraud_analytics
poetry run dbt docs generate --project-dir fraud_analytics --profiles-dir fraud_analytics
poetry run dbt docs serve --project-dir fraud_analytics --profiles-dir fraud_analytics
```

## Organização

- `staging`: padronização e tipos, sem regras complexas;
- `intermediate`: classificações e agregações reutilizáveis;
- `analytics`: fatos, dimensão, marts operacionais, Benford e features de ML.

`fct_transactions` e `fct_fraud_alerts` são incrementais por `event_id`. Os
testes verificam unicidade, relacionamentos, domínios, intervalos, valores
monetários e reconciliação Silver/Gold.

## Benford e ML

`mart_benford_first_digit` considera somente valores positivos e populações com
pelo menos 100 registros por dia/tipo (configurável). A Lei de Benford é um
diagnóstico populacional, não uma classificação individual de fraude. Como o
PaySim é sintético, sua distribuição pode não seguir Benford naturalmente.

`ml_transaction_features` não contém `is_fraud`, `is_flagged_fraud` nem
`predicted_fraud`. Médias e z-scores usam somente eventos anteriores da conta;
amostras pequenas e desvio/MAD zero produzem valores nulos. O rótulo continua em
`fct_transactions` exclusivamente para avaliação posterior.

O Isolation Forest é executado por um job Python separado. Ele usa divisão
temporal, ajusta transformações somente no treino e grava `anomaly_score`,
`is_anomaly`, `model_version` e `scored_at` em tabela própria. O dbt apenas cria
`mart_anomaly_evaluation`, que compara o período de avaliação com `is_fraud`
depois da pontuação. O treinamento não pertence ao dbt nem ao Streamlit.

## Dashboard planejado

A exposição `future_streamlit_fraud_dashboard` documenta uma página futura com
KPIs, filtros, distribuição de rótulos, Benford observado/esperado, dispersão de
valor versus saldo, maiores anomalias, comparação entre regras/modelo/rótulo e
precision, recall, F1 e matriz de confusão. Os gráficos interativos deverão usar
Plotly e informar que anomalia não implica fraude.
