# Machine Learning

O projeto usa `IsolationForest` para complementar as regras de risco. O modelo
não utiliza `is_fraud`, `is_flagged_fraud` ou `predicted_fraud` como features.

## Fluxo

1. dbt cria `analytics.ml_transaction_features`.
2. O job seleciona uma janela limitada e ordenada temporalmente.
3. Os primeiros 80% formam o treino e os últimos 20% a avaliação.
4. Scores e previsões são gravados em `analytics.transaction_anomaly_scores`.
5. A execução é registrada em `monitoring.ml_model_runs`.

## Controles de recursos

| Variável | Padrão |
| --- | ---: |
| `ML_MAX_ROWS` | 100000 |
| `ML_N_ESTIMATORS` | 100 |
| `ML_N_JOBS` | 2 |
| `ML_PERSIST_BATCH_SIZE` | 5000 |
| `ML_CONTAMINATION` | 0.01 |

Os limites evitam carregar uma tabela ilimitada na memória e controlam o
paralelismo durante o treino.

## Interpretação

Anomalia não equivale automaticamente a fraude. O Streamlit compara os sinais
do modelo com regras e rótulos reais, apresenta precisão, recall, F1 e permite
investigar transações individuais.
