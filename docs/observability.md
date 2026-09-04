# Visualização e observabilidade

## Grafana

O dashboard Grafana apresenta volume, alertas, fraudes rotuladas, padrões
temporais e saúde das cargas. O intervalo padrão inclui os timestamps simulados
do PaySim, iniciados em 2023.

Acesse [http://localhost:3000](http://localhost:3000).

## Streamlit

O Streamlit oferece uma interface mais exploratória:

- KPIs e evolução diária.
- Alertas recentes e regras acionadas.
- Comparação entre regras e Isolation Forest.
- Distribuição dos scores do modelo.
- Precisão, recall, F1 e matriz de confusão.
- Ranking de contas suspeitas.
- Investigação detalhada por transação.
- Diagnósticos de saldo e Lei de Benford.

Acesse [http://localhost:8501](http://localhost:8501).

## Atualização

O Grafana atualiza automaticamente. No Streamlit, use **Atualizar dados** para
limpar o cache após uma nova execução da DAG.
