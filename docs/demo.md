# Roteiro de demonstração

Este roteiro ajuda a apresentar o projeto em entrevista ou vídeo sem precisar
explicar todos os detalhes de uma vez.

## 1. Comece pelo problema

Explique que o objetivo não é apenas treinar um modelo. O desafio é transportar
eventos com rastreabilidade, garantir qualidade, avaliar sinais de fraude e
oferecer uma interface de investigação.

## 2. Mostre a DAG

Abra o Airflow em [http://localhost:8083](http://localhost:8083) e apresente:

```text
producer → Bronze → Silver → PostgreSQL → dbt → ML → testes
```

Destaque o parâmetro `producer_limit` e a validação que impede o loader de
avançar com uma carga parcial.

## 3. Mostre o streaming

No Kafka UI, abra o tópico `fraud-transactions-raw` e mostre as três partições.
No Spark Master, mostre Bronze e Silver compartilhando o worker com limites de
recursos. No MinIO, mostre os prefixos:

```text
bronze/paysim/events
silver/transactions
silver/invalid-events
silver/fraud-alerts
```

## 4. Mostre o warehouse

Explique a divisão entre schemas `silver`, `staging`, `intermediate`,
`analytics` e `monitoring`. Destaque que o dbt possui testes de reconciliação e
testes que proíbem labels nas features do ML.

## 5. Mostre o Streamlit

Sugestão de sequência:

1. KPIs da visão geral.
2. Aba **Regras × ML**.
3. Métricas honestas do modelo.
4. Ranking de contas suspeitas.
5. Investigação de uma transação e suas regras acionadas.

## 6. Mostre observabilidade

No Grafana, explique a diferença entre `event_time` simulado e horário real de
processamento. Mostre volume, alertas, fraudes e saúde das cargas.

## 7. Termine com os trade-offs

Uma boa apresentação reconhece limites:

- O modelo não teve recall no corte atual.
- Parquet não oferece ACID no lake.
- O cluster é local, não um benchmark de escala.
- Exactly-once é aproximado por mecanismos idempotentes em camadas.

Feche mostrando como cada limitação gera um próximo passo concreto.

## Pitch de 30 segundos

> Construí um pipeline antifraude ponta a ponta usando Kafka e Spark Streaming,
> com data lake em MinIO, warehouse PostgreSQL modelado por dbt, regras
> explicáveis e Isolation Forest. O Airflow orquestra o fluxo e impede cargas
> parciais; Grafana monitora a operação e Streamlit permite comparar regras, ML
> e fraude real e investigar cada evento. O projeto roda localmente com limites
> de memória e possui qualidade aplicada em todas as camadas.
