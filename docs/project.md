# Sobre o projeto

## Problema

Fraude financeira combina volume, velocidade e necessidade de explicação. Não
basta calcular um score: é preciso receber eventos, preservar rastreabilidade,
validar dados, gerar sinais, avaliar resultados e permitir investigação.

Este projeto simula esse cenário com o PaySim, um dataset sintético de operações
financeiras móveis. Cada linha do CSV torna-se um evento e percorre uma
plataforma de dados completa.

## Escopo da demonstração

O arquivo PaySim usado localmente possui 6.362.620 transações. A execução
registrada nesta documentação publicou aproximadamente 27 mil eventos, cerca de
0,42% da base completa.

Esse recorte é deliberado. Em vez de carregar o CSV inteiro como um único batch,
o projeto publica lotes sucessivos e mantém checkpoint entre execuções. Isso
permite demonstrar os comportamentos que são centrais ao portfólio:

- entrada contínua de novos eventos;
- retomada a partir do último registro confirmado;
- consumo assíncrono por Bronze e Silver;
- persistência incremental e idempotente;
- orquestração e detecção automática de backlog;
- atualização das camadas analíticas e dos scores após cada lote.

O projeto demonstra conhecimento de arquitetura e operação streaming em escala
local. Ele não usa o recorte de 27 mil linhas para afirmar que o modelo possui
qualidade estatística sobre toda a população PaySim.

## Objetivo

Construir uma referência local e reproduzível que demonstre:

1. Ingestão de eventos com garantias práticas de entrega e retomada.
2. Processamento streaming sem funções Python linha a linha no Spark.
3. Organização de dados em Bronze, Silver e Gold.
4. Qualidade e idempotência em diferentes pontos do fluxo.
5. Regras explicáveis combinadas com detecção não supervisionada.
6. Orquestração, observabilidade e experiência de investigação.

## Perguntas respondidas

- Quantas transações e alertas foram processados?
- Quais regras geraram cada alerta?
- Quais contas concentram comportamento suspeito?
- Onde regras e Isolation Forest concordam ou divergem?
- O modelo identifica as fraudes rotuladas no corte de avaliação?
- Uma carga ficou parcial, duplicada ou falhou?
- É possível rastrear uma transação até Kafka e o arquivo de origem?

## Competências demonstradas

| Disciplina | Evidência no projeto |
| --- | --- |
| Engenharia de dados | Kafka, Spark, Parquet, MinIO e PostgreSQL |
| Analytics engineering | Projeto dbt em camadas, documentação e testes |
| Data quality | Schemas, validações, constraints e reconciliações |
| Machine Learning | Features temporais, split cronológico e avaliação |
| DataOps | Airflow, Docker Compose, checkpoints e auditoria |
| Data visualization | Grafana operacional e Streamlit investigativo |
| Engenharia de software | Tipagem, configuração, testes e separação de responsabilidades |

## Fora do escopo

- Decisão antifraude em produção com baixa latência.
- Treino distribuído ou serving online do modelo.
- Garantia exatamente uma vez entre todos os componentes.
- Segurança, TLS e gestão de segredos em nível produtivo.
- Escala horizontal real; o cluster Spark é local e deliberadamente pequeno.
