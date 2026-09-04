# Resultados e aprendizados

## Retrato de uma execução

Métricas observadas em uma execução local em 2 de setembro de 2026:

| Métrica | Valor |
| --- | ---: |
| Transações processadas | 27.034 |
| Volume financeiro sintético | R$ 3.637.121.008,08 |
| Fraudes rotuladas | 82 |
| Alertas pelas regras | 7.797 |
| Anomalias do modelo mais recente | 276 |
| Treino / avaliação | 21.628 / 5.406 |

## Tamanho do recorte

| Escopo | Transações | Proporção aproximada |
| --- | ---: | ---: |
| PaySim completo disponível localmente | 6.362.620 | 100% |
| Eventos publicados no checkpoint documentado | 27.046 | 0,42% |
| Transações válidas materializadas na execução | 27.034 | 0,42% |

A execução usa apenas uma fração da base porque seu objetivo é exercitar o
caminho streaming em lotes controlados em uma máquina local. O producer pode ser
executado repetidamente e continua do checkpoint, sem reler para a memória ou
republicar os registros já confirmados.

Consequentemente, as métricas de ML desta página são um baseline inicial e não
devem ser generalizadas para os 6,3 milhões de registros do PaySim.

O `event_time` vai de `2023-01-01 00:00 UTC` a `2023-01-01 07:00 UTC`, pois o
PaySim usa um relógio simulado independente do horário de processamento.

!!! note "Como interpretar"
    Estes números demonstram funcionamento e rastreabilidade, não desempenho
    produtivo. Eles mudam com o checkpoint e com os lotes executados.

## Resultado do Isolation Forest

No corte de avaliação mais recente:

| Métrica | Valor |
| --- | ---: |
| Verdadeiros positivos | 0 |
| Falsos positivos | 64 |
| Falsos negativos | 3 |
| Verdadeiros negativos | 5.139 |
| Precisão | 0% |
| Recall | 0% |
| F1 | 0% |

Esse resultado é tecnicamente importante: o modelo encontra pontos raros, mas
as anomalias desse recorte não coincidem com as fraudes rotuladas. Um sistema
responsável não deve apresentar “anomalia” como sinônimo de “fraude”.

O recall zero não é apresentado como conclusão definitiva sobre o Isolation
Forest. O conjunto de avaliação contém somente três fraudes, quantidade
insuficiente para uma estimativa estável. Para avaliar o modelo de forma mais
representativa, o próximo experimento deve publicar uma janela maior da base e
garantir volume adequado de casos positivos em cada corte temporal.

## O que foi aprendido

1. **Avaliar é parte do pipeline.** Sem persistir split e versão, seria fácil
   mostrar anomalias visualmente interessantes e ignorar a qualidade.
2. **Regras e ML têm papéis diferentes.** Regras são explicáveis; o modelo pode
   encontrar padrões não previstos, mas precisa demonstrar ganho mensurável.
3. **Desbalanceamento importa.** Há poucas fraudes e o corte temporal pode
   concentrá-las de forma diferente entre treino e avaliação.
4. **Features temporais precisam de histórico.** Muitas contas aparecem poucas
   vezes, deixando estatísticas históricas nulas ou pouco informativas.
5. **Operação também afeta analytics.** Um stream sem executor fez a DAG avançar
   com dados parciais; o projeto ganhou uma validação automática por causa disso.

## Próximos experimentos de ML

- Avaliar Precision@K e Recall@K em vez de somente o corte de contaminação.
- Ajustar threshold usando apenas um conjunto de validação temporal.
- Criar features de destinatários distintos, velocidade e redes de contas.
- Comparar Isolation Forest, Local Outlier Factor e modelos supervisionados.
- Usar backtesting em múltiplas janelas temporais.
- Monitorar drift de features e taxa de alertas por versão.
