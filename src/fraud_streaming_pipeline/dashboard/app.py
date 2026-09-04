"""Dashboard para monitoramento e investigação de transações suspeitas."""

from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from fraud_streaming_pipeline.dashboard import data

# O layout largo acomoda comparações e tabelas sem esconder colunas importantes.
st.set_page_config(page_title="Fraud Analytics", page_icon="🛡️", layout="wide")


@st.cache_resource
def get_engine():
    """Mantém um único pool de conexões durante a sessão do Streamlit."""
    return data.create_database_engine()


@st.cache_data(ttl=30, show_spinner=False)
def load_filters() -> tuple[pd.Timestamp | None, pd.Timestamp | None, list[str]]:
    """Atualiza periodicamente os filtros conforme novos dados chegam à Gold."""
    engine = get_engine()
    minimum, maximum = data.available_dates(engine)
    return minimum, maximum, data.transaction_types(engine)


@st.cache_data(ttl=30, show_spinner=False)
def load_dashboard(start: str, end: str, transaction_type: str | None):
    """Carrega em conjunto os dados exibidos nas abas do dashboard."""
    engine = get_engine()
    return (
        data.overview(engine, start, end, transaction_type),
        data.daily_metrics(engine, start, end, transaction_type),
        data.recent_alerts(engine, start, end, transaction_type),
        data.pipeline_health(engine),
        data.benford(engine, start, end, transaction_type),
        data.anomaly_evaluation(engine),
        data.transaction_sample(engine, start, end, transaction_type),
        data.hourly_patterns(engine, start, end, transaction_type),
        data.rule_frequency(engine, start, end, transaction_type),
        data.anomaly_scores(engine, start, end, transaction_type),
        data.rules_vs_ml(engine, start, end, transaction_type),
        data.suspicious_accounts(engine, start, end, transaction_type),
        data.investigation_candidates(engine, start, end, transaction_type),
    )


@st.cache_data(ttl=30, show_spinner=False)
def load_investigation(event_id: str) -> pd.DataFrame:
    return data.transaction_investigation(get_engine(), event_id)


def money(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


st.title("🛡️ Fraud Analytics")
st.caption("Monitoramento das regras de risco e do modelo de detecção de anomalias")

try:
    minimum, maximum, types = load_filters()
except SQLAlchemyError as exc:
    st.error("Não foi possível conectar ao PostgreSQL Analytics.")
    st.code(str(exc).splitlines()[0])
    st.info(
        "Confirme as variáveis ANALYTICS_DB_* e se o banco e os modelos dbt estão ativos."
    )
    st.stop()

if minimum is None or maximum is None:
    st.warning(
        "Ainda não há transações na camada Gold. Execute a carga Silver e o dbt build."
    )
    st.stop()

with st.sidebar:
    st.header("Filtros")
    selected_dates = st.date_input(
        "Período",
        value=(minimum.date(), maximum.date()),
        min_value=minimum.date(),
        max_value=maximum.date(),
    )
    selected_type = st.selectbox("Tipo de transação", ["Todos", *types])
    if st.button("Atualizar dados", width="stretch"):
        st.cache_data.clear()
        st.rerun()

if isinstance(selected_dates, date):
    start_date = end_date = selected_dates
elif len(selected_dates) == 2:
    start_date, end_date = selected_dates
else:
    st.info("Selecione as datas inicial e final.")
    st.stop()

transaction_type = None if selected_type == "Todos" else selected_type

try:
    (
        summary,
        daily,
        alerts,
        health,
        digits,
        anomaly,
        sample,
        hourly,
        rules,
        scores,
        comparison,
        accounts,
        candidates,
    ) = load_dashboard(start_date.isoformat(), end_date.isoformat(), transaction_type)
except SQLAlchemyError as exc:
    st.error(
        "Não foi possível consultar os marts. Verifique se o dbt build foi concluído."
    )
    st.code(str(exc).splitlines()[0])
    st.stop()

row = summary.iloc[0]
columns = st.columns(5)
columns[0].metric("Transações", f"{int(row.transaction_count):,}".replace(",", "."))
columns[1].metric("Volume", money(float(row.total_amount)))
columns[2].metric("Alertas", f"{int(row.alert_count):,}".replace(",", "."))
columns[3].metric("Fraudes rotuladas", f"{int(row.fraud_count):,}".replace(",", "."))
columns[4].metric("Taxa de alerta", f"{float(row.alert_rate):.2%}")

(
    overview_tab,
    comparison_tab,
    investigation_tab,
    alerts_tab,
    diagnostics_tab,
    operations_tab,
) = st.tabs(
    [
        "Visão geral",
        "Regras × ML",
        "Investigação",
        "Alertas",
        "Diagnósticos",
        "Operação",
    ]
)

with overview_tab:
    if daily.empty:
        st.info("Nenhum dado encontrado para os filtros selecionados.")
    else:
        left, right = st.columns(2)
        left.plotly_chart(
            px.line(
                daily,
                x="event_date",
                y="transaction_count",
                markers=True,
                labels={"event_date": "Data", "transaction_count": "Transações"},
            ),
            width="stretch",
        )
        trend = daily.melt(
            id_vars="event_date",
            value_vars=["predicted_fraud_count", "labeled_fraud_count"],
            var_name="series",
            value_name="count",
        )
        right.plotly_chart(
            px.line(
                trend,
                x="event_date",
                y="count",
                color="series",
                markers=True,
                labels={
                    "event_date": "Data",
                    "count": "Ocorrências",
                    "series": "Série",
                },
            ),
            width="stretch",
        )

with comparison_tab:
    st.subheader("Concordância entre regras e Isolation Forest")
    st.caption(
        "Compara o modelo mais recente com o motor de regras para o período "
        "selecionado. Anomalia é um sinal investigativo, não uma confirmação de fraude."
    )
    if comparison.empty:
        st.info("Execute o Isolation Forest para comparar as duas estratégias.")
    else:
        numeric = ["transaction_count", "fraud_count", "total_amount", "fraud_amount"]
        comparison[numeric] = comparison[numeric].apply(pd.to_numeric)
        both = comparison.set_index("detection_group")

        def group_value(group: str, column: str) -> int:
            return 0 if group not in both.index else int(both.at[group, column])

        metrics = st.columns(4)
        metrics[0].metric(
            "Regras + ML",
            f"{group_value('Regras + ML', 'transaction_count'):,}".replace(",", "."),
        )
        metrics[1].metric(
            "Somente regras",
            f"{group_value('Somente regras', 'transaction_count'):,}".replace(",", "."),
        )
        metrics[2].metric(
            "Somente ML",
            f"{group_value('Somente ML', 'transaction_count'):,}".replace(",", "."),
        )
        metrics[3].metric(
            "Sem alerta",
            f"{group_value('Nenhum alerta', 'transaction_count'):,}".replace(",", "."),
        )

        left, right = st.columns(2)
        left.plotly_chart(
            px.bar(
                comparison,
                x="detection_group",
                y="transaction_count",
                color="detection_group",
                text_auto=True,
                title="Cobertura das estratégias",
                labels={
                    "detection_group": "Detecção",
                    "transaction_count": "Transações",
                },
            ),
            width="stretch",
        )
        right.plotly_chart(
            px.bar(
                comparison,
                x="detection_group",
                y="fraud_count",
                color="detection_group",
                text_auto=True,
                title="Fraudes reais em cada grupo",
                labels={
                    "detection_group": "Detecção",
                    "fraud_count": "Fraudes rotuladas",
                },
            ),
            width="stretch",
        )

        if not scores.empty:
            score_distribution = scores.copy()
            score_distribution["anomaly_score"] = pd.to_numeric(
                score_distribution["anomaly_score"]
            )
            score_distribution["classe"] = score_distribution["is_fraud"].map(
                {True: "Fraude real", False: "Legítima"}
            )
            st.plotly_chart(
                px.histogram(
                    score_distribution,
                    x="anomaly_score",
                    color="classe",
                    nbins=50,
                    barmode="overlay",
                    opacity=0.7,
                    title="Distribuição do score de anomalia",
                    labels={
                        "anomaly_score": "Score do Isolation Forest",
                        "classe": "Rótulo",
                    },
                ),
                width="stretch",
            )

        if not anomaly.empty:
            model = anomaly.iloc[0]
            st.subheader("Qualidade no conjunto de avaliação")
            quality = st.columns(6)
            quality[0].metric(
                "Precisão",
                "—" if pd.isna(model.precision) else f"{model.precision:.2%}",
            )
            quality[1].metric(
                "Recall", "—" if pd.isna(model.recall) else f"{model.recall:.2%}"
            )
            quality[2].metric(
                "F1", "—" if pd.isna(model.f1_score) else f"{model.f1_score:.2%}"
            )
            quality[3].metric("Verdadeiros positivos", int(model.true_positive))
            quality[4].metric("Falsos positivos", int(model.false_positive))
            quality[5].metric("Falsos negativos", int(model.false_negative))

with investigation_tab:
    st.subheader("Contas com maior atividade suspeita")
    if accounts.empty:
        st.info("Nenhuma conta suspeita encontrada para os filtros selecionados.")
    else:
        account_view = accounts.copy()
        for column in ("total_amount", "average_risk_score", "maximum_anomaly_score"):
            account_view[column] = pd.to_numeric(account_view[column])
        st.dataframe(
            account_view,
            width="stretch",
            hide_index=True,
            column_config={
                "origin_account": "Conta de origem",
                "transaction_count": "Transações",
                "rule_alert_count": "Alertas por regra",
                "anomaly_count": "Anomalias ML",
                "fraud_count": "Fraudes reais",
                "total_amount": st.column_config.NumberColumn(
                    "Volume", format="R$ %.2f"
                ),
                "average_risk_score": st.column_config.NumberColumn(
                    "Risco médio", format="%.1f"
                ),
                "maximum_anomaly_score": st.column_config.NumberColumn(
                    "Maior score ML", format="%.4f"
                ),
            },
        )

    st.subheader("Investigar uma transação")
    if candidates.empty:
        st.info("Nenhuma transação alertada ou rotulada encontrada no período.")
    else:
        candidate_rows = candidates.set_index("event_id")

        def candidate_label(event_id: str) -> str:
            candidate = candidate_rows.loc[event_id]
            labels = []
            if candidate["predicted_fraud"]:
                labels.append("Regra")
            if candidate["is_anomaly"]:
                labels.append("ML")
            if candidate["is_fraud"]:
                labels.append("Fraude real")
            return (
                f"{candidate['event_time']} | {candidate['transaction_type']} | "
                f"{money(float(candidate['amount']))} | {' + '.join(labels)}"
            )

        selected_event = st.selectbox(
            "Transação",
            candidate_rows.index.tolist(),
            format_func=candidate_label,
        )
        detail = load_investigation(selected_event)
        if detail.empty:
            st.warning("Não foi possível carregar os detalhes da transação.")
        else:
            item = detail.iloc[0]
            headline = st.columns(5)
            headline[0].metric("Valor", money(float(item.amount)))
            headline[1].metric("Risco das regras", f"{float(item.risk_score):.0f}/100")
            headline[2].metric("Score ML", f"{float(item.anomaly_score):.4f}")
            headline[3].metric(
                "Alerta por regra", "Sim" if item.predicted_fraud else "Não"
            )
            headline[4].metric("Anomalia ML", "Sim" if item.is_anomaly else "Não")

            left, right = st.columns(2)
            with left:
                st.markdown("#### Contexto da transação")
                st.write(f"**Evento:** `{item.event_id}`")
                st.write(f"**Horário:** {item.event_time}")
                st.write(f"**Tipo:** {item.transaction_type}")
                st.write(f"**Origem:** `{item.origin_account}`")
                st.write(f"**Destino:** `{item.destination_account}`")
                st.write(f"**Nível de risco:** {item.risk_level}")
                st.write(f"**Regras acionadas:** {item.triggered_rules or 'Nenhuma'}")
                st.write(f"**Fraude rotulada:** {'Sim' if item.is_fraud else 'Não'}")
            with right:
                st.markdown("#### Saldos e histórico")
                balance_table = pd.DataFrame(
                    {
                        "Conta": ["Origem", "Destino"],
                        "Saldo anterior": [
                            item.origin_old_balance,
                            item.destination_old_balance,
                        ],
                        "Saldo posterior": [
                            item.origin_new_balance,
                            item.destination_new_balance,
                        ],
                        "Variação": [
                            item.origin_balance_change,
                            item.destination_balance_change,
                        ],
                        "Anomalia de saldo": [
                            item.has_origin_balance_anomaly,
                            item.has_destination_balance_anomaly,
                        ],
                    }
                )
                st.dataframe(
                    balance_table,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "Saldo anterior": st.column_config.NumberColumn(
                            format="R$ %.2f"
                        ),
                        "Saldo posterior": st.column_config.NumberColumn(
                            format="R$ %.2f"
                        ),
                        "Variação": st.column_config.NumberColumn(format="R$ %.2f"),
                    },
                )
                st.write(
                    f"**Movimentações recentes da origem:** "
                    f"{int(item.recent_transaction_count or 0)}"
                )
                st.write(
                    f"**Volume recente da origem:** "
                    f"{money(float(item.recent_transaction_volume or 0))}"
                )
                zscore = (
                    "—"
                    if pd.isna(item.log_amount_zscore)
                    else f"{item.log_amount_zscore:.2f}"
                )
                robust = (
                    "—"
                    if pd.isna(item.log_amount_robust_zscore)
                    else f"{item.log_amount_robust_zscore:.2f}"
                )
                st.write(f"**Z-score histórico do valor:** {zscore}")
                st.write(f"**Z-score robusto do valor:** {robust}")

with alerts_tab:
    st.subheader("Alertas mais recentes")
    if alerts.empty:
        st.info("Nenhum alerta encontrado no período.")
    else:
        st.dataframe(
            alerts,
            width="stretch",
            hide_index=True,
            column_config={
                "event_time": st.column_config.DatetimeColumn(
                    "Horário", format="DD/MM/YYYY HH:mm:ss"
                ),
                "amount": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
                "risk_score": st.column_config.ProgressColumn(
                    "Risco", min_value=0, max_value=100
                ),
            },
        )

with diagnostics_tab:
    st.subheader("Exploração das transações")
    st.caption("Os gráficos usam até 5.000 transações recentes do período selecionado.")
    if sample.empty:
        st.info("Nenhuma transação encontrada para análise.")
    else:
        numeric_columns = ["amount", "risk_score", "origin_balance_difference"]
        sample[numeric_columns] = sample[numeric_columns].apply(pd.to_numeric)
        sample["classificacao"] = "Regular"
        sample.loc[sample["predicted_fraud"], "classificacao"] = "Alerta por regra"
        sample.loc[sample["is_fraud"], "classificacao"] = "Fraude rotulada"

        left, right = st.columns(2)
        left.plotly_chart(
            px.scatter(
                sample,
                x="amount",
                y="risk_score",
                color="classificacao",
                symbol="transaction_type",
                hover_data=["event_time", "transaction_type"],
                log_x=True,
                opacity=0.65,
                labels={
                    "amount": "Valor (escala log)",
                    "risk_score": "Score de risco",
                    "classificacao": "Classificação",
                    "transaction_type": "Tipo",
                },
                title="Valor × risco",
            ),
            width="stretch",
        )
        right.plotly_chart(
            px.box(
                sample,
                x="transaction_type",
                y="amount",
                color="classificacao",
                log_y=True,
                points="outliers",
                labels={
                    "amount": "Valor (escala log)",
                    "transaction_type": "Tipo",
                    "classificacao": "Classificação",
                },
                title="Distribuição e valores extremos",
            ),
            width="stretch",
        )

        balance = sample.assign(
            anomalia_saldo=sample["has_origin_balance_anomaly"].map(
                {True: "Origem", False: "Sem anomalia na origem"}
            )
        )
        balance = (
            balance.groupby(["transaction_type", "anomalia_saldo"], as_index=False)
            .size()
            .rename(columns={"size": "count"})
        )
        left, right = st.columns(2)
        left.plotly_chart(
            px.bar(
                balance,
                x="transaction_type",
                y="count",
                color="anomalia_saldo",
                barmode="group",
                title="Anomalias no saldo de origem",
                labels={
                    "transaction_type": "Tipo",
                    "count": "Transações",
                    "anomalia_saldo": "Diagnóstico",
                },
            ),
            width="stretch",
        )
        if rules.empty:
            right.info("Nenhuma regra de risco foi disparada no período.")
        else:
            right.plotly_chart(
                px.bar(
                    rules.sort_values("trigger_count"),
                    x="trigger_count",
                    y="rule",
                    orientation="h",
                    title="Regras mais acionadas",
                    labels={"trigger_count": "Acionamentos", "rule": "Regra"},
                ),
                width="stretch",
            )

    st.subheader("Padrões temporais")
    if hourly.empty:
        st.info("Sem dados horários no período.")
    else:
        hourly["alert_rate"] = pd.to_numeric(hourly["alert_rate"])
        heatmap = hourly.pivot(
            index="transaction_type", columns="event_hour", values="alert_rate"
        ).fillna(0)
        st.plotly_chart(
            px.imshow(
                heatmap,
                aspect="auto",
                color_continuous_scale="YlOrRd",
                labels={"x": "Hora do evento", "y": "Tipo", "color": "Taxa de alerta"},
                title="Concentração de alertas por hora e tipo",
                text_auto=".1%",
            ),
            width="stretch",
        )

    st.subheader("Scores do Isolation Forest")
    if scores.empty:
        st.info("Execute o Isolation Forest para visualizar os scores individuais.")
    else:
        scores[["amount", "anomaly_score", "risk_score"]] = scores[
            ["amount", "anomaly_score", "risk_score"]
        ].apply(pd.to_numeric)
        scores["resultado"] = scores["is_anomaly"].map(
            {True: "Anomalia", False: "Normal"}
        )
        st.caption(f"Modelo: `{scores.iloc[0].model_version}`")
        left, right = st.columns(2)
        left.plotly_chart(
            px.scatter(
                scores,
                x="event_time",
                y="anomaly_score",
                color="resultado",
                hover_data=["amount", "transaction_type", "risk_score"],
                title="Score de anomalia ao longo do tempo",
                labels={
                    "event_time": "Horário",
                    "anomaly_score": "Score",
                    "resultado": "Resultado",
                },
            ),
            width="stretch",
        )
        right.plotly_chart(
            px.scatter(
                scores,
                x="amount",
                y="anomaly_score",
                color="resultado",
                symbol="is_fraud",
                log_x=True,
                hover_data=["event_time", "transaction_type"],
                title="Valor × score do modelo",
                labels={
                    "amount": "Valor (escala log)",
                    "anomaly_score": "Score",
                    "resultado": "Resultado",
                    "is_fraud": "Fraude rotulada",
                },
            ),
            width="stretch",
        )

    st.divider()
    left, right = st.columns(2)
    with left:
        st.subheader("Lei de Benford")
        st.caption("Desvio populacional; isoladamente, não representa fraude.")
        if digits.empty:
            st.info("Amostra insuficiente para o diagnóstico.")
        else:
            figure = go.Figure()
            figure.add_bar(
                x=digits.first_digit, y=digits.observed_proportion, name="Observado"
            )
            figure.add_scatter(
                x=digits.first_digit,
                y=digits.expected_proportion,
                name="Esperado",
                mode="lines+markers",
            )
            figure.update_layout(xaxis_title="Primeiro dígito", yaxis_tickformat=".1%")
            st.plotly_chart(figure, width="stretch")
    with right:
        st.subheader("Isolation Forest")
        st.caption(
            "Avaliação do modelo mais recente; anomalia não equivale necessariamente a fraude."
        )
        if anomaly.empty:
            st.info("Nenhuma execução de modelo disponível.")
        else:
            model = anomaly.iloc[0]
            st.write(f"Modelo: `{model.model_version}`")
            a, b, c = st.columns(3)
            a.metric(
                "Precisão",
                "—" if pd.isna(model.precision) else f"{model.precision:.2%}",
            )
            b.metric("Recall", "—" if pd.isna(model.recall) else f"{model.recall:.2%}")
            c.metric("F1", "—" if pd.isna(model.f1_score) else f"{model.f1_score:.2%}")
            matrix = pd.DataFrame(
                [
                    [model.true_positive, model.false_negative],
                    [model.false_positive, model.true_negative],
                ],
                index=["Fraude", "Legítima"],
                columns=["Anomalia", "Normal"],
            )
            st.dataframe(matrix, width="stretch")

with operations_tab:
    st.subheader("Saúde das cargas")
    if health.empty:
        st.info("Nenhuma execução de carga registrada.")
    else:
        st.dataframe(health, width="stretch", hide_index=True)
