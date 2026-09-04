"""Consultas SQL parametrizadas usadas pela interface Streamlit."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

import pandas as pd
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine


def database_url(env: Mapping[str, str] | None = None) -> URL:
    """Monta a URL do PostgreSQL sem concatenar manualmente as credenciais."""
    values = os.environ if env is None else env
    return URL.create(
        "postgresql+psycopg",
        username=values.get("ANALYTICS_DB_USER", "analytics"),
        password=values.get("ANALYTICS_DB_PASSWORD", "change-me"),
        host=values.get("ANALYTICS_DB_HOST", "localhost"),
        port=int(values.get("ANALYTICS_DB_PORT", "5434")),
        database=values.get("ANALYTICS_DB_NAME", "fraud_analytics"),
    )


def create_database_engine() -> Engine:
    """Cria o pool de conexões reutilizado pelas consultas do dashboard."""
    return create_engine(database_url(), pool_pre_ping=True, pool_recycle=300)


def read_frame(
    engine: Engine,
    query: str,
    params: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Executa uma consulta parametrizada e devolve um DataFrame."""
    with engine.connect() as connection:
        return pd.read_sql_query(text(query), connection, params=dict(params or {}))


def available_dates(engine: Engine) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """Retorna o intervalo temporal existente na camada Gold."""
    frame = read_frame(
        engine,
        "select min(event_date) as min_date, max(event_date) as max_date "
        "from analytics.fct_transactions",
    )
    if frame.empty or pd.isna(frame.at[0, "min_date"]):
        return None, None
    return pd.Timestamp(frame.at[0, "min_date"]), pd.Timestamp(frame.at[0, "max_date"])


def transaction_types(engine: Engine) -> list[str]:
    frame = read_frame(
        engine,
        "select distinct transaction_type from analytics.fct_transactions order by 1",
    )
    return frame["transaction_type"].tolist()


def overview(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        select
            count(*) as transaction_count,
            coalesce(sum(amount), 0) as total_amount,
            count(*) filter (where predicted_fraud) as alert_count,
            count(*) filter (where is_fraud) as fraud_count,
            coalesce(avg(risk_score), 0) as average_risk_score,
            coalesce(
                count(*) filter (where predicted_fraud)::numeric / nullif(count(*), 0), 0
            ) as alert_rate
        from analytics.fct_transactions
        where event_date between :start_date and :end_date
          and (cast(:transaction_type as text) is null
               or transaction_type = cast(:transaction_type as text))
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def daily_metrics(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        select
            event_date,
            sum(transaction_count) as transaction_count,
            sum(total_amount) as total_amount,
            sum(predicted_fraud_count) as predicted_fraud_count,
            sum(labeled_fraud_count) as labeled_fraud_count,
            sum(predicted_fraud_count)::numeric / nullif(sum(transaction_count), 0) as alert_rate
        from analytics.mart_fraud_daily
        where event_date between :start_date and :end_date
          and (cast(:transaction_type as text) is null
               or transaction_type = cast(:transaction_type as text))
        group by event_date
        order by event_date
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def recent_alerts(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        select event_time, transaction_type, amount, origin_account,
               destination_account, risk_score, risk_level, triggered_rules,
               rule_evaluation_class
        from analytics.fct_fraud_alerts
        where event_time >= cast(:start_date as date)
          and event_time < (cast(:end_date as date) + interval '1 day')
          and (cast(:transaction_type as text) is null
               or transaction_type = cast(:transaction_type as text))
        order by event_time desc
        limit 200
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def pipeline_health(engine: Engine) -> pd.DataFrame:
    return read_frame(
        engine,
        "select * from analytics.mart_pipeline_health order by execution_date desc limit 30",
    )


def benford(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        select first_digit,
               sum(observed_count) as observed_count,
               sum(observed_count)::numeric / nullif(sum(sum(observed_count)) over (), 0)
                   as observed_proportion,
               max(expected_proportion) as expected_proportion
        from analytics.mart_benford_first_digit
        where event_date between :start_date and :end_date
          and (cast(:transaction_type as text) is null
               or transaction_type = cast(:transaction_type as text))
        group by first_digit
        order by first_digit
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def anomaly_evaluation(engine: Engine) -> pd.DataFrame:
    return read_frame(engine, "select * from analytics.mart_anomaly_evaluation limit 1")


def transaction_sample(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        select event_id, event_time, transaction_type, amount, risk_score, risk_level,
               predicted_fraud, is_fraud, has_origin_balance_anomaly,
               has_destination_balance_anomaly, origin_balance_difference
        from analytics.fct_transactions
        where event_date between :start_date and :end_date
          and (cast(:transaction_type as text) is null
               or transaction_type = cast(:transaction_type as text))
        order by event_time desc
        limit 5000
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def hourly_patterns(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        select event_hour, transaction_type, count(*) as transaction_count,
               count(*) filter (where predicted_fraud) as alert_count,
               avg(risk_score) as average_risk_score,
               count(*) filter (where predicted_fraud)::numeric
                   / nullif(count(*), 0) as alert_rate
        from analytics.fct_transactions
        where event_date between :start_date and :end_date
          and (cast(:transaction_type as text) is null
               or transaction_type = cast(:transaction_type as text))
        group by event_hour, transaction_type
        order by event_hour, transaction_type
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def rule_frequency(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        select rule, count(*) as trigger_count
        from analytics.fct_transactions t
        cross join lateral unnest(t.triggered_rules) as rule
        where event_date between :start_date and :end_date
          and (cast(:transaction_type as text) is null
               or transaction_type = cast(:transaction_type as text))
        group by rule
        order by trigger_count desc
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def anomaly_scores(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        with latest_model as (
            select model_version
            from monitoring.ml_model_runs
            where status = 'SUCCESS'
            order by finished_at desc
            limit 1
        )
        select t.event_time, t.transaction_type, t.amount, t.risk_score,
               t.is_fraud, s.anomaly_score, s.is_anomaly, s.dataset_split,
               s.model_version
        from analytics.transaction_anomaly_scores s
        join latest_model using (model_version)
        join analytics.fct_transactions t using (event_id)
        where t.event_date between :start_date and :end_date
          and (cast(:transaction_type as text) is null
               or t.transaction_type = cast(:transaction_type as text))
        order by t.event_time desc
        limit 5000
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def rules_vs_ml(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        with latest_model as (
            select model_version
            from monitoring.ml_model_runs
            where status = 'SUCCESS'
            order by finished_at desc
            limit 1
        ), compared as (
            select
                case
                    when t.predicted_fraud and s.is_anomaly then 'Regras + ML'
                    when t.predicted_fraud then 'Somente regras'
                    when s.is_anomaly then 'Somente ML'
                    else 'Nenhum alerta'
                end as detection_group,
                t.is_fraud,
                t.amount
            from analytics.fct_transactions t
            join analytics.transaction_anomaly_scores s using (event_id)
            join latest_model using (model_version)
            where t.event_date between :start_date and :end_date
              and (cast(:transaction_type as text) is null
                   or t.transaction_type = cast(:transaction_type as text))
        )
        select detection_group, count(*) as transaction_count,
               count(*) filter (where is_fraud) as fraud_count,
               coalesce(sum(amount), 0) as total_amount,
               coalesce(sum(amount) filter (where is_fraud), 0) as fraud_amount
        from compared
        group by detection_group
        order by case detection_group
            when 'Regras + ML' then 1
            when 'Somente regras' then 2
            when 'Somente ML' then 3
            else 4 end
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def suspicious_accounts(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        with latest_model as (
            select model_version
            from monitoring.ml_model_runs
            where status = 'SUCCESS'
            order by finished_at desc
            limit 1
        )
        select t.origin_account,
               count(*) as transaction_count,
               count(*) filter (where t.predicted_fraud) as rule_alert_count,
               count(*) filter (where s.is_anomaly) as anomaly_count,
               count(*) filter (where t.is_fraud) as fraud_count,
               coalesce(sum(t.amount), 0) as total_amount,
               coalesce(avg(t.risk_score), 0) as average_risk_score,
               max(s.anomaly_score) as maximum_anomaly_score
        from analytics.fct_transactions t
        join analytics.transaction_anomaly_scores s using (event_id)
        join latest_model using (model_version)
        where t.event_date between :start_date and :end_date
          and (cast(:transaction_type as text) is null
               or t.transaction_type = cast(:transaction_type as text))
        group by t.origin_account
        having count(*) filter (where t.predicted_fraud or s.is_anomaly) > 0
        order by anomaly_count desc, rule_alert_count desc,
                 maximum_anomaly_score desc
        limit 20
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def investigation_candidates(
    engine: Engine, start: str, end: str, transaction_type: str | None
) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        with latest_model as (
            select model_version
            from monitoring.ml_model_runs
            where status = 'SUCCESS'
            order by finished_at desc
            limit 1
        )
        select t.event_id::text as event_id, t.event_time, t.transaction_type,
               t.amount, t.origin_account, t.risk_score, t.predicted_fraud,
               t.is_fraud, s.anomaly_score, s.is_anomaly
        from analytics.fct_transactions t
        join analytics.transaction_anomaly_scores s using (event_id)
        join latest_model using (model_version)
        where t.event_date between :start_date and :end_date
          and (t.predicted_fraud or s.is_anomaly or t.is_fraud)
          and (cast(:transaction_type as text) is null
               or t.transaction_type = cast(:transaction_type as text))
        order by t.is_fraud desc, s.is_anomaly desc, s.anomaly_score desc,
                 t.event_time desc
        limit 200
        """,
        {"start_date": start, "end_date": end, "transaction_type": transaction_type},
    )


def transaction_investigation(engine: Engine, event_id: str) -> pd.DataFrame:
    return read_frame(
        engine,
        """
        with latest_model as (
            select model_version
            from monitoring.ml_model_runs
            where status = 'SUCCESS'
            order by finished_at desc
            limit 1
        )
        select t.event_id::text as event_id, t.event_time, t.transaction_type,
               t.amount, t.origin_account, t.destination_account,
               t.origin_old_balance, t.origin_new_balance,
               t.destination_old_balance, t.destination_new_balance,
               t.origin_balance_change, t.destination_balance_change,
               t.origin_balance_difference, t.has_origin_balance_anomaly,
               t.has_destination_balance_anomaly, t.risk_score, t.risk_level,
               t.triggered_rules, t.predicted_fraud, t.is_fraud,
               s.anomaly_score, s.is_anomaly, s.dataset_split, s.model_version,
               f.recent_transaction_count, f.recent_transaction_volume,
               f.historical_log_amount_mean, f.historical_log_amount_stddev,
               f.log_amount_zscore, f.log_amount_robust_zscore
        from analytics.fct_transactions t
        join analytics.transaction_anomaly_scores s using (event_id)
        join latest_model using (model_version)
        left join analytics.ml_transaction_features f using (event_id)
        where t.event_id = cast(:event_id as uuid)
        limit 1
        """,
        {"event_id": event_id},
    )
