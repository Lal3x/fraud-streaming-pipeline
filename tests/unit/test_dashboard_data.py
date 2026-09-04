import pandas as pd

from fraud_streaming_pipeline.dashboard import data
from fraud_streaming_pipeline.dashboard.data import database_url


def test_database_url_uses_analytics_environment() -> None:
    url = database_url(
        {
            "ANALYTICS_DB_HOST": "db.internal",
            "ANALYTICS_DB_PORT": "5544",
            "ANALYTICS_DB_NAME": "warehouse",
            "ANALYTICS_DB_USER": "reader",
            "ANALYTICS_DB_PASSWORD": "secret:/@",
        }
    )

    assert url.drivername == "postgresql+psycopg"
    assert url.host == "db.internal"
    assert url.port == 5544
    assert url.database == "warehouse"
    assert url.username == "reader"
    assert url.password == "secret:/@"


def test_database_url_has_local_defaults() -> None:
    url = database_url({})

    assert url.host == "localhost"
    assert url.port == 5434
    assert url.database == "fraud_analytics"


def test_available_dates_handles_empty_and_populated_results(monkeypatch) -> None:
    monkeypatch.setattr(data, "read_frame", lambda *_args, **_kwargs: pd.DataFrame())
    assert data.available_dates(object()) == (None, None)

    frame = pd.DataFrame([{"min_date": "2026-01-01", "max_date": "2026-01-03"}])
    monkeypatch.setattr(data, "read_frame", lambda *_args, **_kwargs: frame)

    assert data.available_dates(object()) == (
        pd.Timestamp("2026-01-01"),
        pd.Timestamp("2026-01-03"),
    )


def test_transaction_types_returns_plain_list(monkeypatch) -> None:
    monkeypatch.setattr(
        data,
        "read_frame",
        lambda *_args, **_kwargs: pd.DataFrame(
            {"transaction_type": ["PAYMENT", "TRANSFER"]}
        ),
    )

    assert data.transaction_types(object()) == ["PAYMENT", "TRANSFER"]


def test_dashboard_queries_forward_filters_as_parameters(monkeypatch) -> None:
    calls = []

    def fake_read_frame(_engine, query, params=None):
        calls.append((query, params))
        return pd.DataFrame()

    monkeypatch.setattr(data, "read_frame", fake_read_frame)
    filtered_queries = (
        data.overview,
        data.daily_metrics,
        data.recent_alerts,
        data.benford,
        data.transaction_sample,
        data.hourly_patterns,
        data.rule_frequency,
        data.anomaly_scores,
        data.rules_vs_ml,
        data.suspicious_accounts,
        data.investigation_candidates,
    )

    for query in filtered_queries:
        query(object(), "2026-01-01", "2026-01-02", "TRANSFER")

    assert len(calls) == len(filtered_queries)
    assert all(
        params
        == {
            "start_date": "2026-01-01",
            "end_date": "2026-01-02",
            "transaction_type": "TRANSFER",
        }
        for _, params in calls
    )


def test_simple_dashboard_queries_use_expected_identifiers(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        data,
        "read_frame",
        lambda _engine, query, params=None: (
            calls.append((query, params)) or pd.DataFrame()
        ),
    )

    data.pipeline_health(object())
    data.anomaly_evaluation(object())
    data.transaction_investigation(object(), "event-id")

    assert "mart_pipeline_health" in calls[0][0]
    assert "mart_anomaly_evaluation" in calls[1][0]
    assert calls[2][1] == {"event_id": "event-id"}
