from uuid import UUID

import pytest

from fraud_streaming_pipeline.ml.isolation_forest_job import (
    FEATURES,
    IsolationForestConfig,
    create_run,
    feature_matrix,
    load_features,
    persist_scores,
    record_failure,
    temporal_split_index,
)

REQUIRED_ENV = {
    "ANALYTICS_DB_HOST": "localhost",
    "ANALYTICS_DB_PORT": "5434",
    "ANALYTICS_DB_NAME": "fraud",
    "ANALYTICS_DB_USER": "analytics",
    "ANALYTICS_DB_PASSWORD": "secret",
}


def test_temporal_split_respects_minimum_and_keeps_evaluation_rows() -> None:
    assert temporal_split_index(101, 0.8, 100) == 100
    assert temporal_split_index(200, 0.8, 100) == 160


def test_temporal_split_rejects_small_dataset() -> None:
    with pytest.raises(ValueError, match="At least 101"):
        temporal_split_index(100, 0.8, 100)


def test_config_rejects_invalid_contamination() -> None:
    config = IsolationForestConfig(
        db_host="localhost",
        db_port=5434,
        db_name="fraud_analytics",
        db_user="analytics",
        db_password="secret",
        contamination=0.6,
    )
    with pytest.raises(ValueError, match="ML_CONTAMINATION"):
        config.validate()


def test_config_rejects_unbounded_window_smaller_than_training_minimum() -> None:
    config = IsolationForestConfig(
        db_host="localhost",
        db_port=5434,
        db_name="fraud_analytics",
        db_user="analytics",
        db_password="secret",
        min_train_rows=100,
        max_rows=100,
    )
    with pytest.raises(ValueError, match="ML_MAX_ROWS"):
        config.validate()


def test_feature_matrix_never_includes_labels() -> None:
    row = {
        "event_id": "id",
        "event_time": "2026-01-01",
        "is_fraud": True,
        "is_flagged_fraud": True,
        "predicted_fraud": True,
        "amount": 10,
        "log1p_amount": 2.4,
        "event_hour": 1,
        "origin_balance_change": -10,
        "destination_balance_change": 10,
        "origin_balance_difference": 0,
        "has_origin_balance_anomaly": False,
        "has_destination_balance_anomaly": False,
        "recent_transaction_count": 2,
        "recent_transaction_volume": 30,
        "historical_log_amount_mean": 2,
        "historical_log_amount_stddev": 1,
        "historical_log_amount_median": 2,
        "historical_log_amount_mad": 1,
        "log_amount_zscore": 0.4,
        "log_amount_robust_zscore": 0.2,
        "transaction_type": "PAYMENT",
    }

    matrix = feature_matrix([row])

    assert not {"is_fraud", "is_flagged_fraud", "predicted_fraud"} & set(FEATURES)
    assert len(matrix[0]) == len(FEATURES)


def test_config_reads_environment_and_builds_dsn(monkeypatch) -> None:
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("ML_MAX_ROWS", "200")
    monkeypatch.setenv("ML_MIN_TRAIN_ROWS", "10")
    monkeypatch.setenv("ML_N_JOBS", "1")

    config = IsolationForestConfig.from_env()

    assert config.max_rows == 200
    assert config.n_jobs == 1
    assert "dbname=fraud" in config.dsn


def test_config_rejects_missing_environment(monkeypatch) -> None:
    for name in REQUIRED_ENV:
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match="ANALYTICS_DB_HOST"):
        IsolationForestConfig.from_env()


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"train_fraction": 1}, "ML_TRAIN_FRACTION"),
        ({"min_train_rows": 1}, "ML_MIN_TRAIN_ROWS"),
        ({"n_estimators": 0}, "ML_N_ESTIMATORS"),
        ({"n_jobs": 0}, "ML_N_JOBS"),
        ({"persist_batch_size": 0}, "ML_PERSIST_BATCH_SIZE"),
    ],
)
def test_config_rejects_other_invalid_limits(override, message) -> None:
    values = {
        "db_host": "localhost",
        "db_port": 5434,
        "db_name": "fraud",
        "db_user": "analytics",
        "db_password": "secret",
        **override,
    }

    with pytest.raises(ValueError, match=message):
        IsolationForestConfig(**values).validate()


class Result:
    def __init__(self, rows) -> None:
        self.rows = rows

    def fetchall(self):
        return self.rows


class Transaction:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class Cursor(Transaction):
    def __init__(self) -> None:
        self.batches = []

    def executemany(self, statement, values) -> None:
        self.batches.append((statement, list(values)))


class Connection:
    def __init__(self, query_rows=()) -> None:
        self.query_rows = query_rows
        self.executions = []
        self.commits = 0
        self.cursor_instance = Cursor()

    def execute(self, statement, params=()):
        self.executions.append((statement, params))
        return Result(self.query_rows)

    def commit(self) -> None:
        self.commits += 1

    def transaction(self):
        return Transaction()

    def cursor(self):
        return self.cursor_instance


def test_load_features_limits_and_maps_database_rows() -> None:
    raw = ("id", "2026-01-01", *range(len(FEATURES)))
    connection = Connection([raw])

    rows = load_features(connection, 50)

    assert rows[0]["event_id"] == "id"
    assert rows[0][FEATURES[-1]] == len(FEATURES) - 1
    assert connection.executions[0][1] == (50,)


def test_run_lifecycle_persists_scores_in_bounded_batches() -> None:
    connection = Connection()
    config = IsolationForestConfig(
        db_host="localhost",
        db_port=5434,
        db_name="fraud",
        db_user="analytics",
        db_password="secret",
        min_train_rows=2,
        persist_batch_size=2,
    )
    run_id = UUID("12345678-1234-5678-1234-567812345678")
    rows = [
        {"event_id": f"id-{index}", "event_time": f"2026-01-0{index + 1}"}
        for index in range(3)
    ]

    create_run(connection, run_id, "model-v1", config, "started")
    persist_scores(
        connection,
        rows,
        split_index=2,
        scores=[0.1, -0.2, 0.3],
        predictions=[1, -1, 1],
        run_id=run_id,
        model_version="model-v1",
        scored_at="finished",
        config=config,
    )

    batches = connection.cursor_instance.batches
    assert [len(values) for _, values in batches] == [2, 1]
    assert batches[0][1][0][2] == "TRAIN"
    assert batches[1][1][0][2] == "EVALUATION"
    assert connection.commits == 1
    assert "SUCCESS" in connection.executions[-1][0]


def test_record_failure_truncates_message_and_commits() -> None:
    connection = Connection()
    run_id = UUID("12345678-1234-5678-1234-567812345678")

    record_failure(connection, run_id, RuntimeError("x" * 5000))

    assert len(connection.executions[0][1][0]) == 4000
    assert connection.executions[0][1][1] == run_id
    assert connection.commits == 1
