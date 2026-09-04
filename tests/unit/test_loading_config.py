import pytest

from fraud_streaming_pipeline.etl.loading.job import LoadingConfig

REQUIRED_ENV = {
    "ANALYTICS_DB_HOST": "localhost",
    "ANALYTICS_DB_PORT": "5434",
    "ANALYTICS_DB_NAME": "fraud_analytics",
    "ANALYTICS_DB_USER": "analytics",
    "ANALYTICS_DB_PASSWORD": "secret",
    "MINIO_ENDPOINT": "http://localhost:9000",
    "MINIO_ACCESS_KEY": "access",
    "MINIO_SECRET_KEY": "secret",
    "SILVER_TRANSACTIONS_PATH": "s3a://bucket/silver/transactions",
    "SILVER_ALERTS_PATH": "s3a://bucket/silver/fraud-alerts",
}


def test_loading_config_reads_required_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)

    config = LoadingConfig.from_env()

    assert config.jdbc_url == "jdbc:postgresql://localhost:5434/fraud_analytics"
    assert "password=secret" in config.postgres_dsn


def test_loading_config_rejects_missing_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in REQUIRED_ENV:
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match="ANALYTICS_DB_HOST"):
        LoadingConfig.from_env()


def test_loading_config_reads_expected_minimum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("EXPECTED_MIN_RECORDS", "10000")

    config = LoadingConfig.from_env()

    assert config.expected_min_records == 10000


def test_loading_config_rejects_negative_expected_minimum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("EXPECTED_MIN_RECORDS", "-1")

    with pytest.raises(ValueError, match="EXPECTED_MIN_RECORDS"):
        LoadingConfig.from_env()
