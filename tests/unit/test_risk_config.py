from fraud_streaming_pipeline.etl.silver.fraud_rules import RiskConfig


def test_risk_config_reads_environment(monkeypatch) -> None:
    monkeypatch.setenv("SILVER_HIGH_AMOUNT_THRESHOLD", "250000")
    monkeypatch.setenv("SILVER_ALERT_SCORE", "75")

    config = RiskConfig.from_env()

    assert config.high_amount_threshold == 250_000
    assert config.alert_score == 75
