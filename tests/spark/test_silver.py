import json
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import pytest
from pyspark.sql import DataFrame, SparkSession

from fraud_streaming_pipeline.etl.silver.fraud_rules import RiskConfig
from fraud_streaming_pipeline.etl.silver.job import build_silver_datasets
from fraud_streaming_pipeline.etl.silver.schemas import BRONZE_SCHEMA


def event(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "schema_version": "1.0",
        "event_id": str(uuid4()),
        "source_record_id": "paysim1:1",
        "produced_at": "2026-09-02T12:00:01Z",
        "event_time": "2026-09-02T12:00:00Z",
        "source": "paysim",
        "step": 1,
        "transaction_type": "PAYMENT",
        "amount": 100.0,
        "origin_account": "C100",
        "origin_old_balance": 1_000.0,
        "origin_new_balance": 900.0,
        "destination_account": "M200",
        "destination_old_balance": 0.0,
        "destination_new_balance": 0.0,
        "is_fraud": False,
        "is_flagged_fraud": False,
    }
    values.update(overrides)
    return values


def bronze_frame(
    spark: SparkSession,
    payloads: list[dict[str, Any] | str],
) -> DataFrame:
    rows = []
    for offset, payload in enumerate(payloads):
        raw_value = payload if isinstance(payload, str) else json.dumps(payload)
        rows.append(
            (
                "C100",
                raw_value,
                "fraud-transactions-raw",
                0,
                offset,
                datetime(2026, 9, 2, 12, tzinfo=UTC),
                datetime(2026, 9, 2, 12, 0, 2, tzinfo=UTC),
                date(2026, 9, 2),
            )
        )
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def transform(
    spark: SparkSession,
    payloads: list[dict[str, Any] | str],
) -> tuple[DataFrame, DataFrame, DataFrame]:
    return build_silver_datasets(
        bronze_frame(spark, payloads),
        watermark="24 hours",
        risk_config=RiskConfig(),
    )


def validation_errors(spark: SparkSession, payload: dict[str, Any] | str) -> list[str]:
    _, invalid, _ = transform(spark, [payload])
    return invalid.select("validation_errors").first()[0]


def test_valid_json_is_sent_to_transactions(spark: SparkSession) -> None:
    transactions, invalid, _ = transform(spark, [event()])

    assert transactions.count() == 1
    assert invalid.count() == 0


def test_corrupt_json_is_invalid(spark: SparkSession) -> None:
    assert "invalid_json" in validation_errors(spark, "{not-json")


def test_missing_required_field_is_invalid(spark: SparkSession) -> None:
    payload = event()
    del payload["amount"]

    assert "missing_amount" in validation_errors(spark, payload)


@pytest.mark.parametrize(
    ("overrides", "expected_error"),
    [
        ({"schema_version": "2.0"}, "unsupported_schema_version"),
        ({"transaction_type": "WIRE"}, "invalid_transaction_type"),
        ({"amount": -1}, "negative_amount"),
        ({"origin_account": "X100"}, "invalid_origin_account"),
        ({"origin_old_balance": -1}, "negative_origin_old_balance"),
        (
            {"destination_new_balance": -1},
            "negative_destination_new_balance",
        ),
    ],
)
def test_technical_validation_errors(
    spark: SparkSession,
    overrides: dict[str, Any],
    expected_error: str,
) -> None:
    assert expected_error in validation_errors(spark, event(**overrides))


def test_duplicate_event_id_is_removed(spark: SparkSession) -> None:
    payload = event()
    transactions, _, _ = transform(spark, [payload, payload])

    assert transactions.count() == 1


def test_drained_account_rule(spark: SparkSession) -> None:
    transactions, _, _ = transform(
        spark,
        [event(origin_old_balance=100, origin_new_balance=0)],
    )
    row = transactions.first()

    assert row.is_origin_account_drained is True
    assert row.rule_origin_account_drained is True


def test_high_amount_rule_and_score(spark: SparkSession) -> None:
    transactions, _, alerts = transform(
        spark,
        [
            event(
                transaction_type="TRANSFER",
                amount=150_000,
                origin_old_balance=150_000,
                origin_new_balance=0,
                destination_account="C200",
                destination_new_balance=150_000,
            )
        ],
    )
    row = transactions.first()

    assert row.rule_high_amount is True
    assert row.risk_score == 85
    assert row.risk_level == "HIGH"
    assert row.predicted_fraud is True
    assert alerts.count() == 1


def test_merchant_destination_skips_balance_anomaly(
    spark: SparkSession,
) -> None:
    transactions, _, _ = transform(
        spark,
        [event(destination_new_balance=0)],
    )
    row = transactions.first()

    assert row.is_merchant_destination is True
    assert row.has_destination_balance_anomaly is False


def test_origin_balance_anomaly(spark: SparkSession) -> None:
    transactions, _, _ = transform(
        spark,
        [event(origin_new_balance=950)],
    )
    row = transactions.first()

    assert row.has_origin_balance_anomaly is True
    assert row.rule_origin_balance_anomaly is True


def test_fraud_labels_do_not_change_score(spark: SparkSession) -> None:
    first = event(event_id=str(uuid4()), is_fraud=False, is_flagged_fraud=False)
    second = {
        **first,
        "event_id": str(uuid4()),
        "is_fraud": True,
        "is_flagged_fraud": True,
    }
    transactions, _, _ = transform(spark, [first, second])

    scores = [row.risk_score for row in transactions.collect()]
    assert scores[0] == scores[1]


def test_valid_and_invalid_events_are_separated(
    spark: SparkSession,
) -> None:
    transactions, invalid, _ = transform(
        spark,
        [event(), event(amount=-1), "broken-json"],
    )

    assert transactions.count() == 1
    assert invalid.count() == 2
