"""Transformações que convertem eventos Bronze no modelo analítico Silver."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fraud_streaming_pipeline.etl.silver.schemas import (
    TRANSACTION_EVENT_SCHEMA,
)

BALANCE_TOLERANCE = 0.01


def parse_transaction_events(bronze_events: DataFrame) -> DataFrame:
    """Desserializa o JSON da Bronze usando um schema explícito."""
    """Parse the audited JSON payload using the explicit event schema."""
    return bronze_events.withColumn(
        "_parsed_event",
        F.from_json(F.col("raw_value"), TRANSACTION_EVENT_SCHEMA),
    ).select("*", "_parsed_event.*")


def enrich_transactions(transactions: DataFrame) -> DataFrame:
    """Deriva variações de saldo e indicadores úteis à detecção de fraude."""
    """Add business-oriented, label-independent transaction features."""
    is_merchant = F.col("destination_account").startswith("M")
    expected_origin = F.when(
        F.col("transaction_type") == "CASH_IN",
        F.col("origin_old_balance") + F.col("amount"),
    ).otherwise(F.col("origin_old_balance") - F.col("amount"))
    expected_destination = F.col("destination_old_balance") + F.col("amount")

    return (
        transactions.withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn("is_merchant_destination", is_merchant)
        .withColumn(
            "origin_balance_change",
            F.col("origin_new_balance") - F.col("origin_old_balance"),
        )
        .withColumn(
            "destination_balance_change",
            F.col("destination_new_balance") - F.col("destination_old_balance"),
        )
        .withColumn("expected_origin_balance", expected_origin)
        .withColumn(
            "origin_balance_difference",
            F.col("origin_new_balance") - F.col("expected_origin_balance"),
        )
        .withColumn(
            "has_origin_balance_anomaly",
            F.abs("origin_balance_difference") > BALANCE_TOLERANCE,
        )
        .withColumn(
            "has_destination_balance_anomaly",
            F.when(is_merchant, F.lit(False)).otherwise(
                F.abs(F.col("destination_new_balance") - expected_destination)
                > BALANCE_TOLERANCE
            ),
        )
        .withColumn(
            "is_origin_account_drained",
            F.col("origin_new_balance") <= BALANCE_TOLERANCE,
        )
        .withColumn(
            "processing_latency_seconds",
            F.greatest(
                F.lit(0),
                F.unix_timestamp("ingested_at") - F.unix_timestamp("event_time"),
            ),
        )
        .withColumn(
            "amount_range",
            F.when(F.col("amount") < 1_000, "SMALL")
            .when(F.col("amount") < 10_000, "MEDIUM")
            .when(F.col("amount") < 100_000, "LARGE")
            .otherwise("VERY_LARGE"),
        )
    )
