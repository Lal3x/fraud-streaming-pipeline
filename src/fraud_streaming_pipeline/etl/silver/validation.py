"""Regras de qualidade que separam registros válidos e rejeitados."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SUPPORTED_TRANSACTION_TYPES = (
    "CASH_IN",
    "CASH_OUT",
    "DEBIT",
    "PAYMENT",
    "TRANSFER",
)
UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)


def _error_when(condition: Column, message: str) -> Column:
    """Produz uma mensagem de erro quando uma condição de qualidade é atendida."""
    return F.when(condition, F.lit(message))


def add_validation_errors(events: DataFrame) -> DataFrame:
    """Acumula todos os erros de contrato encontrados em cada evento."""
    """Collect every technical validation failure for each event."""
    required_columns = (
        "schema_version",
        "event_id",
        "source_record_id",
        "produced_at",
        "event_time",
        "source",
        "step",
        "transaction_type",
        "amount",
        "origin_account",
        "origin_old_balance",
        "origin_new_balance",
        "destination_account",
        "destination_old_balance",
        "destination_new_balance",
        "is_fraud",
        "is_flagged_fraud",
    )
    errors = [
        _error_when(F.get_json_object("raw_value", "$").isNull(), "invalid_json"),
        _error_when(
            F.col("schema_version").isNotNull() & (F.col("schema_version") != "1.0"),
            "unsupported_schema_version",
        ),
        _error_when(
            F.col("event_id").isNotNull() & ~F.col("event_id").rlike(UUID_PATTERN),
            "invalid_event_id",
        ),
        _error_when(
            F.col("source_record_id").isNotNull() & (F.trim("source_record_id") == ""),
            "empty_source_record_id",
        ),
        _error_when(
            F.col("transaction_type").isNotNull()
            & ~F.col("transaction_type").isin(*SUPPORTED_TRANSACTION_TYPES),
            "invalid_transaction_type",
        ),
        _error_when(F.col("amount") < 0, "negative_amount"),
        _error_when(
            F.col("origin_account").isNotNull()
            & ~F.col("origin_account").rlike(r"^C[0-9]+$"),
            "invalid_origin_account",
        ),
        _error_when(
            F.col("destination_account").isNotNull()
            & ~F.col("destination_account").rlike(r"^[CM][0-9]+$"),
            "invalid_destination_account",
        ),
        _error_when(F.col("origin_old_balance") < 0, "negative_origin_old_balance"),
        _error_when(F.col("origin_new_balance") < 0, "negative_origin_new_balance"),
        _error_when(
            F.col("destination_old_balance") < 0,
            "negative_destination_old_balance",
        ),
        _error_when(
            F.col("destination_new_balance") < 0,
            "negative_destination_new_balance",
        ),
    ]
    errors.extend(
        _error_when(F.col(column).isNull(), f"missing_{column}")
        for column in required_columns
    )

    return events.withColumn(
        "validation_errors",
        F.filter(F.array(*errors), lambda error: error.isNotNull()),
    )


def split_valid_and_invalid(
    events: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Divide eventos aptos à Silver daqueles enviados para quarentena."""
    """Split parsed events according to their collected validation errors."""
    valid = events.filter(F.size("validation_errors") == 0)
    invalid = events.filter(F.size("validation_errors") > 0).select(
        "raw_value",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at",
        "ingestion_date",
        "validation_errors",
    )
    return valid, invalid
