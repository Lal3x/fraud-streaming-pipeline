"""Schemas Spark dos contratos de entrada e saída da camada Silver."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MONEY_TYPE = DecimalType(20, 2)

BRONZE_SCHEMA = StructType(
    [
        StructField("kafka_key", StringType(), True),
        StructField("raw_value", StringType(), True),
        StructField("kafka_topic", StringType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("ingestion_date", DateType(), False),
    ]
)

TRANSACTION_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("source_record_id", StringType(), True),
        StructField("produced_at", TimestampType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("step", IntegerType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", MONEY_TYPE, True),
        StructField("origin_account", StringType(), True),
        StructField("origin_old_balance", MONEY_TYPE, True),
        StructField("origin_new_balance", MONEY_TYPE, True),
        StructField("destination_account", StringType(), True),
        StructField("destination_old_balance", MONEY_TYPE, True),
        StructField("destination_new_balance", MONEY_TYPE, True),
        StructField("is_fraud", BooleanType(), True),
        StructField("is_flagged_fraud", BooleanType(), True),
    ]
)
