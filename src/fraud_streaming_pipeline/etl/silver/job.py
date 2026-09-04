"""Validação, enriquecimento e classificação dos eventos da camada Silver."""

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from fraud_streaming_pipeline.etl.silver.fraud_rules import (
    RiskConfig,
    apply_fraud_rules,
)
from fraud_streaming_pipeline.etl.silver.schemas import BRONZE_SCHEMA
from fraud_streaming_pipeline.etl.silver.transformations import (
    enrich_transactions,
    parse_transaction_events,
)
from fraud_streaming_pipeline.etl.silver.validation import (
    add_validation_errors,
    split_valid_and_invalid,
)


@dataclass(frozen=True)
class SilverConfig:
    """Configuração das fontes, saídas e checkpoints do processamento Silver."""

    bronze_path: str
    transactions_path: str
    invalid_events_path: str
    fraud_alerts_path: str
    transactions_checkpoint: str
    invalid_events_checkpoint: str
    fraud_alerts_checkpoint: str
    watermark: str
    trigger_interval: str

    @classmethod
    def from_env(cls) -> "SilverConfig":
        """Carrega caminhos e intervalos usando valores seguros como padrão."""
        bucket = os.getenv("MINIO_BUCKET", "fraud-data-lake")
        checkpoint_root = "/opt/spark/project/checkpoints"
        return cls(
            bronze_path=os.getenv(
                "SILVER_BRONZE_PATH",
                f"s3a://{bucket}/bronze/paysim/events",
            ),
            transactions_path=os.getenv(
                "SILVER_TRANSACTIONS_PATH",
                f"s3a://{bucket}/silver/transactions",
            ),
            invalid_events_path=os.getenv(
                "SILVER_INVALID_EVENTS_PATH",
                f"s3a://{bucket}/silver/invalid-events",
            ),
            fraud_alerts_path=os.getenv(
                "SILVER_FRAUD_ALERTS_PATH",
                f"s3a://{bucket}/silver/fraud-alerts",
            ),
            transactions_checkpoint=os.getenv(
                "SILVER_TRANSACTIONS_CHECKPOINT",
                f"{checkpoint_root}/silver-transactions-v1",
            ),
            invalid_events_checkpoint=os.getenv(
                "SILVER_INVALID_EVENTS_CHECKPOINT",
                f"{checkpoint_root}/silver-invalid-events-v1",
            ),
            fraud_alerts_checkpoint=os.getenv(
                "SILVER_FRAUD_ALERTS_CHECKPOINT",
                f"{checkpoint_root}/silver-fraud-alerts-v1",
            ),
            watermark=os.getenv("SILVER_WATERMARK", "24 hours"),
            trigger_interval=os.getenv("SILVER_TRIGGER_INTERVAL", "10 seconds"),
        )


def configure_s3a(spark: SparkSession, config: SilverConfig) -> None:
    """Configura S3A apenas quando algum caminho usa MinIO ou S3."""
    paths = (
        config.bronze_path,
        config.transactions_path,
        config.invalid_events_path,
        config.fraud_alerts_path,
    )
    if not any(path.startswith("s3a://") for path in paths):
        return

    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    endpoint = os.getenv("MINIO_ENDPOINT")
    if not access_key or not secret_key or not endpoint:
        raise ValueError(
            "MINIO_ROOT_USER, MINIO_ROOT_PASSWORD and MINIO_ENDPOINT "
            "are required for s3a:// paths"
        )

    hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_config.set("fs.s3a.endpoint", endpoint)
    hadoop_config.set("fs.s3a.endpoint.region", "us-east-1")
    hadoop_config.set("fs.s3a.path.style.access", "true")
    hadoop_config.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_config.set("fs.s3a.access.key", access_key)
    hadoop_config.set("fs.s3a.secret.key", secret_key)
    hadoop_config.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def build_silver_datasets(
    bronze_events: DataFrame,
    watermark: str,
    risk_config: RiskConfig,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Produz transações deduplicadas, rejeições e alertas de fraude."""
    validated = add_validation_errors(parse_transaction_events(bronze_events))
    valid, invalid = split_valid_and_invalid(validated)
    deduplicated = valid.withWatermark("event_time", watermark).dropDuplicates(
        ["event_id"]
    )
    transactions = apply_fraud_rules(
        enrich_transactions(deduplicated), risk_config
    ).drop("_parsed_event", "validation_errors")
    alerts = transactions.filter("predicted_fraud")
    return transactions, invalid, alerts


def start_parquet_sink(
    events: DataFrame,
    path: str,
    checkpoint: str,
    trigger_interval: str,
    partition_column: str,
) -> StreamingQuery:
    """Inicia uma saída Parquet incremental com checkpoint independente."""
    return (
        events.writeStream.format("parquet")
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .option("compression", "snappy")
        .partitionBy(partition_column)
        .trigger(processingTime=trigger_interval)
        .start()
    )


def main() -> None:
    """Executa simultaneamente as três saídas da camada Silver."""
    config = SilverConfig.from_env()
    risk_config = RiskConfig.from_env()
    spark = (
        SparkSession.builder.appName("fraud-transactions-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_s3a(spark, config)

    bronze_events = (
        spark.readStream.schema(BRONZE_SCHEMA)
        .format("parquet")
        .load(config.bronze_path)
    )
    transactions, invalid, alerts = build_silver_datasets(
        bronze_events,
        watermark=config.watermark,
        risk_config=risk_config,
    )
    queries = [
        start_parquet_sink(
            transactions,
            config.transactions_path,
            config.transactions_checkpoint,
            config.trigger_interval,
            "event_date",
        ),
        start_parquet_sink(
            invalid,
            config.invalid_events_path,
            config.invalid_events_checkpoint,
            config.trigger_interval,
            "ingestion_date",
        ),
        start_parquet_sink(
            alerts,
            config.fraud_alerts_path,
            config.fraud_alerts_checkpoint,
            config.trigger_interval,
            "event_date",
        ),
    ]

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
