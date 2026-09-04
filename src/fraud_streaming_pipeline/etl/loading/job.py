"""Carga incremental e idempotente da camada Silver no PostgreSQL."""

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg import Connection
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

JDBC_DRIVER = "org.postgresql.Driver"
PIPELINE_NAME = "silver_to_postgres"

TRANSACTION_COLUMNS = (
    "event_id",
    "source_record_id",
    "schema_version",
    "event_time",
    "produced_at",
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
    "event_hour",
    "is_merchant_destination",
    "origin_balance_change",
    "destination_balance_change",
    "expected_origin_balance",
    "origin_balance_difference",
    "has_origin_balance_anomaly",
    "has_destination_balance_anomaly",
    "is_origin_account_drained",
    "processing_latency_seconds",
    "amount_range",
    "rule_high_amount",
    "rule_risky_transaction_type",
    "rule_origin_account_drained",
    "rule_origin_balance_anomaly",
    "risk_score",
    "risk_level",
    "predicted_fraud",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "ingestion_date",
)

ALERT_COLUMNS = (
    "event_id",
    "event_time",
    "transaction_type",
    "amount",
    "origin_account",
    "destination_account",
    "risk_score",
    "risk_level",
    "predicted_fraud",
)


@dataclass(frozen=True)
class LoadingConfig:
    """Credenciais, caminhos e controles de integridade da carga."""

    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    transactions_path: str
    alerts_path: str
    expected_min_records: int = 0

    @classmethod
    def from_env(cls) -> "LoadingConfig":
        """Carrega o ambiente e falha cedo quando uma variável obrigatória falta."""
        required = (
            "ANALYTICS_DB_HOST",
            "ANALYTICS_DB_PORT",
            "ANALYTICS_DB_NAME",
            "ANALYTICS_DB_USER",
            "ANALYTICS_DB_PASSWORD",
            "MINIO_ENDPOINT",
            "MINIO_ACCESS_KEY",
            "MINIO_SECRET_KEY",
            "SILVER_TRANSACTIONS_PATH",
            "SILVER_ALERTS_PATH",
        )
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        config = cls(
            db_host=os.environ["ANALYTICS_DB_HOST"],
            db_port=int(os.environ["ANALYTICS_DB_PORT"]),
            db_name=os.environ["ANALYTICS_DB_NAME"],
            db_user=os.environ["ANALYTICS_DB_USER"],
            db_password=os.environ["ANALYTICS_DB_PASSWORD"],
            minio_endpoint=os.environ["MINIO_ENDPOINT"],
            minio_access_key=os.environ["MINIO_ACCESS_KEY"],
            minio_secret_key=os.environ["MINIO_SECRET_KEY"],
            transactions_path=os.environ["SILVER_TRANSACTIONS_PATH"],
            alerts_path=os.environ["SILVER_ALERTS_PATH"],
            expected_min_records=int(os.getenv("EXPECTED_MIN_RECORDS", "0")),
        )
        if config.expected_min_records < 0:
            raise ValueError("EXPECTED_MIN_RECORDS cannot be negative")
        return config

    @property
    def postgres_dsn(self) -> str:
        """Monta a conexão usada nas operações transacionais de controle."""
        return (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_password}"
        )

    @property
    def jdbc_url(self) -> str:
        """Monta a URL JDBC usada pelo Spark para escrever no staging."""
        return f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"


def configure_s3a(spark: SparkSession, config: LoadingConfig) -> None:
    """Configura o Spark para ler os Parquets armazenados no MinIO."""
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop.set("fs.s3a.endpoint", config.minio_endpoint)
    hadoop.set("fs.s3a.endpoint.region", "us-east-1")
    hadoop.set("fs.s3a.path.style.access", "true")
    hadoop.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop.set("fs.s3a.access.key", config.minio_access_key)
    hadoop.set("fs.s3a.secret.key", config.minio_secret_key)
    hadoop.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def list_parquet_files(spark: SparkSession, root_path: str) -> list[str]:
    """Lista os Parquets físicos sem materializar seu conteúdo em memória."""
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(root_path)
    filesystem = path.getFileSystem(hadoop)
    if not filesystem.exists(path):
        return []
    iterator = filesystem.listFiles(path, True)
    files: list[str] = []
    while iterator.hasNext():
        item = iterator.next()
        item_path = item.getPath().toString()
        if item_path.endswith(".parquet") and "/_" not in item_path:
            files.append(item_path)
    return sorted(files)


def processed_files(connection: Connection[Any], dataset_name: str) -> set[str]:
    """Recupera do manifesto os arquivos já processados com sucesso."""
    rows = connection.execute(
        "SELECT source_file FROM monitoring.loaded_files WHERE dataset_name = %s",
        (dataset_name,),
    ).fetchall()
    return {row[0] for row in rows}


def prepare_transactions(frame: DataFrame, load_id: UUID) -> DataFrame:
    """Prepara transações para a tabela temporária da carga atual."""
    return frame.select(
        F.lit(str(load_id)).cast("string").alias("load_id"),
        F.input_file_name().alias("source_file"),
        *(F.col(column) for column in TRANSACTION_COLUMNS),
        F.to_date("event_time").alias("event_date"),
        F.to_json("triggered_rules").alias("triggered_rules_json"),
    )


def prepare_alerts(frame: DataFrame, load_id: UUID) -> DataFrame:
    """Prepara apenas os alertas previstos para a tabela temporária."""
    return frame.select(
        F.lit(str(load_id)).cast("string").alias("load_id"),
        F.input_file_name().alias("source_file"),
        *(F.col(column) for column in ALERT_COLUMNS),
        F.to_json("triggered_rules").alias("triggered_rules_json"),
    )


def write_stage(
    frame: DataFrame,
    table: str,
    config: LoadingConfig,
) -> None:
    """Escreve no staging em lotes JDBC para limitar o uso de memória."""
    (
        frame.write.format("jdbc")
        .option("url", config.jdbc_url)
        .option("dbtable", table)
        .option("user", config.db_user)
        .option("password", config.db_password)
        .option("driver", JDBC_DRIVER)
        .option("batchsize", "5000")
        .mode("append")
        .save()
    )


def start_metric(
    connection: Connection[Any], load_id: UUID, started_at: datetime
) -> None:
    """Registra o começo da execução para monitoramento operacional."""
    connection.execute(
        """
        INSERT INTO monitoring.pipeline_metrics (
            load_id, pipeline_name, status, started_at
        ) VALUES (%s, %s, 'RUNNING', %s)
        """,
        (load_id, PIPELINE_NAME, started_at),
    )
    connection.commit()


def promote(
    connection: Connection[Any],
    load_id: UUID,
    transaction_files: list[str],
    alert_files: list[str],
    counts: dict[str, int],
) -> tuple[int, int]:
    """Atomically promote staged rows, manifests and success metrics."""
    with connection.transaction():
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))", (PIPELINE_NAME,)
        )
        transaction_insert = connection.execute(
            """
            INSERT INTO silver.transactions (
                event_id, source_record_id, schema_version, event_time, produced_at,
                source, step, transaction_type, amount, origin_account,
                origin_old_balance, origin_new_balance, destination_account,
                destination_old_balance, destination_new_balance, is_fraud,
                is_flagged_fraud, event_date, event_hour, is_merchant_destination,
                origin_balance_change, destination_balance_change,
                expected_origin_balance, origin_balance_difference,
                has_origin_balance_anomaly, has_destination_balance_anomaly,
                is_origin_account_drained, processing_latency_seconds, amount_range,
                rule_high_amount, rule_risky_transaction_type,
                rule_origin_account_drained, rule_origin_balance_anomaly,
                triggered_rules, risk_score, risk_level, predicted_fraud,
                kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
                ingested_at, ingestion_date, source_file, load_id
            )
            SELECT
                event_id::uuid, source_record_id, schema_version, event_time, produced_at,
                source, step, transaction_type, amount, origin_account,
                origin_old_balance, origin_new_balance, destination_account,
                destination_old_balance, destination_new_balance, is_fraud,
                is_flagged_fraud, event_date, event_hour, is_merchant_destination,
                origin_balance_change, destination_balance_change,
                expected_origin_balance, origin_balance_difference,
                has_origin_balance_anomaly, has_destination_balance_anomaly,
                is_origin_account_drained, processing_latency_seconds, amount_range,
                rule_high_amount, rule_risky_transaction_type,
                rule_origin_account_drained, rule_origin_balance_anomaly,
                ARRAY(SELECT jsonb_array_elements_text(triggered_rules_json::jsonb)),
                risk_score, risk_level, predicted_fraud, kafka_topic, kafka_partition,
                kafka_offset, kafka_timestamp, ingested_at, ingestion_date,
                source_file, load_id::uuid
            FROM loading.transactions_stage
            WHERE load_id = %s::text
            ON CONFLICT (event_id) DO NOTHING
            """,
            (load_id,),
        )
        inserted = transaction_insert.rowcount
        alert_insert = connection.execute(
            """
            INSERT INTO silver.fraud_alerts (
                event_id, event_time, transaction_type, amount, origin_account,
                destination_account, triggered_rules, risk_score, risk_level,
                predicted_fraud, source_file, load_id
            )
            SELECT
                event_id::uuid, event_time, transaction_type, amount, origin_account,
                destination_account,
                ARRAY(SELECT jsonb_array_elements_text(triggered_rules_json::jsonb)),
                risk_score, risk_level, predicted_fraud, source_file, load_id::uuid
            FROM loading.fraud_alerts_stage
            WHERE load_id = %s::text
            ON CONFLICT (event_id) DO NOTHING
            """,
            (load_id,),
        )
        alerts_inserted = alert_insert.rowcount
        for dataset_name, files in (
            ("transactions", transaction_files),
            ("fraud_alerts", alert_files),
        ):
            for source_file in files:
                connection.execute(
                    """
                    INSERT INTO monitoring.loaded_files (
                        dataset_name, source_file, load_id, row_count
                    ) VALUES (%s, %s, %s, 0)
                    ON CONFLICT (dataset_name, source_file) DO NOTHING
                    """,
                    (dataset_name, source_file, load_id),
                )
        duplicated = counts["records_read"] - inserted
        connection.execute(
            """
            UPDATE monitoring.pipeline_metrics
            SET status = 'SUCCESS', finished_at = CURRENT_TIMESTAMP,
                files_discovered = %s, files_processed = %s, records_read = %s,
                records_valid = %s, records_rejected = %s, records_inserted = %s,
                records_duplicated = %s, alerts_read = %s, alerts_inserted = %s
            WHERE load_id = %s
            """,
            (
                counts["files_discovered"],
                len(transaction_files) + len(alert_files),
                counts["records_read"],
                counts["records_read"],
                0,
                inserted,
                duplicated,
                counts["alerts_read"],
                alerts_inserted,
                load_id,
            ),
        )
        connection.execute(
            "DELETE FROM loading.transactions_stage WHERE load_id = %s::text",
            (load_id,),
        )
        connection.execute(
            "DELETE FROM loading.fraud_alerts_stage WHERE load_id = %s::text",
            (load_id,),
        )
    return inserted, alerts_inserted


def record_failure(config: LoadingConfig, load_id: UUID, error: Exception) -> None:
    """Registra a falha sem esconder a exceção que interrompeu a carga."""
    with psycopg.connect(config.postgres_dsn) as connection:
        connection.execute(
            """
            UPDATE monitoring.pipeline_metrics
            SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP, error_message = %s
            WHERE load_id = %s
            """,
            (str(error)[:4000], load_id),
        )


def main() -> None:
    """Carrega somente arquivos novos e promove o staging atomicamente."""
    config = LoadingConfig.from_env()
    load_id = uuid4()
    # timezone.utc keeps this job compatible with the image's Python 3.10.
    started_at = datetime.now(timezone.utc)  # noqa: UP017
    spark = SparkSession.builder.appName(PIPELINE_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    configure_s3a(spark, config)
    try:
        with psycopg.connect(config.postgres_dsn) as connection:
            start_metric(connection, load_id, started_at)
            transaction_candidates = list_parquet_files(spark, config.transactions_path)
            alert_candidates = list_parquet_files(spark, config.alerts_path)
            transaction_files = sorted(
                set(transaction_candidates)
                - processed_files(connection, "transactions")
            )
            alert_files = sorted(
                set(alert_candidates) - processed_files(connection, "fraud_alerts")
            )
            transaction_count = 0
            alert_count = 0
            if transaction_files:
                transactions = prepare_transactions(
                    spark.read.parquet(*transaction_files), load_id
                ).persist(StorageLevel.DISK_ONLY)
                try:
                    transaction_count = transactions.count()
                    if transaction_count < config.expected_min_records:
                        raise RuntimeError(
                            "Silver data is not ready: expected at least "
                            f"{config.expected_min_records} new records, found "
                            f"{transaction_count}. Airflow can safely retry this load."
                        )
                    write_stage(transactions, "loading.transactions_stage", config)
                finally:
                    transactions.unpersist()
            elif config.expected_min_records:
                raise RuntimeError(
                    "Silver data is not ready: expected at least "
                    f"{config.expected_min_records} new records, found 0. "
                    "Airflow can safely retry this load."
                )
            if alert_files:
                alerts = prepare_alerts(
                    spark.read.parquet(*alert_files), load_id
                ).persist(StorageLevel.DISK_ONLY)
                try:
                    alert_count = alerts.count()
                    write_stage(alerts, "loading.fraud_alerts_stage", config)
                finally:
                    alerts.unpersist()
            counts = {
                "files_discovered": len(transaction_candidates) + len(alert_candidates),
                "records_read": transaction_count,
                "alerts_read": alert_count,
            }
            inserted, alerts_inserted = promote(
                connection, load_id, transaction_files, alert_files, counts
            )
            print(
                json.dumps(
                    {
                        "load_id": str(load_id),
                        "records_read": transaction_count,
                        "records_inserted": inserted,
                        "records_duplicated": transaction_count - inserted,
                        "alerts_read": alert_count,
                        "alerts_inserted": alerts_inserted,
                    }
                )
            )
    except Exception as error:
        record_failure(config, load_id, error)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Silver load failed: {error}", file=sys.stderr)
        raise
