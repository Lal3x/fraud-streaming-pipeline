"""Persiste eventos Kafka na camada Bronze com Spark Structured Streaming."""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery


def configure_s3a(spark: SparkSession, output_path: str) -> None:
    """Configura o acesso S3A somente quando a saída aponta para S3/MinIO."""
    if not output_path.startswith("s3a://"):
        return

    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    if not endpoint or not access_key or not secret_key:
        raise ValueError(
            "MINIO_ENDPOINT, MINIO_ROOT_USER and MINIO_ROOT_PASSWORD are required"
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


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
    """Cria a fonte contínua Kafka com os limites definidos no ambiente."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("includeHeaders", "true")
        .load()
    )


def to_bronze(kafka_events: DataFrame) -> DataFrame:
    """Preserva payload e metadados Kafka para auditoria e reprocessamento."""
    bronze_events = kafka_events.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("headers").alias("kafka_headers"),
        F.current_timestamp().alias("ingested_at"),
        F.current_date().alias("ingestion_date"),
    )

    return (
        bronze_events.withColumn("year", F.date_format("ingested_at", "yyyy"))
        .withColumn("month", F.date_format("ingested_at", "MM"))
        .withColumn("day", F.date_format("ingested_at", "dd"))
        .withColumn("hour", F.date_format("ingested_at", "HH"))
    )


def write_bronze_stream(
    bronze_events: DataFrame,
    output_path: str,
    checkpoint_path: str,
) -> StreamingQuery:
    """Grava a Bronze em Parquet com checkpoint independente da saída."""
    return (
        bronze_events.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .option("compression", "snappy")
        .partitionBy("year", "month", "day", "hour")
        .trigger(processingTime="5 seconds")
        .start()
    )


def main() -> None:
    """Inicializa a sessão Spark e mantém a ingestão ativa até seu encerramento."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "fraud-transactions-raw")
    output_path = os.getenv(
        "BRONZE_OUTPUT_PATH",
        "s3a://fraud-data-lake/bronze/paysim/events",
    )
    checkpoint_path = os.getenv(
        "BRONZE_CHECKPOINT_PATH",
        "/opt/spark/project/checkpoints/bronze-paysim-events-v3",
    )

    spark = (
        SparkSession.builder.appName("fraud-transactions-bronze")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_s3a(spark, output_path)

    kafka_events = read_kafka_stream(
        spark=spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )
    query = write_bronze_stream(
        bronze_events=to_bronze(kafka_events),
        output_path=output_path,
        checkpoint_path=checkpoint_path,
    )

    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
