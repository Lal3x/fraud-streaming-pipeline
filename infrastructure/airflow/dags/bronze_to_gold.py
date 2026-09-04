"""Orquestra o pipeline contínuo da publicação até a Gold e o modelo de ML.

As validações entre tarefas evitam propagar uma carga parcial quando algum
contêiner termina sem produzir o resultado esperado.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta

import docker
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param, get_current_context, task
from docker.types import Mount

DAG_ID = "fraud_bronze_to_gold"
DOCKER_URL = "unix://var/run/docker.sock"


def required_env(name: str) -> str:
    """Obtém uma variável obrigatória e interrompe a DAG se estiver vazia."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"A variável de ambiente {name} é obrigatória")
    return value


ANALYTICS_ENVIRONMENT = {
    "ANALYTICS_DB_HOST": "host.docker.internal",
    "ANALYTICS_DB_PORT": required_env("ANALYTICS_DB_PORT"),
    "ANALYTICS_DB_NAME": required_env("ANALYTICS_DB_NAME"),
    "ANALYTICS_DB_USER": required_env("ANALYTICS_DB_USER"),
}


with DAG(
    dag_id=DAG_ID,
    description="Consolida Bronze e Silver no PostgreSQL e materializa a Gold com dbt",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "producer_limit": Param(
            10000,
            type="integer",
            minimum=1,
            maximum=100000,
            title="Quantidade de eventos",
            section="Producer",
        ),
        "events_per_second": Param(
            100.0,
            type="number",
            exclusiveMinimum=0,
            maximum=10000,
            title="Eventos por segundo",
            section="Producer",
        ),
        "stream_timeout_seconds": Param(
            120,
            type="integer",
            minimum=10,
            maximum=1800,
            title="Timeout dos streams (segundos)",
            section="Streaming",
        ),
        "bronze_settle_seconds": Param(
            20,
            type="integer",
            minimum=0,
            maximum=300,
            title="Espera da Bronze (segundos)",
            section="Streaming",
        ),
        "silver_settle_seconds": Param(
            120,
            type="integer",
            minimum=0,
            maximum=300,
            title="Espera da Silver (segundos)",
            section="Streaming",
        ),
        "dbt_select": Param(
            "+path:models/marts",
            type="string",
            minLength=1,
            maxLength=200,
            pattern=r"^[A-Za-z0-9_+.:,/* -]+$",
            title="Seleção dbt",
            description="Seleção dbt executada com `dbt build --select`.",
            section="Gold",
        ),
        "full_refresh": Param(
            False,
            type="boolean",
            title="Reconstruir modelos incrementais",
            section="Gold",
        ),
    },
    tags=["fraud", "bronze", "silver", "gold", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task(task_id="run_producer")
    def run_producer() -> None:
        """Publica o lote solicitado e exige avanço real do checkpoint."""
        """Publica um lote parametrizado do PaySim no Kafka."""
        params = get_current_context()["params"]
        from fraud_streaming_pipeline.main import main as producer_main

        producer_main(
            [
                "--publisher",
                "kafka",
                "--limit",
                str(params["producer_limit"]),
                "--events-per-second",
                str(params["events_per_second"]),
            ]
        )

    def wait_for_container(container_name: str, settle_param: str) -> str:
        """Aguarda um serviço de streaming ficar ativo e estável."""
        """Espera um processador contínuo ficar ativo e concluir micro-batches."""
        params = get_current_context()["params"]

        timeout = int(params["stream_timeout_seconds"])
        deadline = time.monotonic() + timeout
        client = docker.DockerClient(base_url=DOCKER_URL)
        status = "not_found"

        try:
            while time.monotonic() < deadline:
                try:
                    container = client.containers.get(container_name)
                    container.reload()
                    status = container.status
                except docker.errors.NotFound:
                    status = "not_found"

                if status == "running":
                    settle_seconds = int(params[settle_param])
                    if settle_seconds:
                        time.sleep(settle_seconds)
                    return status
                time.sleep(5)
        finally:
            client.close()

        raise RuntimeError(
            f"Container {container_name} não ficou ativo em {timeout}s. "
            f"Estado: {status}"
        )

    @task(task_id="ingest_bronze")
    def ingest_bronze() -> str:
        """Verifica se a ingestão Bronze continua saudável após a publicação."""
        return wait_for_container("fraud-bronze-stream", "bronze_settle_seconds")

    @task(task_id="transform_silver")
    def transform_silver() -> str:
        """Verifica se o processamento Silver continua saudável."""
        return wait_for_container("fraud-silver-stream", "silver_settle_seconds")

    load_silver = DockerOperator(
        task_id="load_postgres",
        image="fraud-silver-loader:local",
        docker_url=DOCKER_URL,
        api_version="auto",
        command=[
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[2]",
            "--conf",
            "spark.jars.ivy=/tmp/.ivy2",
            "--conf",
            "spark.driver.memory=768m",
            "--conf",
            "spark.sql.shuffle.partitions=2",
            "--packages",
            "org.apache.hadoop:hadoop-aws:3.4.2,org.postgresql:postgresql:42.7.13",
            "/opt/spark/project/src/fraud_streaming_pipeline/etl/loading/job.py",
        ],
        environment={
            **ANALYTICS_ENVIRONMENT,
            "MINIO_ENDPOINT": "http://host.docker.internal:9000",
            "MINIO_ACCESS_KEY": required_env("MINIO_ACCESS_KEY"),
            "SILVER_TRANSACTIONS_PATH": required_env("SILVER_TRANSACTIONS_PATH"),
            "SILVER_ALERTS_PATH": required_env("SILVER_ALERTS_PATH"),
            "EXPECTED_MIN_RECORDS": "{{ params.producer_limit }}",
            "PYTHONPATH": "/opt/spark/project/src",
        },
        private_environment={
            "ANALYTICS_DB_PASSWORD": required_env("ANALYTICS_DB_PASSWORD"),
            "MINIO_SECRET_KEY": required_env("MINIO_SECRET_KEY"),
        },
        mounts=[
            Mount(
                source="fraud-analytics-loader-ivy",
                target="/tmp/.ivy2",
                type="volume",
            )
        ],
        extra_hosts={"host.docker.internal": "host-gateway"},
        mount_tmp_dir=False,
        mem_limit="1200m",
        auto_remove="success",
        force_pull=False,
        execution_timeout=timedelta(hours=1),
    )

    build_gold = DockerOperator(
        task_id="dbt_build_gold",
        image="fraud-dbt-gold:local",
        docker_url=DOCKER_URL,
        api_version="auto",
        entrypoint="/bin/bash",
        command=(
            "-ceu 'args=(dbt build --project-dir /opt/dbt/fraud_analytics "
            '--profiles-dir /opt/dbt/fraud_analytics --select "$DBT_SELECT"); '
            'if [[ "$DBT_FULL_REFRESH" == "true" ]]; then '
            'args+=(--full-refresh); fi; exec "${args[@]}"\''
        ),
        environment={
            **ANALYTICS_ENVIRONMENT,
            "DBT_SELECT": "{{ params.dbt_select }}",
            "DBT_FULL_REFRESH": "{{ params.full_refresh | lower }}",
        },
        private_environment={
            "ANALYTICS_DB_PASSWORD": required_env("ANALYTICS_DB_PASSWORD"),
        },
        extra_hosts={"host.docker.internal": "host-gateway"},
        mount_tmp_dir=False,
        mem_limit="768m",
        auto_remove="success",
        force_pull=False,
        execution_timeout=timedelta(minutes=30),
    )

    run_isolation_forest = DockerOperator(
        task_id="run_isolation_forest",
        image="fraud-isolation-forest:local",
        docker_url=DOCKER_URL,
        api_version="auto",
        environment={
            **ANALYTICS_ENVIRONMENT,
            "ML_TRAIN_FRACTION": "0.8",
            "ML_CONTAMINATION": "0.01",
            "ML_RANDOM_STATE": "42",
            "ML_MIN_TRAIN_ROWS": "100",
            "ML_MAX_ROWS": "100000",
            "ML_N_ESTIMATORS": "100",
            "ML_N_JOBS": "2",
            "ML_PERSIST_BATCH_SIZE": "5000",
        },
        private_environment={
            "ANALYTICS_DB_PASSWORD": required_env("ANALYTICS_DB_PASSWORD"),
        },
        extra_hosts={"host.docker.internal": "host-gateway"},
        mount_tmp_dir=False,
        mem_limit="1200m",
        auto_remove="success",
        force_pull=False,
        execution_timeout=timedelta(minutes=30),
    )

    validate_gold = DockerOperator(
        task_id="validate_gold",
        image="fraud-dbt-gold:local",
        docker_url=DOCKER_URL,
        api_version="auto",
        entrypoint="dbt",
        command=[
            "test",
            "--project-dir",
            "/opt/dbt/fraud_analytics",
            "--profiles-dir",
            "/opt/dbt/fraud_analytics",
            "--select",
            "{{ params.dbt_select }}",
        ],
        environment=ANALYTICS_ENVIRONMENT,
        private_environment={
            "ANALYTICS_DB_PASSWORD": required_env("ANALYTICS_DB_PASSWORD"),
        },
        extra_hosts={"host.docker.internal": "host-gateway"},
        mount_tmp_dir=False,
        mem_limit="768m",
        auto_remove="success",
        force_pull=False,
        execution_timeout=timedelta(minutes=30),
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> run_producer()
        >> ingest_bronze()
        >> transform_silver()
        >> load_silver
        >> build_gold
        >> run_isolation_forest
        >> validate_gold
        >> finish
    )
