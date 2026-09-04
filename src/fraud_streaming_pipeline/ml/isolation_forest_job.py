"""Treinamento e persistência do detector de anomalias Isolation Forest."""

import json
import math
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

MODEL_TYPE = "IsolationForest"
CATEGORICAL_FEATURES = ("transaction_type",)
NUMERIC_FEATURES = (
    "amount",
    "log1p_amount",
    "event_hour",
    "origin_balance_change",
    "destination_balance_change",
    "origin_balance_difference",
    "has_origin_balance_anomaly",
    "has_destination_balance_anomaly",
    "recent_transaction_count",
    "recent_transaction_volume",
    "historical_log_amount_mean",
    "historical_log_amount_stddev",
    "historical_log_amount_median",
    "historical_log_amount_mad",
    "log_amount_zscore",
    "log_amount_robust_zscore",
)
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


@dataclass(frozen=True)
class IsolationForestConfig:
    """Configuração do modelo com limites explícitos de CPU e memória."""

    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    train_fraction: float = 0.8
    contamination: float = 0.01
    random_state: int = 42
    min_train_rows: int = 100
    max_rows: int = 100_000
    n_estimators: int = 100
    n_jobs: int = 2
    persist_batch_size: int = 5_000
    model_version: str | None = None

    @classmethod
    def from_env(cls) -> "IsolationForestConfig":
        """Carrega os parâmetros do modelo a partir do ambiente."""
        required = (
            "ANALYTICS_DB_HOST",
            "ANALYTICS_DB_PORT",
            "ANALYTICS_DB_NAME",
            "ANALYTICS_DB_USER",
            "ANALYTICS_DB_PASSWORD",
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
            train_fraction=float(os.getenv("ML_TRAIN_FRACTION", "0.8")),
            contamination=float(os.getenv("ML_CONTAMINATION", "0.01")),
            random_state=int(os.getenv("ML_RANDOM_STATE", "42")),
            min_train_rows=int(os.getenv("ML_MIN_TRAIN_ROWS", "100")),
            max_rows=int(os.getenv("ML_MAX_ROWS", "100000")),
            n_estimators=int(os.getenv("ML_N_ESTIMATORS", "100")),
            n_jobs=int(os.getenv("ML_N_JOBS", "2")),
            persist_batch_size=int(os.getenv("ML_PERSIST_BATCH_SIZE", "5000")),
            model_version=os.getenv("ML_MODEL_VERSION") or None,
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Rejeita parâmetros incompatíveis com treino e avaliação confiáveis."""
        if not 0 < self.train_fraction < 1:
            raise ValueError("ML_TRAIN_FRACTION must be between 0 and 1")
        if not 0 < self.contamination <= 0.5:
            raise ValueError("ML_CONTAMINATION must be greater than 0 and at most 0.5")
        if self.min_train_rows < 2:
            raise ValueError("ML_MIN_TRAIN_ROWS must be at least 2")
        if self.max_rows <= self.min_train_rows:
            raise ValueError("ML_MAX_ROWS must be greater than ML_MIN_TRAIN_ROWS")
        if self.n_estimators < 1:
            raise ValueError("ML_N_ESTIMATORS must be at least 1")
        if self.n_jobs < 1:
            raise ValueError("ML_N_JOBS must be at least 1")
        if self.persist_batch_size < 1:
            raise ValueError("ML_PERSIST_BATCH_SIZE must be at least 1")

    @property
    def dsn(self) -> str:
        """Monta a conexão com o banco analítico."""
        return (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_password}"
        )


def temporal_split_index(
    row_count: int,
    train_fraction: float,
    min_train_rows: int,
) -> int:
    """Calcula um corte cronológico que preserva ao menos uma linha de avaliação."""
    if row_count < min_train_rows + 1:
        raise ValueError(
            f"At least {min_train_rows + 1} feature rows are required; got {row_count}"
        )
    return min(
        max(math.ceil(row_count * train_fraction), min_train_rows), row_count - 1
    )


def load_features(connection: Connection[Any], max_rows: int) -> list[dict[str, Any]]:
    """Busca uma janela recente limitada para manter o consumo previsível."""
    columns = ", ".join(("event_id", "event_time", *FEATURES))
    rows = connection.execute(
        f"SELECT {columns} FROM ("
        f"SELECT {columns} FROM analytics.ml_transaction_features "
        "ORDER BY event_time DESC, event_id DESC LIMIT %s"
        ") AS recent ORDER BY event_time, event_id",
        (max_rows,),
    ).fetchall()
    names = ("event_id", "event_time", *FEATURES)
    return [dict(zip(names, row, strict=True)) for row in rows]


def feature_matrix(rows: Sequence[dict[str, Any]]) -> list[list[Any]]:
    """Organiza as features na ordem exigida pelo pipeline do modelo."""
    return [[row[name] for name in FEATURES] for row in rows]


def build_model(config: IsolationForestConfig) -> Any:
    """Cria pré-processamento e Isolation Forest como pipeline único."""
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import IsolationForest
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    numeric_indexes = list(range(len(NUMERIC_FEATURES)))
    categorical_indexes = list(range(len(NUMERIC_FEATURES), len(FEATURES)))
    preprocessor = ColumnTransformer(
        transformers=(
            (
                "numeric",
                Pipeline(
                    steps=(
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    )
                ),
                numeric_indexes,
            ),
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", sparse_output=True),
                categorical_indexes,
            ),
        )
    )
    return Pipeline(
        steps=(
            ("preprocessor", preprocessor),
            (
                "model",
                IsolationForest(
                    contamination=config.contamination,
                    random_state=config.random_state,
                    n_estimators=config.n_estimators,
                    n_jobs=config.n_jobs,
                ),
            ),
        )
    )


def create_run(
    connection: Connection[Any],
    run_id: UUID,
    model_version: str,
    config: IsolationForestConfig,
    started_at: datetime,
) -> None:
    connection.execute(
        """
        INSERT INTO monitoring.ml_model_runs (
            run_id, model_version, model_type, status, started_at,
            contamination, train_fraction, random_state, feature_names
        ) VALUES (%s, %s, %s, 'RUNNING', %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            model_version,
            MODEL_TYPE,
            started_at,
            config.contamination,
            config.train_fraction,
            config.random_state,
            Jsonb(list(FEATURES)),
        ),
    )
    connection.commit()


def persist_scores(
    connection: Connection[Any],
    rows: Sequence[dict[str, Any]],
    split_index: int,
    scores: Sequence[float],
    predictions: Sequence[int],
    run_id: UUID,
    model_version: str,
    scored_at: datetime,
    config: IsolationForestConfig,
) -> None:
    with connection.transaction():
        statement = """
        INSERT INTO analytics.transaction_anomaly_scores (
            event_id, model_version, dataset_split, anomaly_score,
            is_anomaly, scored_at
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id, model_version) DO UPDATE SET
            dataset_split = EXCLUDED.dataset_split,
            anomaly_score = EXCLUDED.anomaly_score,
            is_anomaly = EXCLUDED.is_anomaly,
            scored_at = EXCLUDED.scored_at
        """
        with connection.cursor() as cursor:
            for start in range(0, len(rows), config.persist_batch_size):
                end = min(start + config.persist_batch_size, len(rows))
                values = (
                    (
                        rows[index]["event_id"],
                        model_version,
                        "TRAIN" if index < split_index else "EVALUATION",
                        float(scores[index]),
                        bool(predictions[index] == -1),
                        scored_at,
                    )
                    for index in range(start, end)
                )
                cursor.executemany(statement, values)
        anomaly_count = int(sum(prediction == -1 for prediction in predictions))
        connection.execute(
            """
            UPDATE monitoring.ml_model_runs
            SET status = 'SUCCESS', finished_at = %s, training_cutoff = %s,
                train_row_count = %s, evaluation_row_count = %s,
                scored_row_count = %s, anomaly_row_count = %s
            WHERE run_id = %s
            """,
            (
                scored_at,
                rows[split_index - 1]["event_time"],
                split_index,
                len(rows) - split_index,
                len(rows),
                anomaly_count,
                run_id,
            ),
        )


def record_failure(connection: Connection[Any], run_id: UUID, error: Exception) -> None:
    """Marca a execução como falha e preserva a mensagem de diagnóstico."""
    connection.execute(
        """
        UPDATE monitoring.ml_model_runs
        SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP, error_message = %s
        WHERE run_id = %s
        """,
        (str(error)[:4000], run_id),
    )
    connection.commit()


def main() -> None:
    """Treina, avalia e publica scores de anomalia no banco analítico."""
    import numpy as np

    config = IsolationForestConfig.from_env()
    run_id = uuid4()
    # Keep timezone.utc because the container also supports Python 3.10.
    now = datetime.now(timezone.utc)  # noqa: UP017
    model_version = (
        config.model_version or f"iforest-{now:%Y%m%dT%H%M%SZ}-{str(run_id)[:8]}"
    )
    with psycopg.connect(config.dsn) as connection:
        create_run(connection, run_id, model_version, config, now)
        try:
            rows = load_features(connection, config.max_rows)
            split_index = temporal_split_index(
                len(rows), config.train_fraction, config.min_train_rows
            )
            matrix = np.asarray(feature_matrix(rows), dtype=object)
            model = build_model(config)
            model.fit(matrix[:split_index])
            anomaly_scores = -model.decision_function(matrix)
            predictions = model.predict(matrix)
            scored_at = datetime.now(timezone.utc)  # noqa: UP017
            persist_scores(
                connection,
                rows,
                split_index,
                anomaly_scores,
                predictions,
                run_id,
                model_version,
                scored_at,
                config,
            )
            print(
                json.dumps(
                    {
                        "run_id": str(run_id),
                        "model_version": model_version,
                        "train_rows": split_index,
                        "evaluation_rows": len(rows) - split_index,
                        "scored_rows": len(rows),
                        "anomalies": int(sum(predictions == -1)),
                    }
                )
            )
        except Exception as error:
            record_failure(connection, run_id, error)
            raise


if __name__ == "__main__":
    main()
