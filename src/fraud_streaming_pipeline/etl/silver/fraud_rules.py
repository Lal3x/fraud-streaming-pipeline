"""Regras explicáveis de risco aplicadas às transações válidas."""

import os
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class RiskConfig:
    """Limiares e pesos configuráveis usados no cálculo do risco."""

    high_amount_threshold: float = 100_000.0
    high_amount_weight: int = 40
    risky_type_weight: int = 25
    drained_account_weight: int = 20
    origin_anomaly_weight: int = 15
    medium_risk_score: int = 30
    high_risk_score: int = 70
    alert_score: int = 50

    @classmethod
    def from_env(cls) -> "RiskConfig":
        """Monta a configuração das regras a partir do ambiente."""
        return cls(
            high_amount_threshold=float(
                os.getenv("SILVER_HIGH_AMOUNT_THRESHOLD", "100000")
            ),
            high_amount_weight=int(os.getenv("SILVER_HIGH_AMOUNT_WEIGHT", "40")),
            risky_type_weight=int(os.getenv("SILVER_RISKY_TYPE_WEIGHT", "25")),
            drained_account_weight=int(
                os.getenv("SILVER_DRAINED_ACCOUNT_WEIGHT", "20")
            ),
            origin_anomaly_weight=int(os.getenv("SILVER_ORIGIN_ANOMALY_WEIGHT", "15")),
            medium_risk_score=int(os.getenv("SILVER_MEDIUM_RISK_SCORE", "30")),
            high_risk_score=int(os.getenv("SILVER_HIGH_RISK_SCORE", "70")),
            alert_score=int(os.getenv("SILVER_ALERT_SCORE", "50")),
        )


def _weighted(rule: Column, weight: int) -> Column:
    """Converte uma condição Spark em sua contribuição numérica para o risco."""
    return F.when(rule, F.lit(weight)).otherwise(F.lit(0))


def apply_fraud_rules(
    transactions: DataFrame,
    config: RiskConfig,
) -> DataFrame:
    """Calcula sinais, score e decisão de alerta de forma auditável."""
    """Apply explainable risk rules without using fraud label columns."""
    with_rules = (
        transactions.withColumn(
            "rule_high_amount",
            F.col("amount") >= config.high_amount_threshold,
        )
        .withColumn(
            "rule_risky_transaction_type",
            F.col("transaction_type").isin("TRANSFER", "CASH_OUT"),
        )
        .withColumn(
            "rule_origin_account_drained",
            F.col("is_origin_account_drained"),
        )
        .withColumn(
            "rule_origin_balance_anomaly",
            F.col("has_origin_balance_anomaly"),
        )
    )
    scored = with_rules.withColumn(
        "risk_score",
        _weighted(F.col("rule_high_amount"), config.high_amount_weight)
        + _weighted(
            F.col("rule_risky_transaction_type"),
            config.risky_type_weight,
        )
        + _weighted(
            F.col("rule_origin_account_drained"),
            config.drained_account_weight,
        )
        + _weighted(
            F.col("rule_origin_balance_anomaly"),
            config.origin_anomaly_weight,
        ),
    )

    return (
        scored.withColumn(
            "triggered_rules",
            F.filter(
                F.array(
                    F.when(F.col("rule_high_amount"), "HIGH_AMOUNT"),
                    F.when(
                        F.col("rule_risky_transaction_type"),
                        "RISKY_TRANSACTION_TYPE",
                    ),
                    F.when(
                        F.col("rule_origin_account_drained"),
                        "ORIGIN_ACCOUNT_DRAINED",
                    ),
                    F.when(
                        F.col("rule_origin_balance_anomaly"),
                        "ORIGIN_BALANCE_ANOMALY",
                    ),
                ),
                lambda rule: rule.isNotNull(),
            ),
        )
        .withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= config.high_risk_score, "HIGH")
            .when(F.col("risk_score") >= config.medium_risk_score, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn("predicted_fraud", F.col("risk_score") >= config.alert_score)
    )
