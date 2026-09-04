"""Contrato tipado dos eventos publicados pelo produtor."""

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class TransactionType(StrEnum):
    """Tipos de operação disponíveis na base PaySim."""

    CASH_IN = "CASH_IN"
    CASH_OUT = "CASH_OUT"
    DEBIT = "DEBIT"
    PAYMENT = "PAYMENT"
    TRANSFER = "TRANSFER"


class TransactionEvent(BaseModel):
    """Evento canônico validado antes de entrar no Kafka."""

    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    schema_version: Literal["1.0"] = "1.0"
    event_id: UUID
    source_record_id: str
    produced_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    event_time: datetime
    source: Literal["paysim"] = "paysim"

    step: int = Field(ge=1)
    transaction_type: TransactionType = Field(alias="type")
    amount: Decimal = Field(ge=0)

    origin_account: str = Field(
        alias="nameOrig",
        pattern=r"^C\d+$",
    )
    origin_old_balance: Decimal = Field(
        alias="oldbalanceOrg",
        ge=0,
    )
    origin_new_balance: Decimal = Field(
        alias="newbalanceOrig",
        ge=0,
    )

    destination_account: str = Field(
        alias="nameDest",
        pattern=r"^[CM]\d+$",
    )
    destination_old_balance: Decimal = Field(
        alias="oldbalanceDest",
        ge=0,
    )
    destination_new_balance: Decimal = Field(
        alias="newbalanceDest",
        ge=0,
    )

    is_fraud: bool = Field(alias="isFraud")
    is_flagged_fraud: bool = Field(alias="isFlaggedFraud")
