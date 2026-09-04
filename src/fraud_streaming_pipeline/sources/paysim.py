"""Leitura incremental e validação da base PaySim."""

import csv
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from fraud_streaming_pipeline.models.transaction import (
    TransactionEvent,
)


class PaySimReader:
    """Transforma linhas do CSV em eventos validados sem carregar toda a base."""

    DATASET_ID = "paysim1"

    REQUIRED_COLUMNS: ClassVar[set[str]] = {
        "step",
        "type",
        "amount",
        "nameOrig",
        "oldbalanceOrg",
        "newbalanceOrig",
        "nameDest",
        "oldbalanceDest",
        "newbalanceDest",
        "isFraud",
        "isFlaggedFraud",
    }

    def __init__(
        self,
        file_path: Path,
        simulation_start: datetime = datetime(2023, 1, 1, tzinfo=UTC),
    ) -> None:
        self.file_path = file_path
        self.simulation_start = simulation_start

    def read(
        self,
        start_after: int = 0,
        limit: int | None = None,
    ) -> Iterator[tuple[int, TransactionEvent]]:
        """Percorre o CSV sob demanda, respeitando checkpoint e limite do lote."""
        self._validate_parameters(
            start_after=start_after,
            limit=limit,
        )
        self._validate_file()

        published_in_execution = 0

        with self.file_path.open(
            mode="r",
            encoding="utf-8",
            newline="",
        ) as csv_file:
            reader = csv.DictReader(csv_file)

            self._validate_header(reader.fieldnames)

            for record_number, row in enumerate(reader, start=1):
                if record_number <= start_after:
                    continue

                if limit is not None and published_in_execution >= limit:
                    break

                event = self._create_event(
                    row=row,
                    record_number=record_number,
                )

                published_in_execution += 1

                yield record_number, event

    def _create_event(
        self,
        row: dict[str, str],
        record_number: int,
    ) -> TransactionEvent:
        """Converte uma linha PaySim em um evento determinístico e tipado."""
        source_record_id = f"{self.DATASET_ID}:{record_number}"

        # UUID5 garante o mesmo event_id em reprocessamentos da mesma linha.
        event_data = {
            **row,
            "source_record_id": source_record_id,
            "event_id": uuid5(
                NAMESPACE_URL,
                source_record_id,
            ),
            "event_time": self.simulation_start + timedelta(hours=int(row["step"]) - 1),
        }

        try:
            return TransactionEvent.model_validate(event_data)
        except ValidationError as error:
            csv_line = record_number + 1

            raise ValueError(f"Invalid transaction on CSV line {csv_line}") from error

    def _validate_file(self) -> None:
        """Garante que o caminho de entrada existe e representa um arquivo."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"PaySim file not found: {self.file_path}")

        if not self.file_path.is_file():
            raise ValueError(f"PaySim path is not a file: {self.file_path}")

    def _validate_header(
        self,
        fieldnames: Sequence[str] | None,
    ) -> None:
        """Verifica se o CSV contém todas as colunas exigidas pelo contrato."""
        available_columns = set(fieldnames or [])

        missing_columns = self.REQUIRED_COLUMNS - available_columns

        if missing_columns:
            formatted_columns = ", ".join(sorted(missing_columns))

            raise ValueError(f"Missing required PaySim columns: {formatted_columns}")

    @staticmethod
    def _validate_parameters(
        start_after: int,
        limit: int | None,
    ) -> None:
        """Rejeita posições e limites incompatíveis com a leitura incremental."""
        if start_after < 0:
            raise ValueError("start_after cannot be negative")

        if limit is not None and limit <= 0:
            raise ValueError("limit must be greater than zero")
