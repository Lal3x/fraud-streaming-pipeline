"""Persistência local do progresso do produtor PaySim."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class ProducerCheckpoint(BaseModel):
    """Representa o último registro publicado com sucesso."""

    dataset: Literal["paysim1"] = "paysim1"
    last_published_record: int = Field(default=0, ge=0)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CheckpointStore:
    """Lê e grava o checkpoint em JSON de maneira atômica."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path

    def load(self) -> ProducerCheckpoint:
        """Carrega o progresso salvo ou retorna o estado inicial."""
        if not self.file_path.exists():
            return ProducerCheckpoint()

        content = self.file_path.read_text(encoding="utf-8")
        return ProducerCheckpoint.model_validate_json(content)

    def save(self, record_number: int) -> None:
        """Persiste o registro confirmado sem deixar um JSON parcial."""
        checkpoint = ProducerCheckpoint(
            last_published_record=record_number,
            updated_at=datetime.now(UTC),
        )

        self.file_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # Primeiro escreve em arquivo temporário; replace é atômico no mesmo disco.
        temporary_file = self.file_path.with_suffix(".tmp")

        temporary_file.write_text(
            checkpoint.model_dump_json(indent=2),
            encoding="utf-8",
        )

        temporary_file.replace(self.file_path)
