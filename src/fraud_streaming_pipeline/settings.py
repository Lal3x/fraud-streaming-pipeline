"""Configurações do produtor carregadas do ambiente de execução."""

from datetime import UTC, datetime
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Centraliza configurações vindas de variáveis de ambiente ou do `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "fraud-transactions-raw"
    kafka_client_id: str = "paysim-producer"

    paysim_file: Path = Path("data/raw/paysim.csv")
    producer_checkpoint: Path = Path("data/checkpoints/producer.json")

    producer_limit: int = Field(default=10, gt=0)
    events_per_second: float = Field(default=2.0, gt=0)
    paysim_simulation_start: datetime = datetime(2023, 1, 1, tzinfo=UTC)
