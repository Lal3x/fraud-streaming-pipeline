"""Contrato comum para destinos de publicação de eventos."""

from typing import Protocol

from fraud_streaming_pipeline.models.transaction import (
    TransactionEvent,
)


class EventPublisher(Protocol):
    """Contrato implementado por destinos como console e Kafka."""

    def publish(self, event: TransactionEvent) -> None:
        """Publica um evento e retorna somente após a confirmação do destino."""
        ...

    def close(self) -> None:
        """Finaliza publicações pendentes e libera recursos externos."""
        ...
