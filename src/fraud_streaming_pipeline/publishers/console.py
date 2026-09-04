"""Publicador simples usado para desenvolvimento e diagnóstico local."""

import sys
from typing import TextIO

from fraud_streaming_pipeline.models.transaction import (
    TransactionEvent,
)


class ConsolePublisher:
    """Publica eventos JSON em uma saída de texto, por padrão o terminal."""

    def __init__(
        self,
        output: TextIO | None = None,
    ) -> None:
        self._output = output if output is not None else sys.stdout

    def publish(self, event: TransactionEvent) -> None:
        """Serializa e escreve um evento, forçando o envio imediato do buffer."""
        message = event.model_dump_json()

        print(
            message,
            file=self._output,
            flush=True,
        )

    def close(self) -> None:
        """Finaliza sem fechar uma saída que pertence ao chamador."""
