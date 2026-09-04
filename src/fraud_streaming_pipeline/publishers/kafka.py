"""Publicação confiável de eventos no Apache Kafka."""

from collections.abc import Mapping
from typing import Any, Self

from confluent_kafka import KafkaError, KafkaException, Producer

from fraud_streaming_pipeline.models.transaction import TransactionEvent


class KafkaPublisher:
    """Publica transações JSON em um tópico Kafka com confirmação de entrega."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "fraud-transactions-raw",
        producer_config: Mapping[str, Any] | None = None,
        delivery_timeout: float = 10.0,
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers cannot be empty")

        if not topic.strip():
            raise ValueError("topic cannot be empty")

        if delivery_timeout <= 0:
            raise ValueError("delivery_timeout must be greater than zero")

        config: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "fraud-transaction-producer",
            "acks": "all",
            "enable.idempotence": True,
        }
        config.update(producer_config or {})

        self._topic = topic
        self._delivery_timeout = delivery_timeout
        self._producer = Producer(config)

    def publish(self, event: TransactionEvent) -> None:
        """Serializa um evento e aguarda a confirmação do broker."""
        delivery_error: KafkaError | None = None
        delivered = False

        def on_delivery(error: KafkaError | None, _message: Any) -> None:
            nonlocal delivery_error, delivered
            delivery_error = error
            delivered = True

        # A conta de origem mantém eventos relacionados na mesma partição.
        self._producer.produce(
            topic=self._topic,
            key=event.origin_account.encode("utf-8"),
            value=event.model_dump_json().encode("utf-8"),
            headers={"content-type": "application/json"},
            on_delivery=on_delivery,
        )

        remaining = self._producer.flush(self._delivery_timeout)

        if remaining > 0 or not delivered:
            raise TimeoutError(
                "Kafka did not confirm message delivery within "
                f"{self._delivery_timeout} seconds"
            )

        if delivery_error is not None:
            raise KafkaException(delivery_error)

    def close(self) -> None:
        """Espera a entrega das mensagens que ainda estão na fila local."""
        remaining = self._producer.flush(self._delivery_timeout)

        if remaining > 0:
            raise TimeoutError(
                f"Kafka publisher closed with {remaining} message(s) pending"
            )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: object,
        _exc_value: object,
        _traceback: object,
    ) -> None:
        self.close()
