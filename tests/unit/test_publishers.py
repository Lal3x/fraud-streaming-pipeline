from datetime import UTC, datetime
from decimal import Decimal
from io import StringIO
from uuid import UUID

import pytest

from fraud_streaming_pipeline.models.transaction import TransactionEvent
from fraud_streaming_pipeline.publishers.console import ConsolePublisher
from fraud_streaming_pipeline.publishers.kafka import KafkaPublisher


def transaction() -> TransactionEvent:
    return TransactionEvent.model_validate(
        {
            "event_id": UUID("12345678-1234-5678-1234-567812345678"),
            "source_record_id": "paysim1:1",
            "produced_at": datetime(2026, 1, 1, tzinfo=UTC),
            "event_time": datetime(2026, 1, 1, tzinfo=UTC),
            "step": 1,
            "type": "PAYMENT",
            "amount": Decimal("10.50"),
            "nameOrig": "C100",
            "oldbalanceOrg": Decimal(100),
            "newbalanceOrig": Decimal("89.50"),
            "nameDest": "M200",
            "oldbalanceDest": 0,
            "newbalanceDest": 0,
            "isFraud": False,
            "isFlaggedFraud": False,
        }
    )


def test_console_publisher_writes_one_json_line() -> None:
    output = StringIO()
    publisher = ConsolePublisher(output)

    publisher.publish(transaction())
    publisher.close()

    assert output.getvalue().count("\n") == 1
    assert '"source_record_id":"paysim1:1"' in output.getvalue()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"bootstrap_servers": " "}, "bootstrap_servers"),
        ({"topic": " "}, "topic"),
        ({"delivery_timeout": 0}, "delivery_timeout"),
    ],
)
def test_kafka_publisher_validates_configuration(kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        KafkaPublisher(**kwargs)


class FakeProducer:
    def __init__(self, config, *, delivery_error=None, remaining=0, confirm=True):
        self.config = config
        self.delivery_error = delivery_error
        self.remaining = remaining
        self.confirm = confirm
        self.message = None

    def produce(self, **kwargs) -> None:
        self.message = kwargs
        if self.confirm:
            kwargs["on_delivery"](self.delivery_error, object())

    def flush(self, _timeout) -> int:
        return self.remaining


def test_kafka_publisher_sends_json_with_account_as_key(monkeypatch) -> None:
    fake = FakeProducer({})
    monkeypatch.setattr(
        "fraud_streaming_pipeline.publishers.kafka.Producer", lambda config: fake
    )

    with KafkaPublisher(producer_config={"client.id": "test"}) as publisher:
        publisher.publish(transaction())

    assert fake.config == {}
    assert fake.message["topic"] == "fraud-transactions-raw"
    assert fake.message["key"] == b"C100"
    assert fake.message["headers"] == {"content-type": "application/json"}


def test_kafka_publisher_raises_on_timeout(monkeypatch) -> None:
    fake = FakeProducer({}, remaining=1, confirm=False)
    monkeypatch.setattr(
        "fraud_streaming_pipeline.publishers.kafka.Producer", lambda _config: fake
    )
    publisher = KafkaPublisher()

    with pytest.raises(TimeoutError, match="confirm"):
        publisher.publish(transaction())


def test_kafka_publisher_raises_delivery_error(monkeypatch) -> None:
    error = object()
    fake = FakeProducer({}, delivery_error=error)
    monkeypatch.setattr(
        "fraud_streaming_pipeline.publishers.kafka.Producer", lambda _config: fake
    )
    publisher = KafkaPublisher()
    monkeypatch.setattr(
        "fraud_streaming_pipeline.publishers.kafka.KafkaException",
        lambda received: RuntimeError(str(received)),
    )

    with pytest.raises(RuntimeError):
        publisher.publish(transaction())


def test_kafka_close_rejects_pending_messages(monkeypatch) -> None:
    fake = FakeProducer({}, remaining=2)
    monkeypatch.setattr(
        "fraud_streaming_pipeline.publishers.kafka.Producer", lambda _config: fake
    )

    with pytest.raises(TimeoutError, match="2 message"):
        KafkaPublisher().close()
