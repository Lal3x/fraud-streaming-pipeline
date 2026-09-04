import argparse
from typing import ClassVar

import pytest

from fraud_streaming_pipeline import main as producer_main
from fraud_streaming_pipeline.settings import Settings


@pytest.mark.parametrize("value", ["0", "-1"])
def test_positive_int_rejects_non_positive_values(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        producer_main.positive_int(value)


@pytest.mark.parametrize("value", ["0", "-0.5"])
def test_positive_float_rejects_non_positive_values(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        producer_main.positive_float(value)


def test_positive_parsers_return_numbers() -> None:
    assert producer_main.positive_int("3") == 3
    assert producer_main.positive_float("2.5") == 2.5


def test_parser_uses_settings_as_defaults() -> None:
    settings = Settings(producer_limit=25, events_per_second=10)

    args = producer_main.create_parser(settings).parse_args(["--publisher", "console"])

    assert args.publisher == "console"
    assert args.limit == 25
    assert args.events_per_second == 10


class FakeCheckpoint:
    last_published_record = 7


class FakeStore:
    saved: ClassVar[list[int]] = []

    def __init__(self, _path) -> None:
        pass

    def load(self):
        return FakeCheckpoint()

    def save(self, record_number: int) -> None:
        self.saved.append(record_number)


class FakeReader:
    arguments = None

    def __init__(self, _path, simulation_start) -> None:
        self.simulation_start = simulation_start

    def read(self, **kwargs):
        self.arguments = kwargs
        yield 8, object()
        yield 9, object()


class FakePublisher:
    published: ClassVar[list[object]] = []
    closed = False

    def __init__(self, *args, **kwargs) -> None:
        pass

    def publish(self, event) -> None:
        self.published.append(event)

    def close(self) -> None:
        type(self).closed = True


def test_main_resumes_checkpoint_and_publishes_requested_batch(
    monkeypatch, capsys
) -> None:
    FakeStore.saved = []
    FakePublisher.published = []
    FakePublisher.closed = False
    reader = FakeReader(None, None)
    monkeypatch.setattr(producer_main, "Settings", Settings)
    monkeypatch.setattr(producer_main, "CheckpointStore", FakeStore)
    monkeypatch.setattr(producer_main, "PaySimReader", lambda *args, **kwargs: reader)
    monkeypatch.setattr(producer_main, "ConsolePublisher", FakePublisher)
    monkeypatch.setattr(producer_main, "sleep", lambda _seconds: None)

    producer_main.main(
        ["--publisher", "console", "--limit", "2", "--events-per-second", "100"]
    )

    assert reader.arguments == {"start_after": 7, "limit": 2}
    assert FakeStore.saved == [8, 9]
    assert len(FakePublisher.published) == 2
    assert FakePublisher.closed is True
    assert "Published 2 PaySim transaction(s) to console" in capsys.readouterr().out
