"""Ponto de entrada do produtor que simula transações PaySim em streaming."""

import argparse
from collections.abc import Sequence
from contextlib import closing
from time import monotonic, sleep

from fraud_streaming_pipeline.publishers.console import ConsolePublisher
from fraud_streaming_pipeline.publishers.kafka import KafkaPublisher
from fraud_streaming_pipeline.settings import Settings
from fraud_streaming_pipeline.sources.paysim import PaySimReader
from fraud_streaming_pipeline.state.checkpoint import CheckpointStore


def positive_int(value: str) -> int:
    """Converte um argumento da CLI em inteiro estritamente positivo."""
    parsed_value = int(value)

    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")

    return parsed_value


def positive_float(value: str) -> float:
    """Converte um argumento da CLI em número estritamente positivo."""
    parsed_value = float(value)

    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")

    return parsed_value


def create_parser(settings: Settings) -> argparse.ArgumentParser:
    """Monta a interface de linha de comando com os valores padrão configurados."""
    parser = argparse.ArgumentParser(
        description="Publish PaySim transactions to Kafka or the console."
    )
    parser.add_argument(
        "--publisher",
        choices=("kafka", "console"),
        default="kafka",
        help="destination for transaction events (default: kafka)",
    )
    parser.add_argument(
        "--limit",
        type=positive_int,
        default=settings.producer_limit,
        help=(f"maximum events to publish (default: {settings.producer_limit})"),
    )
    parser.add_argument(
        "--events-per-second",
        type=positive_float,
        default=settings.events_per_second,
        help=(f"maximum publication rate (default: {settings.events_per_second:g})"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Publica um lote limitado e retoma do último registro confirmado."""
    settings = Settings()
    args = create_parser(settings).parse_args(argv)
    checkpoint_store = CheckpointStore(settings.producer_checkpoint)
    checkpoint = checkpoint_store.load()
    reader = PaySimReader(
        settings.paysim_file,
        simulation_start=settings.paysim_simulation_start,
    )

    published = 0
    interval_seconds = 1 / args.events_per_second
    last_published_at: float | None = None

    publisher: KafkaPublisher | ConsolePublisher

    if args.publisher == "kafka":
        publisher = KafkaPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            producer_config={"client.id": settings.kafka_client_id},
        )
    else:
        publisher = ConsolePublisher()

    # O checkpoint só avança depois que o destino confirma a publicação.
    # Assim uma interrupção não cria um salto silencioso na fonte.
    with closing(publisher):
        for record_number, event in reader.read(
            start_after=checkpoint.last_published_record,
            limit=args.limit,
        ):
            # Controla a vazão sem carregar o CSV inteiro na memória.
            if last_published_at is not None:
                elapsed = monotonic() - last_published_at
                sleep(max(0.0, interval_seconds - elapsed))

            publisher.publish(event)
            last_published_at = monotonic()
            checkpoint_store.save(record_number)
            published += 1

    print(f"Published {published} PaySim transaction(s) to {args.publisher}")


if __name__ == "__main__":
    main()
