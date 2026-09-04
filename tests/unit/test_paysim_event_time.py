from datetime import UTC, datetime

import pytest

from fraud_streaming_pipeline.sources.paysim import PaySimReader

HEADER = (
    "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,"
    "oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud\n"
)


def write_csv(path, *rows: str) -> None:
    path.write_text(HEADER + "".join(rows), encoding="utf-8")


def test_event_time_is_derived_from_simulation_start(tmp_path) -> None:
    csv_path = tmp_path / "paysim.csv"
    write_csv(csv_path, "3,PAYMENT,100,C100,1000,900,M200,0,0,0,0\n")
    simulation_start = datetime(2023, 1, 1, tzinfo=UTC)

    _, transaction = next(
        PaySimReader(csv_path, simulation_start=simulation_start).read()
    )

    assert transaction.event_time == datetime(2023, 1, 1, 2, tzinfo=UTC)


def test_reader_respects_checkpoint_and_limit(tmp_path) -> None:
    path = tmp_path / "paysim.csv"
    write_csv(
        path,
        "1,PAYMENT,10,C100,100,90,M200,0,0,0,0\n",
        "2,TRANSFER,20,C101,100,80,C201,0,20,0,0\n",
        "3,CASH_OUT,30,C102,100,70,C202,0,30,0,0\n",
    )

    records = list(PaySimReader(path).read(start_after=1, limit=1))

    assert len(records) == 1
    assert records[0][0] == 2
    assert records[0][1].source_record_id == "paysim1:2"


def test_event_id_is_deterministic_across_reprocessing(tmp_path) -> None:
    path = tmp_path / "paysim.csv"
    write_csv(path, "1,PAYMENT,10,C100,100,90,M200,0,0,0,0\n")
    reader = PaySimReader(path)

    first = next(reader.read())[1]
    second = next(reader.read())[1]

    assert first.event_id == second.event_id


@pytest.mark.parametrize(
    ("start_after", "limit", "message"),
    [(-1, None, "start_after"), (0, 0, "limit")],
)
def test_reader_rejects_invalid_parameters(
    tmp_path, start_after, limit, message
) -> None:
    path = tmp_path / "paysim.csv"
    write_csv(path)

    with pytest.raises(ValueError, match=message):
        list(PaySimReader(path).read(start_after=start_after, limit=limit))


def test_reader_rejects_missing_or_directory_path(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="not found"):
        list(PaySimReader(tmp_path / "missing.csv").read())

    with pytest.raises(ValueError, match="not a file"):
        list(PaySimReader(tmp_path).read())


def test_reader_rejects_missing_columns(tmp_path) -> None:
    path = tmp_path / "paysim.csv"
    path.write_text("step,type\n1,PAYMENT\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required PaySim columns"):
        list(PaySimReader(path).read())


def test_reader_reports_invalid_csv_line(tmp_path) -> None:
    path = tmp_path / "paysim.csv"
    write_csv(path, "1,PAYMENT,-10,C100,100,90,M200,0,0,0,0\n")

    with pytest.raises(ValueError, match="CSV line 2"):
        list(PaySimReader(path).read())
