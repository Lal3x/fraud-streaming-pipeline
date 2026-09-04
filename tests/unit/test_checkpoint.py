from datetime import UTC

from fraud_streaming_pipeline.state.checkpoint import CheckpointStore


def test_load_returns_initial_checkpoint_when_file_does_not_exist(tmp_path) -> None:
    checkpoint = CheckpointStore(tmp_path / "checkpoint.json").load()

    assert checkpoint.dataset == "paysim1"
    assert checkpoint.last_published_record == 0
    assert checkpoint.updated_at.tzinfo == UTC


def test_save_persists_and_replaces_checkpoint_atomically(tmp_path) -> None:
    path = tmp_path / "nested" / "checkpoint.json"
    store = CheckpointStore(path)

    store.save(10)
    first_update = store.load().updated_at
    store.save(25)
    checkpoint = store.load()

    assert checkpoint.last_published_record == 25
    assert checkpoint.updated_at >= first_update
    assert not path.with_suffix(".tmp").exists()


def test_load_rejects_invalid_checkpoint(tmp_path) -> None:
    path = tmp_path / "checkpoint.json"
    path.write_text('{"last_published_record": -1}', encoding="utf-8")

    store = CheckpointStore(path)

    try:
        store.load()
    except ValueError as error:
        assert "last_published_record" in str(error)
    else:
        raise AssertionError("Checkpoint inválido deveria ser rejeitado")
