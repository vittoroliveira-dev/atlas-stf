"""Tests for _checkpoint: save/load/roundtrip."""

from atlas_stf.scraper._checkpoint import load_checkpoint, mark_partition_complete, save_checkpoint
from atlas_stf.scraper._config import CheckpointState


class TestCheckpoint:
    def test_load_missing(self, tmp_path) -> None:
        assert load_checkpoint(tmp_path) is None

    def test_roundtrip(self, tmp_path) -> None:
        state = CheckpointState(
            target_base="decisoes",
            current_partition="2024-01",
            search_after=["2024-01-15", 12345],
            partition_doc_count=500,
            total_doc_count=1500,
            completed_partitions=["2023-12"],
        )
        save_checkpoint(state, tmp_path)
        loaded = load_checkpoint(tmp_path)
        assert loaded is not None
        assert loaded.target_base == "decisoes"
        assert loaded.current_partition == "2024-01"
        assert loaded.search_after == ["2024-01-15", 12345]
        assert loaded.partition_doc_count == 500
        assert loaded.total_doc_count == 1500
        assert loaded.completed_partitions == ["2023-12"]
        assert loaded.last_updated != ""

    def test_atomic_write(self, tmp_path) -> None:
        state = CheckpointState(target_base="decisoes", current_partition="2024-01")
        save_checkpoint(state, tmp_path)
        # No .tmp file should remain
        assert not (tmp_path / "_checkpoint.json.tmp").exists()
        assert (tmp_path / "_checkpoint.json").exists()

    def test_mark_partition_complete(self) -> None:
        state = CheckpointState(
            target_base="decisoes",
            current_partition="2024-01",
            search_after=["2024-01-31", 99999],
            partition_doc_count=800,
        )
        mark_partition_complete(state, "2024-01")
        assert "2024-01" in state.completed_partitions
        assert state.search_after is None
        assert state.partition_doc_count == 0

    def test_mark_idempotent(self) -> None:
        state = CheckpointState(target_base="decisoes", current_partition="2024-01")
        mark_partition_complete(state, "2024-01")
        mark_partition_complete(state, "2024-01")
        assert state.completed_partitions.count("2024-01") == 1
