"""Integration tests for write_stream_resilient — simulated server scenarios.

Tests three critical scenarios:
1. Stall (no bytes after N seconds)
2. Mid-stream disconnect after partial bytes
3. Server ignores Range and returns 200
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from atlas_stf.core.http_stream_safety import (
    get_part_resume_offset,
    write_stream_resilient,
)

# ---------------------------------------------------------------------------
# Fake stream helpers
# ---------------------------------------------------------------------------


class _FakeStream:
    """Simulates an HTTP stream response."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def iter_bytes(self) -> Iterator[bytes]:
        yield from self._chunks


class _StallStream:
    """Yields some chunks then yields empty bytes forever (simulates stall)."""

    def __init__(self, initial_chunks: list[bytes]) -> None:
        self._initial = initial_chunks

    def iter_bytes(self) -> Iterator[bytes]:
        yield from self._initial
        while True:
            yield b""


class _DisconnectStream:
    """Yields some chunks then raises ConnectionError (simulates network drop)."""

    def __init__(self, chunks: list[bytes], error_after: int) -> None:
        self._chunks = chunks
        self._error_after = error_after

    def iter_bytes(self) -> Iterator[bytes]:
        yielded = 0
        for chunk in self._chunks:
            yielded += len(chunk)
            if yielded > self._error_after:
                raise ConnectionError("Simulated network disconnect")
            yield chunk


# ---------------------------------------------------------------------------
# Scenario 1: Stall detection
# ---------------------------------------------------------------------------


class TestStallDetection:
    def test_stall_raises_timeout_and_preserves_part(self, tmp_path: Path) -> None:
        dest = tmp_path / "test.zip"
        stream = _StallStream([b"x" * 1000, b"y" * 500])

        with pytest.raises(TimeoutError, match="stalled"):
            write_stream_resilient(
                stream,
                dest,
                max_download_bytes=10_000_000,
                stall_timeout_seconds=0.1,
            )

        # .part preserved for resume
        part = dest.with_suffix(".zip.part")
        assert part.exists()
        assert part.stat().st_size == 1500

        # Progress marker written
        progress = part.with_suffix(".progress")
        assert progress.exists()
        data = json.loads(progress.read_text())
        assert data["bytes_written"] == 1500

    def test_stall_resume_offset_recoverable(self, tmp_path: Path) -> None:
        dest = tmp_path / "test.zip"
        stream = _StallStream([b"A" * 2000])

        with pytest.raises(TimeoutError):
            write_stream_resilient(
                stream,
                dest,
                max_download_bytes=10_000_000,
                stall_timeout_seconds=0.1,
            )

        offset = get_part_resume_offset(dest)
        assert offset == 2000


# ---------------------------------------------------------------------------
# Scenario 2: Mid-stream disconnect
# ---------------------------------------------------------------------------


class TestMidStreamDisconnect:
    def test_disconnect_preserves_part_with_progress(self, tmp_path: Path) -> None:
        dest = tmp_path / "test.zip"
        chunks = [b"A" * 1000, b"B" * 1000, b"C" * 1000]
        stream = _DisconnectStream(chunks, error_after=2500)

        with pytest.raises(ConnectionError):
            write_stream_resilient(
                stream,
                dest,
                max_download_bytes=10_000_000,
                remote_etag='"abc123"',
                remote_content_length=3000,
            )

        # .part should have the bytes written before disconnect
        part = dest.with_suffix(".zip.part")
        assert part.exists()
        assert part.stat().st_size >= 2000  # at least 2 chunks

        # Progress marker has remote metadata
        progress = part.with_suffix(".progress")
        assert progress.exists()
        data = json.loads(progress.read_text())
        assert data["etag"] == '"abc123"'
        assert data["content_length"] == 3000

    def test_resume_after_disconnect(self, tmp_path: Path) -> None:
        """After disconnect, resume offset should match partial file size."""
        dest = tmp_path / "test.zip"
        chunks = [b"X" * 500, b"Y" * 500, b"Z" * 500]
        stream = _DisconnectStream(chunks, error_after=1200)

        with pytest.raises(ConnectionError):
            write_stream_resilient(
                stream,
                dest,
                max_download_bytes=10_000_000,
                remote_etag='"etag1"',
                remote_content_length=1500,
            )

        offset = get_part_resume_offset(
            dest, expected_etag='"etag1"', expected_content_length=1500
        )
        assert offset > 0
        assert offset <= 1200

    def test_resume_with_changed_etag_discards_partial(self, tmp_path: Path) -> None:
        """If remote ETag changed, partial should be discarded."""
        dest = tmp_path / "test.zip"

        # Simulate a partial download
        part = dest.with_suffix(".zip.part")
        part.write_bytes(b"old data" * 100)
        progress = part.with_suffix(".progress")
        progress.write_text(json.dumps({
            "bytes_written": 800, "etag": '"old_etag"', "content_length": 2000,
        }))

        # Try resume with different etag — should discard
        offset = get_part_resume_offset(
            dest, expected_etag='"new_etag"', expected_content_length=2000
        )
        assert offset == 0
        assert not part.exists()


# ---------------------------------------------------------------------------
# Scenario 3: Server ignores Range (200 instead of 206)
# ---------------------------------------------------------------------------


class TestServerIgnoresRange:
    def test_full_download_on_200_produces_correct_file(self, tmp_path: Path) -> None:
        """When resume_offset is given but server returns full content (200),
        write_stream_resilient with resume_offset=0 should produce the full file."""
        dest = tmp_path / "test.zip"
        full_data = b"ABCDEFGHIJ" * 100

        # Simulate: we had a partial, but server returned 200 (full content).
        # Caller should set resume_offset=0 when detecting 200 status.
        stream = _FakeStream([full_data])
        actual = write_stream_resilient(
            stream,
            dest,
            max_download_bytes=10_000_000,
            resume_offset=0,  # caller detected 200, reset offset
        )

        assert actual == len(full_data)
        assert dest.exists()
        assert dest.read_bytes() == full_data

    def test_resume_correctly_appends(self, tmp_path: Path) -> None:
        """When server returns 206, the partial is appended correctly."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")

        # Write initial partial
        first_half = b"A" * 500
        part.write_bytes(first_half)

        # Resume with second half
        second_half = b"B" * 500
        stream = _FakeStream([second_half])
        actual = write_stream_resilient(
            stream,
            dest,
            max_download_bytes=10_000_000,
            resume_offset=500,
        )

        assert actual == 1000
        assert dest.exists()
        assert dest.read_bytes() == first_half + second_half


# ---------------------------------------------------------------------------
# Progress marker validation
# ---------------------------------------------------------------------------


class TestProgressValidation:
    def test_size_mismatch_discards(self, tmp_path: Path) -> None:
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        part.write_bytes(b"x" * 100)
        progress = part.with_suffix(".progress")
        progress.write_text(json.dumps({"bytes_written": 99999}))  # wild mismatch

        offset = get_part_resume_offset(dest)
        assert offset == 0
        assert not part.exists()

    def test_no_progress_trusts_file_size(self, tmp_path: Path) -> None:
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        part.write_bytes(b"x" * 1234)
        # No .progress file

        offset = get_part_resume_offset(dest)
        assert offset == 1234


# ---------------------------------------------------------------------------
# Adversarial: .progress corruption and .part divergence (CORE-03 hardening)
# ---------------------------------------------------------------------------


class TestProgressCorruption:
    """Exercises fail-safe behavior when .progress is corrupted or absent."""

    def test_invalid_json_falls_back_to_file_size(self, tmp_path: Path) -> None:
        """Corrupt JSON in .progress → treated as no marker, trust file size."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        part.write_bytes(b"x" * 5000)
        progress = part.with_suffix(".progress")
        progress.write_text("{truncated", encoding="utf-8")

        offset = get_part_resume_offset(dest)
        assert offset == 5000

    def test_empty_progress_file_falls_back_to_file_size(self, tmp_path: Path) -> None:
        """Empty .progress → JSON decode fails, trust file size."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        part.write_bytes(b"x" * 3000)
        progress = part.with_suffix(".progress")
        progress.write_text("", encoding="utf-8")

        offset = get_part_resume_offset(dest)
        assert offset == 3000

    def test_progress_missing_bytes_written_key(self, tmp_path: Path) -> None:
        """Valid JSON but missing bytes_written → recorded=0, discards."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        part.write_bytes(b"x" * 2000)
        progress = part.with_suffix(".progress")
        progress.write_text(json.dumps({"etag": '"abc"'}), encoding="utf-8")

        # recorded = data.get("bytes_written", 0) → 0
        # condition: recorded > 0 fails → falls to mismatch branch → discards
        offset = get_part_resume_offset(dest)
        assert offset == 0
        assert not part.exists()

    def test_corrupt_progress_with_content_length_check(self, tmp_path: Path) -> None:
        """Corrupt .progress + expected_content_length → discards if part >= expected."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        part.write_bytes(b"x" * 5000)
        progress = part.with_suffix(".progress")
        progress.write_text("not json!", encoding="utf-8")

        # Falls through to no-marker path; actual_size(5000) >= expected(5000) → discard
        offset = get_part_resume_offset(dest, expected_content_length=5000)
        assert offset == 0
        assert not part.exists()

    def test_corrupt_progress_resumes_when_part_smaller_than_expected(self, tmp_path: Path) -> None:
        """Corrupt .progress but part < expected → trusts file size (resume)."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        part.write_bytes(b"x" * 3000)
        progress = part.with_suffix(".progress")
        progress.write_text("{{{invalid", encoding="utf-8")

        offset = get_part_resume_offset(dest, expected_content_length=10000)
        assert offset == 3000


class TestPartSizeDivergence:
    """Exercises the 8KB tolerance boundary between .progress and .part."""

    def test_within_tolerance_resumes(self, tmp_path: Path) -> None:
        """Difference within 8192 bytes → trusts actual file size and resumes."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        actual_size = 10000
        recorded = actual_size + 4096  # within 8KB
        part.write_bytes(b"x" * actual_size)
        progress = part.with_suffix(".progress")
        progress.write_text(json.dumps({"bytes_written": recorded}), encoding="utf-8")

        offset = get_part_resume_offset(dest)
        assert offset == actual_size

    def test_outside_tolerance_discards(self, tmp_path: Path) -> None:
        """Difference > 8192 bytes → discards as unreliable."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        actual_size = 10000
        recorded = actual_size + 8193  # exceeds 8KB tolerance
        part.write_bytes(b"x" * actual_size)
        progress = part.with_suffix(".progress")
        progress.write_text(json.dumps({"bytes_written": recorded}), encoding="utf-8")

        offset = get_part_resume_offset(dest)
        assert offset == 0
        assert not part.exists()

    def test_tolerance_boundary_exact_8192(self, tmp_path: Path) -> None:
        """Exactly 8192 bytes difference → within tolerance, resumes."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        actual_size = 50000
        recorded = actual_size - 8192
        part.write_bytes(b"x" * actual_size)
        progress = part.with_suffix(".progress")
        progress.write_text(json.dumps({"bytes_written": recorded}), encoding="utf-8")

        offset = get_part_resume_offset(dest)
        assert offset == actual_size

    def test_recorded_zero_discards(self, tmp_path: Path) -> None:
        """Progress says 0 bytes written but .part has data → discards."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        part.write_bytes(b"x" * 5000)
        progress = part.with_suffix(".progress")
        progress.write_text(json.dumps({"bytes_written": 0}), encoding="utf-8")

        # recorded(0) > 0 is False → mismatch branch → discard
        offset = get_part_resume_offset(dest)
        assert offset == 0
        assert not part.exists()

    def test_empty_part_returns_zero(self, tmp_path: Path) -> None:
        """Empty .part file (0 bytes) → always returns 0 without reading progress."""
        dest = tmp_path / "test.zip"
        part = dest.with_suffix(".zip.part")
        part.write_bytes(b"")
        progress = part.with_suffix(".progress")
        progress.write_text(json.dumps({"bytes_written": 5000}), encoding="utf-8")

        offset = get_part_resume_offset(dest)
        assert offset == 0
