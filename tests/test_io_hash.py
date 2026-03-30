"""Tests for atlas_stf.io_hash — file hashing utilities."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.io_hash import file_sha256


class TestFileSha256:
    def test_known_content(self, tmp_path: Path) -> None:
        p = tmp_path / "hello.txt"
        p.write_bytes(b"hello world")
        # SHA-256 of "hello world" (no newline)
        assert file_sha256(p) == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

    def test_empty_file(self, tmp_path: Path) -> None:
        p = tmp_path / "empty"
        p.write_bytes(b"")
        # SHA-256 of empty bytes
        assert file_sha256(p) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def test_large_file_chunked(self, tmp_path: Path) -> None:
        """File larger than 8192 bytes to exercise chunked reading."""
        p = tmp_path / "large.bin"
        data = b"x" * 20000
        p.write_bytes(data)
        import hashlib
        expected = hashlib.sha256(data).hexdigest()
        assert file_sha256(p) == expected

    def test_deterministic(self, tmp_path: Path) -> None:
        p = tmp_path / "det.txt"
        p.write_bytes(b"deterministic")
        assert file_sha256(p) == file_sha256(p)
