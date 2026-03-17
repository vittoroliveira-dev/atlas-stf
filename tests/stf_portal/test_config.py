"""Tests for STF portal configuration."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.stf_portal._config import StfPortalConfig


def test_default_config(tmp_path: Path):
    config = StfPortalConfig(output_dir=tmp_path / "portal")
    assert config.rate_limit_seconds == 3.0
    assert config.max_concurrent == 1
    assert config.refetch_after_days == 30
    assert (tmp_path / "portal").exists()


def test_custom_config(tmp_path: Path):
    config = StfPortalConfig(
        output_dir=tmp_path / "out",
        rate_limit_seconds=5.0,
        max_processes=100,
    )
    assert config.rate_limit_seconds == 5.0
    assert config.max_processes == 100


def test_concurrency_defaults(tmp_path: Path):
    config = StfPortalConfig(output_dir=tmp_path / "portal")
    assert config.max_in_flight == 4
    assert config.tab_concurrency == 2
    assert config.global_rate_seconds == 1.0


def test_proxy_config(tmp_path: Path):
    proxies = ["socks5://localhost:1080", "socks5://localhost:1081"]
    config = StfPortalConfig(output_dir=tmp_path / "portal", proxies=proxies)
    assert config.proxies == proxies


def test_empty_proxies_default(tmp_path: Path):
    config = StfPortalConfig(output_dir=tmp_path / "portal")
    assert config.proxies == []


def test_circuit_breaker_defaults(tmp_path: Path):
    config = StfPortalConfig(output_dir=tmp_path / "portal")
    assert config.circuit_breaker_threshold == 5
    assert config.circuit_breaker_cooldown == 120.0
