import logging
import os
from unittest import mock

import pytest


def _reload_worker(env_overrides: dict):
    import ingestion.worker as worker

    with mock.patch.dict(os.environ, env_overrides, clear=False):
        # Recompute config derived from environment without reloading module-level metrics.
        worker.BINANCE_HOSTS = worker._list_from_env("BINANCE_HOSTS", "")
        worker.BINANCE_WS_BASE = os.getenv("BINANCE_WS_BASE", worker.BINANCE_WS_BASE)

    return worker


def test_validate_binance_connectivity_uses_ws_base(monkeypatch, caplog):
    worker = _reload_worker({"BINANCE_HOSTS": "", "BINANCE_WS_BASE": "wss://stream.binance.com:9443"})
    caplog.set_level(logging.WARNING)

    called_hosts = []

    def fake_dns(host: str) -> bool:
        called_hosts.append(host)
        return True

    monkeypatch.setattr(worker, "_dns_lookup", fake_dns)
    monkeypatch.setattr(worker, "_https_ping", lambda url, timeout=5.0: True)

    assert worker.validate_binance_connectivity() is True
    assert called_hosts == ["stream.binance.com"]
    assert any("derived stream.binance.com" in rec.message for rec in caplog.records)


def test_validate_binance_connectivity_flags_failure(monkeypatch, caplog):
    worker = _reload_worker({"BINANCE_HOSTS": "", "BINANCE_WS_BASE": "wss://api.binance.com"})
    caplog.set_level(logging.INFO)

    monkeypatch.setattr(worker, "_dns_lookup", lambda host: True)
    monkeypatch.setattr(worker, "_https_ping", lambda url, timeout=5.0: False)

    assert worker.validate_binance_connectivity() is False
    assert any("All Binance host checks failed" in rec.message for rec in caplog.records)
