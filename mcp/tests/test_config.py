"""Tests for environment configuration, including TLS material."""

import pytest

from bifract_mcp import config

BASE_ENV = {
    "BIFRACT_URL": "https://bifract.example.com/",
    "BIFRACT_API_KEY": "bifract_abc123",
}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in list(config.os.environ):
        if name.startswith("BIFRACT_"):
            monkeypatch.delenv(name, raising=False)
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)


def test_defaults(monkeypatch):
    cfg = config.load()
    assert cfg.error is None
    assert cfg.url == "https://bifract.example.com"
    assert cfg.api_base == "https://bifract.example.com/api/v1"
    assert cfg.verify is True
    assert cfg.client_cert is None
    assert cfg.timeout == config.DEFAULT_TIMEOUT


def test_missing_settings_are_reported_not_raised(monkeypatch):
    monkeypatch.delenv("BIFRACT_URL")
    monkeypatch.delenv("BIFRACT_API_KEY")
    cfg = config.load()
    assert "BIFRACT_URL" in cfg.error
    assert "BIFRACT_API_KEY" in cfg.error


def test_url_scheme_is_required(monkeypatch):
    monkeypatch.setenv("BIFRACT_URL", "bifract.example.com")
    assert "http://" in config.load().error


def test_verification_can_be_disabled(monkeypatch):
    monkeypatch.setenv("BIFRACT_VERIFY_SSL", "false")
    assert config.load().verify is False
    monkeypatch.setenv("BIFRACT_VERIFY_SSL", "TRUE")
    assert config.load().verify is True


def test_ca_bundle_overrides_the_verify_flag(monkeypatch, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("cert")
    monkeypatch.setenv("BIFRACT_CA_CERT", str(ca))
    monkeypatch.setenv("BIFRACT_VERIFY_SSL", "false")
    cfg = config.load()
    assert cfg.verify == str(ca)
    assert cfg.error is None


def test_missing_ca_bundle_is_reported(monkeypatch, tmp_path):
    monkeypatch.setenv("BIFRACT_CA_CERT", str(tmp_path / "absent.pem"))
    assert "BIFRACT_CA_CERT file not found" in config.load().error


def test_client_certificate_pair(monkeypatch, tmp_path):
    cert, key = tmp_path / "c.pem", tmp_path / "k.pem"
    cert.write_text("cert")
    key.write_text("key")
    monkeypatch.setenv("BIFRACT_CLIENT_CERT", str(cert))
    monkeypatch.setenv("BIFRACT_CLIENT_KEY", str(key))
    cfg = config.load()
    assert cfg.client_cert == (str(cert), str(key))
    assert cfg.error is None


def test_combined_client_certificate(monkeypatch, tmp_path):
    cert = tmp_path / "combined.pem"
    cert.write_text("cert+key")
    monkeypatch.setenv("BIFRACT_CLIENT_CERT", str(cert))
    assert config.load().client_cert == str(cert)


def test_missing_client_key_is_reported(monkeypatch, tmp_path):
    cert = tmp_path / "c.pem"
    cert.write_text("cert")
    monkeypatch.setenv("BIFRACT_CLIENT_CERT", str(cert))
    monkeypatch.setenv("BIFRACT_CLIENT_KEY", str(tmp_path / "absent.pem"))
    assert "BIFRACT_CLIENT_KEY file not found" in config.load().error


@pytest.mark.parametrize("value", ["abc", "0", "-5"])
def test_bad_timeout_falls_back_to_the_default(monkeypatch, value):
    monkeypatch.setenv("BIFRACT_TIMEOUT", value)
    cfg = config.load()
    assert cfg.timeout == config.DEFAULT_TIMEOUT
    assert "BIFRACT_TIMEOUT" in cfg.error


def test_timeout_override(monkeypatch):
    monkeypatch.setenv("BIFRACT_TIMEOUT", "120")
    assert config.load().timeout == 120.0
