"""Configuration smoke tests."""

from neo_trader.config import Settings


def test_settings_defaults_are_safe() -> None:
    settings = Settings()

    assert settings.environment == "development"
    assert settings.dry_run is True
    assert settings.live_trading_enabled is False
    assert settings.trading_mode == "readonly"
    assert settings.broker_name == "paper"
    assert settings.tbank_mode == "readonly"
    assert settings.tbank_max_retries >= 0
