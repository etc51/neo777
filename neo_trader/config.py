"""Application settings for the neo_trader scaffold."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables or .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="NEO_TRADER_",
        extra="ignore",
    )

    environment: Literal["development", "test", "production"] = "development"
    log_level: str = "INFO"
    dry_run: bool = Field(
        default=True,
        description="Must remain true in this scaffold; live trading is not implemented.",
    )
    live_trading_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices("NEO_TRADER_LIVE_TRADING_ENABLED", "LIVE_TRADING_ENABLED"),
        description="Explicit opt-in required before any live gateway can submit orders.",
    )
    trading_mode: Literal["readonly", "sandbox", "live"] = Field(
        default="readonly",
        validation_alias=AliasChoices("NEO_TRADER_TRADING_MODE", "TRADING_MODE"),
        description="Global safety mode; readonly is the only safe default.",
    )
    data_dir: Path = Path("./data")
    broker_name: str = "paper"
    tbank_token: SecretStr | None = Field(
        default=None,
        description="T-Bank Invest API token for dry-run connectivity experiments only.",
    )
    tbank_mode: Literal["readonly", "sandbox", "live"] = "readonly"
    tbank_base_url: str | None = None
    tbank_timeout_seconds: float = Field(default=10.0, gt=0)
    tbank_max_retries: int = Field(default=3, ge=0)
    tbank_backoff_seconds: float = Field(default=0.25, ge=0)


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings."""

    return Settings()
