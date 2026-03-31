from functools import lru_cache
from pathlib import Path
from typing import Optional
import os

from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    project_name: str = "Predictive Maintenance Platform"
    environment: str = "local"
    debug: bool = True

    # Database
    postgres_user: str = "pm_user"
    postgres_password: str = "pm_pass"
    postgres_db: str = "pm_db"
    postgres_host: str = "postgres"
    postgres_port: int = 5432

    # AI service
    ai_service_url: str = "http://ai-service:8000"

    # Auth / Security
    jwt_secret: str = "change-me"
    jwt_algorithm: str = "HS256"
    jwt_exp_minutes: int = 60

    # Notifications - All values must come from .env file
    email_smtp_host: str = ""  # Must be set in .env file
    email_smtp_port: int = 587
    email_smtp_user: str = ""  # Must be set in .env file
    email_smtp_pass: str = ""  # Must be set in .env file
    email_sender: str = ""  # Must be set in .env file
    slack_webhook_url: Optional[str] = None
    notification_email: str = ""  # Fallback recipient if no recipients in DB (optional, can be set in .env)

    # Reporting
    reports_dir: Path = Path("./reports")
    
    # Refresh token
    refresh_token_exp_days: int = 30

    class Config:
        # Load .env from backend directory so SMTP/notification settings are found
        # whether the app is run from repo root or backend/
        env_file = str(Path(__file__).resolve().parent.parent.parent / ".env")
        env_file_encoding = "utf-8"
        case_sensitive = False
        env_file_ignore_empty = True


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    return settings

def clear_settings_cache():
    """Clear settings cache - useful for testing or env var changes"""
    get_settings.cache_clear()

