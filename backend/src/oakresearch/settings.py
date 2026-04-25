from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "OakResearch"
    environment: str = Field(default="development", alias="ENVIRONMENT")
    database_url: str = Field(default="postgresql://oakresearch:oakresearch@db:5432/oakresearch", alias="DATABASE_URL")
    app_secret: str = Field(default="change-me-in-production", alias="APP_SECRET")
    session_secret: str = Field(default="change-me-in-production", alias="SESSION_SECRET")
    gemini_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    storage_path: str = Field(default="/data/oakresearch", alias="SOURCE_STORAGE_PATH")
    web_origin: str = Field(default="http://localhost:5173", alias="WEB_ORIGIN")
    cookie_secure: bool = Field(default=False, alias="COOKIE_SECURE")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
