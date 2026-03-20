from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    app_name: str = "Idempotent Pipeline"
    database_url: str = "sqlite+aiosqlite:///./pipeline.db"
    # Set to postgresql+asyncpg://user:pass@host/db to migrate to Postgres
    debug: bool = False


settings = Settings()
