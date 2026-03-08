from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Idempotent Pipeline"
    database_url: str = "sqlite+aiosqlite:///./pipeline.db"
    # Set to postgresql+asyncpg://user:pass@host/db to migrate to Postgres
    debug: bool = False

    class Config:
        env_file = ".env"


settings = Settings()
