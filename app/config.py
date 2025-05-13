import os
from functools import lru_cache

class Settings:
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./data.sqlite")
    TZ: str = os.getenv("TZ", "Europe/Paris")
    SHOW_BROWSER: bool = os.getenv("SHOW_BROWSER", "1") == "1"

@lru_cache
def get_settings() -> Settings:
    return Settings()