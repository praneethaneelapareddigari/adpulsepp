
import os

class Settings:
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_USER: str = os.getenv("DB_USER", "adpulse")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "adpulse")
    DB_NAME: str = os.getenv("DB_NAME", "adpulse")

settings = Settings()
