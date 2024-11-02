# config.py
import os
from dotenv import load_dotenv
from pydantic import BaseSettings

load_dotenv()

class Settings(BaseSettings):
    TIMESCALE_DB_PASSWORD: str = os.getenv("TIMESCALE_DB_PASSWORD")
    DATABASE_URL: str = os.getenv("HOST")
    SECRET_KEY: str = os.getenv("SECRET_KEY")

    class Config:
        env_file = ".env"

settings = Settings()

# Dynamically construct the full DATABASE_URL with password
settings.DATABASE_URL = settings.DATABASE_URL.replace(
    "@", f":{settings.TIMESCALE_DB_PASSWORD}@"
)

print(settings.DATABASE_URL)
