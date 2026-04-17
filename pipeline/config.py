"""Central configuration — all env vars in one place."""
import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # Database
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "bungewatch")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "bungewatch")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "bungewatch")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # Anthropic
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    CLAUDE_MODEL: str = "claude-haiku-4-5"

    # Scraping
    KENYALAW_BASE_URL: str = os.getenv(
        "KENYALAW_BASE_URL", "https://kenyalaw.org/kl/index.php"
    )
    PARLIAMENT_BASE_URL: str = os.getenv(
        "PARLIAMENT_BASE_URL", "https://www.parliament.go.ke"
    )
    SCRAPE_YEARS: list[int] = [
        int(y) for y in os.getenv("SCRAPE_YEARS", "2024,2025,2026").split(",")
    ]
    REQUEST_DELAY_SECONDS: float = float(os.getenv("REQUEST_DELAY_SECONDS", "2"))

    # PDF storage
    PDF_STORAGE_PATH: str = os.getenv("PDF_STORAGE_PATH", "/tmp/bungewatch/pdfs")

    # Claude cost per token (Haiku pricing, USD)
    HAIKU_INPUT_COST_PER_1K: float = 0.00025
    HAIKU_OUTPUT_COST_PER_1K: float = 0.00125


settings = Settings()
