"""Central configuration and logging setup. All values can be overridden by environment variables (.env)."""
import logging
import os
from pathlib import Path
from urllib.parse import quote_plus

BASE_DIR = Path(os.getenv("APP_BASE_DIR", Path(__file__).resolve().parent.parent))
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
REPORT_DIR = DATA_DIR / "reports"
LOG_DIR = BASE_DIR / "logs"

YEAR = int(os.getenv("TAXI_YEAR", "2024"))
MONTHS = [int(m) for m in os.getenv("TAXI_MONTHS", "1,2,3").split(",") if m.strip()]
BASE_URL = os.getenv("TAXI_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data")

SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "16")


def period_bounds(year: int = YEAR, months=None):
    """Return (start, end) date strings: start inclusive, end exclusive."""
    months = months or MONTHS
    first, last = min(months), max(months)
    start = f"{year:04d}-{first:02d}-01"
    end = f"{year + 1:04d}-01-01" if last == 12 else f"{year:04d}-{last + 1:02d}-01"
    return start, end


def postgres_url() -> str:
    user = os.environ["POSTGRES_USER"]
    password = quote_plus(os.environ["POSTGRES_PASSWORD"])
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
        ],
        force=True,
    )
