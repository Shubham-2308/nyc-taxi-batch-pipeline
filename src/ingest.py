"""Stage 1 - Data ingestion: download monthly NYC TLC Yellow Taxi Parquet files."""
import logging
import time
from pathlib import Path

import requests

from . import config

log = logging.getLogger("ingest")

MIN_VALID_BYTES = 1_000_000  # a real monthly file is tens of MB; smaller means a failed download


def file_name(year: int, month: int) -> str:
    return f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def download_month(year: int, month: int, retries: int = 3) -> Path:
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    target = config.RAW_DIR / file_name(year, month)

    if target.exists() and target.stat().st_size >= MIN_VALID_BYTES:
        log.info("Already downloaded, skipping: %s", target.name)
        return target

    url = f"{config.BASE_URL}/{file_name(year, month)}"
    tmp = target.with_suffix(".part")

    for attempt in range(1, retries + 1):
        try:
            log.info("Downloading %s (attempt %d/%d)", url, attempt, retries)
            with requests.get(url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        fh.write(chunk)
            if tmp.stat().st_size < MIN_VALID_BYTES:
                raise IOError(f"Downloaded file too small: {tmp.stat().st_size} bytes")
            tmp.replace(target)
            log.info("Saved %s (%.1f MB)", target.name, target.stat().st_size / 1e6)
            return target
        except Exception as exc:  # network errors, HTTP errors, truncated files
            log.warning("Download failed: %s", exc)
            if tmp.exists():
                tmp.unlink()
            if attempt == retries:
                raise
            time.sleep(5 * attempt)
    raise RuntimeError("unreachable")


def download_all() -> list:
    return [download_month(config.YEAR, m) for m in config.MONTHS]
