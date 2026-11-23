"""Pipeline entry point: ingest -> process -> load. Run with: python -m src.run_pipeline"""
import argparse
import logging
import sys
import time

from . import config
from .ingest import download_all
from .load_postgres import load_to_postgres
from .process import run_processing

log = logging.getLogger("pipeline")


def main() -> int:
    parser = argparse.ArgumentParser(description="NYC taxi batch pipeline")
    parser.add_argument("--skip-load", action="store_true", help="skip the PostgreSQL step")
    args = parser.parse_args()

    config.setup_logging()
    started = time.time()
    try:
        log.info("=== Pipeline started: %d, months %s ===", config.YEAR, config.MONTHS)
        files = download_all()
        report = run_processing(files)
        runtime = round(time.time() - started, 1)
        if not args.skip_load:
            load_to_postgres(report, runtime)
        log.info("=== Pipeline finished in %.1f s ===", time.time() - started)
        return 0
    except Exception:
        log.exception("Pipeline failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
