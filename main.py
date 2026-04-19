import sys
from datetime import datetime, timezone
from logger import get_logger
from ingestion import coin_prices, global_market

log = get_logger("main")


def main():
    # Accept an optional execution date as a command-line argument.
    # Usage:
    #   python main.py               → runs for current UTC time
    #   python main.py 2025-04-15   → runs for that specific date
    #
    # This lets you manually trigger a backfill for a missed day
    # without touching the code.
    execution_date = None

    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        try:
            execution_date = datetime.strptime(
                date_str, "%Y-%m-%d"
            ).replace(tzinfo=timezone.utc)
            log.info(f"Backfill mode: running for {date_str}")
        except ValueError:
            log.error(
                f"Invalid date format '{date_str}'. "
                f"Use YYYY-MM-DD e.g. 2025-04-15"
            )
            sys.exit(1)

    log.info("=" * 50)
    log.info("Pipeline run starting")
    log.info("=" * 50)

    errors = []

    for name, module in [
        ("coin_prices",   coin_prices),
        ("global_market", global_market),
    ]:
        try:
            log.info(f"--- Starting {name} ---")
            module.run(execution_date=execution_date)
            log.info(f"--- {name} complete ---")
        except Exception as e:
            log.error(f"{name} FAILED: {e}", exc_info=True)
            errors.append(name)

    log.info("=" * 50)
    if errors:
        log.error(f"Pipeline finished with failures: {errors}")
        sys.exit(1)
    else:
        log.info("Pipeline run complete — all ingestions succeeded")


if __name__ == "__main__":
    main()