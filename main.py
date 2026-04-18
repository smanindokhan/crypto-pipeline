import sys
from logger import get_logger
from ingestion import coin_prices, global_market

log = get_logger("main")

def main():
    log.info("=" * 50)
    log.info("Pipeline run starting")
    log.info("=" * 50)

    errors = []

    # Run each ingestion inside its own try/except.
    # This means if coin_prices fails (e.g. API timeout),
    # global_market still runs — and vice versa.
    # We collect errors and report them all at the end.
    for name, module in [
        ("coin_prices",   coin_prices),
        ("global_market", global_market),
    ]:
        try:
            log.info(f"--- Starting {name} ---")
            module.run()
            log.info(f"--- {name} complete ---")
        except Exception as e:
            # exc_info=True tells the logger to include the full
            # stack trace in the log file — invaluable for debugging
            log.error(f"{name} FAILED: {e}", exc_info=True)
            errors.append(name)

    log.info("=" * 50)
    if errors:
        log.error(f"Pipeline finished with failures: {errors}")
        # sys.exit(1) signals failure to any orchestrator
        # (Airflow, cron, CI) — exit code 0 = success, 1 = failure
        sys.exit(1)
    else:
        log.info("Pipeline run complete — all ingestions succeeded")

if __name__ == "__main__":
    main()