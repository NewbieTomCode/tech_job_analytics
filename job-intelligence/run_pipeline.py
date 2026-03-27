"""
Main pipeline runner: Extract → Load

Usage:
    python run_pipeline.py             # fetch from API and load into DB
    python run_pipeline.py --from-file saved_data/input/all_jobs.json  # load from existing JSON
"""
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Job Intelligence Pipeline")
    parser.add_argument(
        "--from-file",
        type=str,
        help="Load jobs from an existing JSON file instead of fetching from API",
    )
    args = parser.parse_args()

    if args.from_file:
        from src.load.load_data import load_from_json
        logger.info(f"Loading from file: {args.from_file}")
        stats = load_from_json(args.from_file)
    else:
        from src.extract.fetch_data import fetch_job_data
        from src.load.load_data import load_jobs
        logger.info("Fetching from Adzuna API...")
        jobs = fetch_job_data()
        logger.info(f"Fetched {len(jobs)} jobs, loading into database...")
        stats = load_jobs(jobs)

    logger.info(f"Pipeline complete: {stats}")


if __name__ == "__main__":
    main()
