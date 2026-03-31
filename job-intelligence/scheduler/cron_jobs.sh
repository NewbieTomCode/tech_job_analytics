#!/bin/bash
# =============================================================
# Job Intelligence Pipeline - Cron Scheduler
# =============================================================
# Runs the data pipeline on a schedule.
#
# Usage (add to crontab with: crontab -e):
#   # Run pipeline daily at 8am
#   0 8 * * * /path/to/cron_jobs.sh >> /var/log/job_pipeline.log 2>&1
#
# Or with Docker:
#   0 8 * * * cd /path/to/tech_job_analytics && docker-compose run --rm app >> /var/log/job_pipeline.log 2>&1
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================="
echo "Pipeline run started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================="

cd "$PROJECT_DIR"

# Run the pipeline
python run_pipeline.py

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Pipeline completed successfully: $(date '+%Y-%m-%d %H:%M:%S')"
else
    echo "Pipeline FAILED with exit code $EXIT_CODE: $(date '+%Y-%m-%d %H:%M:%S')"
fi

echo "============================================="
exit $EXIT_CODE
