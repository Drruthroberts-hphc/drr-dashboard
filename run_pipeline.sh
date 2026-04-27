#!/bin/bash
# DRR Dashboard Weekly Pipeline Runner
# Called by launchd every Monday at 6 AM

# Move into the project directory
cd "/Users/ruthroberts/Desktop/Code output/drr-dashboard" || exit 1

# Set PATH so launchd can find python3, git, etc.
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"

# Timestamp for the log
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
echo "============================================================"
echo "[$TIMESTAMP] Starting DRR Dashboard pipeline"
echo "============================================================"

# Run the pipeline
/usr/bin/python3 main.py
PIPELINE_EXIT=$?

if [ $PIPELINE_EXIT -ne 0 ]; then
    echo "[$TIMESTAMP] Pipeline failed with exit code $PIPELINE_EXIT"
    exit $PIPELINE_EXIT
fi

# Find the latest snapshot file (sorted by name picks the most recent date)
LATEST_SNAPSHOT=$(ls -1 snapshots/snapshot_*.json | sort | tail -1)
echo "[$TIMESTAMP] Latest snapshot: $LATEST_SNAPSHOT"

# Push to GitHub Pages
git add snapshots/manifest.json "$LATEST_SNAPSHOT" 2>&1
git commit -m "Weekly pipeline run $(date '+%Y-%m-%d')

Automated pipeline run via launchd.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>" 2>&1
git push 2>&1

echo "[$TIMESTAMP] Pipeline complete and pushed to GitHub"
echo ""
