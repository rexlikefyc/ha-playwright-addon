#!/bin/bash
set -e

SCRIPT_NAME=$(jq --raw-output '.script_name' /data/options.json)

echo "Starting Playwright script: $SCRIPT_NAME"
python3 "$SCRIPT_NAME"

echo "Script execution finished."
