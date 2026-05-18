#!/usr/bin/env bash

set -euo pipefail

SESSION_ID="${1:-demo}"
MESSAGE="${2:-"list the countries available in cce index"}"
BASE_URL="${BASE_URL:-http://localhost:8000}"

echo "Streaming events for session '${SESSION_ID}' (message: ${MESSAGE})..."
curl -N \
  -H "Content-Type: application/json" \
  -X POST \
  "${BASE_URL}/chat?stream=true" \
  -d "{\"session_id\": \"${SESSION_ID}\", \"message\": \"${MESSAGE}\"}"

