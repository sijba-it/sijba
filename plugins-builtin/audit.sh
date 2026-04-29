#!/usr/bin/env bash
# Audit handler: logs every inbound message and continues pipeline.
set -euo pipefail

INPUT=$(cat)
ID=$(echo "$INPUT"    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id','?'))")
SRC=$(echo "$INPUT"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('source','?'))")
TEXT=$(echo "$INPUT"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('pipeline_text',''))")

echo "[audit] id=$ID source=$SRC text=$(echo "$TEXT" | head -c 120)" >&2

echo '{"continue": true, "metadata": {"audited": true}}'
