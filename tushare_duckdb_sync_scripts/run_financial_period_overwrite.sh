#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
source "$SCRIPT_DIR/bootstrap.sh"

"${PYTHON_BIN:-python}" "$SCRIPT_DIR/run_financial_period_overwrite.py" "$@"
