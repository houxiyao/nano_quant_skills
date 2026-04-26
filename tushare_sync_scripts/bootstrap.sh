#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)

source /Users/mgong/miniforge3/etc/profile.d/conda.sh
conda activate legonanobot

cd "$PROJECT_ROOT"
export PYTHONUNBUFFERED=1
