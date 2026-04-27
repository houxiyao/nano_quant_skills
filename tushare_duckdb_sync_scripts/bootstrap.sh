#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT="$SCRIPT_DIR"

resolve_conda_sh() {
	local candidate
	if [[ -n "${CONDA_SH_PATH:-}" && -f "${CONDA_SH_PATH}" ]]; then
		printf '%s\n' "${CONDA_SH_PATH}"
		return 0
	fi

	if [[ -n "${CONDA_EXE:-}" ]]; then
		candidate="$(cd -- "$(dirname -- "${CONDA_EXE}")/.." && pwd)/etc/profile.d/conda.sh"
		if [[ -f "${candidate}" ]]; then
			printf '%s\n' "${candidate}"
			return 0
		fi
	fi

	for candidate in \
		"$HOME/miniforge3/etc/profile.d/conda.sh" \
		"$HOME/mambaforge/etc/profile.d/conda.sh" \
		"$HOME/miniconda3/etc/profile.d/conda.sh" \
		"$HOME/anaconda3/etc/profile.d/conda.sh"
	do
		if [[ -f "${candidate}" ]]; then
			printf '%s\n' "${candidate}"
			return 0
		fi
	done

	return 1
}

if conda_sh_path="$(resolve_conda_sh)"; then
	source "$conda_sh_path"
	conda activate "${CONDA_ENV_NAME:-legonanobot}"
fi

cd "$PROJECT_ROOT"
export PYTHONUNBUFFERED=1
