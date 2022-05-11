#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" >/dev/null 2>&1 && pwd )"
VENV_PATH="${VENV_PATH:-"${SCRIPT_DIR}/venv"}"
source "${VENV_PATH}/bin/activate"
exec python "${SCRIPT_DIR}/run_pychord.py" $@
