#!/bin/sh
# zero-install-env.sh — Set up Pegasus runtime prediction environment.
#
# Usage:
#   source zero-install-env.sh [/path/to/pegasus-home]
#
# Example:
#   source src/zero-install-env.sh /scitech/shared/home/bamboo/pegasus-runtime/pegasus-5.0.6

# ── Resolve script location (works when sourced in bash/zsh/sh) ─────────────
_SELF="${BASH_SOURCE[0]:-${(%):-%x}}"
_SELF="${_SELF:-$0}"
REPO="$(cd "$(dirname "$_SELF")" && pwd)"

# ── Pegasus binary (first arg, or ~/pegasus-local) ───────────────────────────
_PEG_ARG="${1:-$HOME/pegasus-local}"
PEGASUS_TAR="$(cd "$_PEG_ARG" 2>/dev/null && pwd)"
if [ -z "$PEGASUS_TAR" ]; then
    echo "[zero-install-env] ERROR: Pegasus home not found at: $_PEG_ARG" >&2
    return 1
fi
export PEGASUS_HOME="$PEGASUS_TAR"

# ── Virtual environment ───────────────────────────────────────────────────────
_VENV="$(cd "$REPO/.." && pwd)/venv"
if [ ! -d "$_VENV" ]; then
    echo "[zero-install-env] ERROR: venv not found at $_VENV" >&2
    echo "[zero-install-env] Create it with:" >&2
    echo "  python3.12 -m venv $_VENV" >&2
    echo "  source $_VENV/bin/activate" >&2
    echo "  pip install pyyaml torch scikit-learn numpy pandas astropy gitpython" >&2
    echo "  pip install -e $REPO/packages/pegasus-python" >&2
    echo "  pip install -e $REPO/packages/pegasus-runtime" >&2
    return 1
fi

# shellcheck disable=SC1091
. "$_VENV/bin/activate"
export PEGASUS_PYTHON="$_VENV/bin/python"

# ── PATH: our bin/ and scripts/ first, then Pegasus tarball bin ──────────────
export PATH="\
$REPO/bin:\
$REPO/scripts:\
$PEGASUS_TAR/bin:\
${PATH:-}"

echo "[zero-install-env] PEGASUS_HOME = $PEGASUS_HOME"
echo "[zero-install-env] venv         = $_VENV"
echo "[zero-install-env] python       = $(python --version 2>&1)"
echo "[zero-install-env] Commands available: pegasus-plan, pegasus-inject-prescripts, pegasus-runtime-predictor"
