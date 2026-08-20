#!/bin/sh
# zero-install-env.sh — Run Pegasus runtime prediction with NO installation.
#
# Usage:
#   source zero-install-env.sh [/path/to/pegasus-tarball]
#
# Example:
#   source src/zero-install-env.sh pegasus
#   source src/zero-install-env.sh /abs/path/to/pegasus

# ── Resolve script location (works when sourced in bash/zsh/sh) ─────────────
_SELF="${BASH_SOURCE[0]:-${(%):-%x}}"
_SELF="${_SELF:-$0}"
REPO="$(cd "$(dirname "$_SELF")" && pwd)"

# ── Pegasus binary tarball (first arg, or ~/pegasus-local) ───────────────────
_PEG_ARG="${1:-$HOME/pegasus-local}"
# Resolve to absolute path
PEGASUS_TAR="$(cd "$_PEG_ARG" 2>/dev/null && pwd)"
if [ -z "$PEGASUS_TAR" ]; then
    echo "[zero-install-env] ERROR: Pegasus tarball not found at: $_PEG_ARG" >&2
    return 1
fi

export PEGASUS_HOME="$PEGASUS_TAR"

# ── Python 3.8+ — create a local python3 symlink pointing to python3.12 ──────
# Safe for shared accounts: only affects sessions that source this file.
_LOCALBIN="$REPO/localbin"
mkdir -p "$_LOCALBIN"
for _py in python3.12 python3.11 python3.10 python3.9 python3.8; do
    if command -v "$_py" >/dev/null 2>&1; then
        ln -sf "$(command -v "$_py")" "$_LOCALBIN/python3"
        export PEGASUS_PYTHON="$_LOCALBIN/python3"
        break
    fi
done
export PATH="$_LOCALBIN:$PATH"

# ── Python interpreter — prefer python3.12+, fall back to python3 ────────────
for _py in python3.12 python3.11 python3.10 python3.9 python3.8 python3; do
    if command -v "$_py" >/dev/null 2>&1; then
        export PEGASUS_PYTHON="$_py"
        break
    fi
done

# ── Python packages — loaded directly from source, no pip install ────────────
export PYTHONPATH="\
$REPO/packages/pegasus-api/src:\
$REPO/packages/pegasus-common/src:\
$REPO/packages/pegasus-python/src:\
$REPO/packages/pegasus-worker/src:\
$REPO/packages/pegasus-runtime/src:\
${PYTHONPATH:-}"

# ── PATH: our bin/ first, then tarball bin, then wrapper scripts ─────────────
export PATH="\
$REPO/bin:\
$REPO/scripts:\
$PEGASUS_TAR/bin:\
${PATH:-}"

echo "[zero-install-env] PEGASUS_HOME = $PEGASUS_HOME"
echo "[zero-install-env] REPO         = $REPO"
echo "[zero-install-env] Commands available: pegasus-plan, pegasus-inject-prescripts, pegasus-runtime-predictor"
