#!/bin/sh
# zero-install-env.sh — Run Pegasus runtime prediction with NO installation.
#
# Usage:
#   source zero-install-env.sh [/path/to/pegasus-tarball] [/path/to/this-repo]
#
# Defaults:
#   PEGASUS_TAR  = ~/pegasus-local          (extracted Pegasus binary tarball)
#   REPO         = directory of this script  (cloned pegasus-wms-runtime repo)
#
# After sourcing, the following commands are available without any pip install:
#   pegasus-plan
#   pegasus-inject-prescripts
#   pegasus-runtime-predictor

REPO="$(cd "$(dirname "$0")" && pwd)"
PEGASUS_TAR="${1:-$HOME/pegasus-local}"

# ── Pegasus Java infrastructure ─────────────────────────────────────────────
export PEGASUS_HOME="$PEGASUS_TAR"

# ── Python packages (no pip install — loaded directly from source) ───────────
export PYTHONPATH="\
$REPO/packages/pegasus-api/src:\
$REPO/packages/pegasus-common/src:\
$REPO/packages/pegasus-python/src:\
$REPO/packages/pegasus-worker/src:\
$REPO/packages/pegasus-runtime/src:\
${PYTHONPATH:-}"

# ── PATH: our bin/ first (modified pegasus-plan), then tarball, then wrappers ─
export PATH="\
$REPO/bin:\
$REPO/scripts:\
$PEGASUS_TAR/bin:\
${PATH:-}"

echo "[zero-install-env] PEGASUS_HOME = $PEGASUS_HOME"
echo "[zero-install-env] REPO         = $REPO"
echo "[zero-install-env] Commands available:"
echo "    pegasus-plan"
echo "    pegasus-inject-prescripts"
echo "    pegasus-runtime-predictor"
