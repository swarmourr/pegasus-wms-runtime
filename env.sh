#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Pegasus WMS + Runtime Prediction — Environment Setup
#
# Usage:
#   source env.sh                         # auto-detect install dir
#   source env.sh /path/to/install/dir    # explicit install dir
#
# ─────────────────────────────────────────────────────────────────────────────

# ── 1. Resolve install directory ─────────────────────────────────────────────
if [ -n "$1" ]; then
    INSTALL_DIR="$1"
else
    # Default: directory containing this script
    INSTALL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

# ── 2. Pegasus ────────────────────────────────────────────────────────────────
export PEGASUS_HOME="${INSTALL_DIR}/pegasus"

# ── 3. HTCondor ──────────────────────────────────────────────────────────────
export CONDOR_CONFIG="${INSTALL_DIR}/condor/condor.conf"

# ── 4. Runtime Prediction extension ──────────────────────────────────────────
# wrappers/ MUST come before pegasus/bin so our pegasus-plan wrapper
# intercepts every call and injects the prediction jobs before planning.
export PEGASUS_RUNTIME_PREDICTION_HOME="${INSTALL_DIR}/pegasus-wms-runtime-src"

# Optional: point to a custom trained model
# export PEGASUS_RUNTIME_PREDICTION_MODEL="${PEGASUS_RUNTIME_PREDICTION_HOME}/packages/pegasus-python/src/Pegasus/models/pegasus_oracle_model.pkl"

# Optional: disable prediction for all runs in this shell
# export PEGASUS_RUNTIME_PREDICTION=false

# ── 5. PATH ───────────────────────────────────────────────────────────────────
export PATH="\
${PEGASUS_RUNTIME_PREDICTION_HOME}/wrappers:\
${PEGASUS_HOME}/bin:\
${INSTALL_DIR}/condor/bin:\
${INSTALL_DIR}/condor/sbin:\
$PATH"

# ── 6. Python path for Pegasus packages ──────────────────────────────────────
export PYTHONPATH="\
${PEGASUS_RUNTIME_PREDICTION_HOME}/packages/pegasus-python/src:\
${PEGASUS_RUNTIME_PREDICTION_HOME}/packages/pegasus-api/src:\
${PEGASUS_RUNTIME_PREDICTION_HOME}/packages/pegasus-common/src:\
$PYTHONPATH"

# ── 7. Verify ─────────────────────────────────────────────────────────────────
echo "[env.sh] PEGASUS_HOME              = ${PEGASUS_HOME}"
echo "[env.sh] CONDOR_CONFIG             = ${CONDOR_CONFIG}"
echo "[env.sh] PREDICTION_HOME           = ${PEGASUS_RUNTIME_PREDICTION_HOME}"
echo "[env.sh] pegasus-plan wrapper      = $(which pegasus-plan 2>/dev/null || echo 'NOT FOUND')"
echo "[env.sh] pegasus-runtime-predictor = $(which pegasus-runtime-predictor 2>/dev/null || echo 'NOT FOUND')"
