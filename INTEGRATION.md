# PegasusOracle — Runtime Prediction Integration

This document explains how the AI-based runtime predictor is integrated into Pegasus WMS, file by file, in the exact order of execution.

---

## Overview

The integration hooks into the normal Pegasus workflow execution pipeline **without modifying the Java planner or any catalog files**. The user runs `pegasus-plan` as usual — the prediction happens automatically before each job is submitted to HTCondor.

```
User: pegasus-plan workflow.yml ...
         |
         v
  [bin/pegasus-plan]               wrapper — runs real planner, then injects prescripts
         |
         v
  [pegasus_inject_prescripts.py]   post-processes .dag — adds SCRIPT PRE per job
         |
         v
  condor_submit_dag workflow.dag   HTCondor/DAGMan takes over
         |
         | (before each job)
         v
  [pegasus_runtime_predictor.py]   prescript — runs AI model, patches .sub file
         |
         v
  [predictor.py]                   core engine — PegasusOracle model + patch_sub_file()
         |
         v
  HTCondor submits job with +PredictedRuntime in ClassAd
```

---

## File 1 — `bin/pegasus-plan`

**Type:** Modified (original Pegasus file replaced with a wrapper)

**When it runs:** When the user calls `pegasus-plan`

### What it does

1. Locates the **real** `pegasus-plan` (Java planner inside `PEGASUS_HOME/bin/`) — never calls itself recursively
2. Runs the real planner with **all original arguments unchanged** (`"$@"`)
3. Captures the planner output to detect the submit directory (e.g. `/submit/pegasus/run0001`)
4. Calls `pegasus-inject-prescripts <submit_dir>` automatically

### Key design decisions

- The user sees **no difference** — same command, same arguments, same output
- If `pegasus-inject-prescripts` fails, the workflow continues anyway (`|| true`)
- Works in two modes: system install (`PATH`) or zero-install tarball (`PEGASUS_HOME`)

### Code flow

```sh
# 1. Find real pegasus-plan
_REAL="$PEGASUS_HOME/bin/pegasus-plan"

# 2. Run it, capture output
"$_REAL" "$@" > "$_tmpout" 2>&1

# 3. Detect submit dir from "pegasus-run /path/run0001" line
_run_dir=$(grep "pegasus-run" "$_tmpout" | grep -oE '/[^[:space:]]+/run[0-9]+')

# 4. Inject prescripts
pegasus-inject-prescripts "$_run_dir" || true
```

---

## File 2 — `packages/pegasus-python/src/Pegasus/cli/pegasus_inject_prescripts.py`

**Type:** New file (created by us)

**When it runs:** Once, immediately after `pegasus-plan` finishes

### What it does

1. Reads `braindump.yml` from the submit dir to find the `.dag` file
2. Parses every `JOB` line in the `.dag` → reads each `.sub` file for `+pegasus_wf_dax_job_id`
3. Filters out Pegasus system jobs (`stage_in_*`, `stage_out_*`, `cleanup_*`, `register_*`, etc.)
4. Computes **topological levels** using BFS on the `PARENT/CHILD` dependency graph
   - Level 0 = jobs with no parents (run first)
   - Level 1 = jobs whose only parents are at level 0
   - etc.
5. Injects a `SCRIPT PRE` line before each user job's `JOB` line in the `.dag`
6. Writes the modified `.dag` back in place

### What gets injected into the `.dag`

```
SCRIPT PRE inspiral_0  pegasus-runtime-predictor  workflow.yml  output/  --level=0  --job-id=inspiral_0
JOB inspiral_0 inspiral_0.sub
```

The `--level=N` argument allows all parallel jobs at the same level to share one prediction computation pass. The `--job-id` tells the predictor which `.sub` file to patch.

### What it reads / writes

| File | Action |
|------|--------|
| `braindump.yml` | Read — to find the `.dag` path |
| `<job>.sub` (all) | Read — to extract `+pegasus_wf_dax_job_id` |
| `workflow.dag` | Read then write — injects `SCRIPT PRE` lines |

---

## File 3 — `packages/pegasus-python/src/Pegasus/cli/pegasus_runtime_predictor.py`

**Type:** New file (created by us)

**When it runs:** Once per job, as the `SCRIPT PRE` hook called by DAGMan before each job is submitted

### What it does — Phase 1: Compute predictions

> Only the **first** process to arrive per level does this. The rest wait for the JSON file.

1. Loads `workflow.yml` — reads all jobs, input/output files, Condor profiles
2. Scans disk near the output directory for **actual staged file sizes** (more accurate than YAML values)
3. Reads all `.sub` files via `scan_sub_files()` — extracts `request_cpus`, `request_memory_mb`
4. Calls `WorkflowRuntimePredictor.predict(wf)` — the AI model batch-predicts all jobs
5. Filters results to only the jobs at `--level=N`
6. Writes results **atomically** (write to `.tmp` then `os.replace`) to:
   - `runtime_predictions_L{N}.json`
   - `runtime_predictions_L{N}.csv`
7. Appends a row to `level_prediction_timings.csv`

Atomic rename ensures parallel jobs never read a partial file.

### What it does — Phase 2: Patch this job's `.sub`

> Every process (including those that skipped Phase 1) does this.

1. Waits up to **60 seconds** for the JSON file to appear
2. Reads the JSON — finds the prediction entry matching its `--job-id`
3. Handles **clustered jobs**: if the `.sub` references a `.in` file (list of tasks), it sums predictions for all constituent tasks
4. Calls `patch_sub_file(sub_path, pred)` — writes the prediction into the `.sub`
5. Appends a row to `prescript_timings.csv` — records how long this prescript invocation took

### Output files written

| File | Content |
|------|---------|
| `runtime_predictions_L{N}.json` | Full predictions for DAG level N |
| `runtime_predictions_L{N}.csv` | Same as CSV |
| `level_prediction_timings.csv` | How long prediction computation took per level |
| `prescript_timings.csv` | How long each prescript invocation took per job |

### Error handling

If anything fails, the script exits with code `0` and writes empty fallback JSON/CSV files. The workflow **never stops** because of a prediction failure.

---

## File 4 — `packages/pegasus-runtime/src/Pegasus/runtime/predictor.py`

**Type:** New file (created by us) — the core engine

**When it runs:** Called by `pegasus_runtime_predictor.py`

### What it contains

#### `RuntimePredictionConfig` (dataclass)

Configuration for the predictor. All fields optional:

| Property key | Field | Default |
|---|---|---|
| `pegasus.runtime.prediction.enable` | `enabled` | `True` |
| `pegasus.runtime.prediction.model.path` | `model_path` | auto-discovered |
| `pegasus.runtime.prediction.sparse.threshold` | `sparse_threshold` | from model |
| `pegasus.runtime.prediction.interval.low` | `q_lo` | 0.05 |
| `pegasus.runtime.prediction.interval.high` | `q_hi` | 0.95 |
| `pegasus.runtime.prediction.slot.cpu.count` | `slot_cpu_count` | from condor_status |
| `pegasus.runtime.prediction.slot.ram` | `slot_ram` | from condor_status |

#### `PegasusOracle` (PyTorch neural network)

The same architecture as `testpaper.py` — must match exactly so the `.pkl` is portable:

- **Backbone:** 4 ResBlocks (512-dim) with Mish activation + LayerNorm + Dropout
- **Heads:** `median_head`, `low_offset`, `high_offset` — predicts median + confidence interval
- **VAE branch:** encoder → `fc_mu` / `fc_logvar` → reparameterize → decoder (for anomaly detection)

#### `WorkflowRuntimePredictor`

Main class. `predict(wf, sub_resources)` does:

1. Loads the `.pkl` model file
2. Queries `condor_status` for slot hardware info (CPU count, speed, RAM)
3. For each job: builds a feature vector from transformation name, file sizes, CPU/RAM requests, site info
4. Runs all feature vectors through the neural network in one batch
5. Applies the **routing logic**:
   - `rule_map` hit → use fixed rule value (`RULE_BASED`)
   - training count < sparse threshold → use smart rule stats (`SPARSE`)
   - training count = 0 → neural network prediction (`ZERO_SHOT`)
   - otherwise → neural network prediction (`NORMAL`)
6. Returns a list of prediction dicts per job

#### `scan_sub_files(submit_dir)` — reads `.sub` files

Reads all `.sub` files in parallel (32-thread pool). For each file extracts:

| Field read from `.sub` | Used for |
|---|---|
| `+pegasus_wf_dax_job_id` | Matching predictions to jobs |
| `request_cpus` | Feature input to model |
| `request_memory` | Feature input to model |
| `queue` (cluster `.in` file) | Detecting clustered jobs |

#### `patch_sub_file(sub_path, prediction)` — writes into `.sub`

Inserts 4 HTCondor ClassAd attributes just before the `queue` line, and extends `periodic_remove`:

```
+PredictedRuntime     = 1538
+PredictedRuntimeLow  = 900
+PredictedRuntimeHigh = 3200
+PredictionStatus     = "NORMAL"
periodic_remove = (existing condition) || ((JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > 9600))
```

The `periodic_remove` timeout is `max(upper_bound * 3, 3600)` seconds — a safety net to kill runaway jobs.

#### `find_submit_dir(workflow_yml)` — locates the submit directory

Tries in order:
1. `$PEGASUS_SUBMIT_DIR` environment variable
2. Walk up from CWD looking for `braindump.yml` matching `$PEGASUS_WF_UUID`
3. Search under the workflow YAML directory

---

## File 5 — `packages/pegasus-runtime/src/Pegasus/runtime/models/pegasus_oracle_model.pkl`

**Type:** New file (created by us)

The trained model artifact. Contains:

- The `PegasusOracle` neural network weights
- The `StandardScaler` fitted on training data
- Transformation name encoder (`LabelEncoder`)
- Site encoder
- Routing artifacts: `rule_map`, `smart_rule`, `trans_counts`, `sparse_thr`
- Quantile bounds (`q_lo`, `q_hi`)

Bundled inside the `pegasus-wms.runtime` Python package so it is available anywhere the package is installed.

---

## File 6 — `packages/pegasus-api/src/Pegasus/api/workflow.py`

**Type:** Modified (small addition to existing Pegasus file)

**Lines added:** ~25 lines inside the `Workflow.plan()` method

### What was added

An optional hook that runs prediction **before** the Java planner is called, triggered only when:

```python
wf.plan(**{"pegasus.runtime.prediction.enable": "true"})
```

This is a **secondary** integration path for users of the Python API. The primary path is `bin/pegasus-plan` (the wrapper), which works for all users regardless of how they invoke the planner.

---

## Installation

### Option A — pip install

```bash
cd PegasusIntegration/pegasus
pip install .                      # installs all packages + registers CLI commands
cp packages/pegasus-runtime/src/Pegasus/runtime/models/pegasus_oracle_model.pkl \
   $(python3 -c "import Pegasus.runtime; import os; print(os.path.dirname(Pegasus.runtime.__file__))")/models/
```

### Option B — zero install (no pip required)

```bash
source zero-install-env.sh         # sets PYTHONPATH + puts wrapper scripts on PATH
```

### Option C — one command

```bash
python3 install_pegasus.py
```

---

## Usage

After installation, use Pegasus exactly as before:

```bash
pegasus-plan workflow.yml --sites condorpool --dir submit/ --output-sites local
```

Predictions are written automatically to `output/runtime_predictions_L{N}.json`.

To disable prediction for one run:

```bash
PEGASUS_INJECT_SKIP=1 pegasus-plan workflow.yml ...
```

---

## What Pegasus files were changed

| File | Change type | What changed |
|------|-------------|--------------|
| `bin/pegasus-plan` | Modified | Replaced Java launcher with thin wrapper shell script |
| `packages/pegasus-api/src/Pegasus/api/workflow.py` | Modified | Added optional prediction hook in `plan()` |
| `packages/pegasus-python/setup.py` | Modified | Registered CLI entry points for the two new scripts |

All other Pegasus files are **untouched**.
