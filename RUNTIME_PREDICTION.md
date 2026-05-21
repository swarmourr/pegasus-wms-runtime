# Pegasus Runtime Prediction

Runtime prediction adds **one infrastructure job per DAG level** to your workflow — exactly like stage-in and cleanup jobs. The predictor runs on the submit machine before each level's user jobs, using an ML model to estimate their runtimes and writes the results to JSON/CSV.

---

## How It Works

```
[predictor-L0] → [Level-0 user jobs]
                         ↓
                 [predictor-L1] → [Level-1 user jobs]
                                          ↓
                                  [predictor-L2] → [Level-2 user jobs]
```

- Predictor at **L0** uses static input sizes from `workflow.yml`
- Predictor at **L1** reads actual file sizes written by L0 jobs (more accurate)
- Predictor at **LN** always waits for all L(N-1) jobs to finish

The **abstract workflow** (`workflow.yml`) is never modified. The predictor jobs are injected transparently into a temporary copy used only during planning, then the Java planner handles:
- Submit file (`.sub`) creation in `run00XX/00/00/`
- Database registration (stampede events)
- `pegasus-status` and `pegasus-analyzer` visibility
- Graphviz visualization

---

## Setup

### 1. Install

```bash
git clone https://github.com/swarmourr/pegasus-wms-runtime.git
cd pegasus-wms-runtime
python3 install_pegasus.py /path/to/pegasus-install
```

This places a `pegasus-plan` wrapper before the real Pegasus binary in your `PATH`.

### 2. Verify

```bash
which pegasus-plan
# Should show: /path/to/pegasus-wms-runtime/wrappers/pegasus-plan
```

### 3. Place the trained model

Copy your trained `pegasus_oracle_model.pkl` to:

```
pegasus-wms-runtime/packages/pegasus-python/src/Pegasus/models/pegasus_oracle_model.pkl
```

---

## Usage

Run `pegasus-plan` exactly as before — no changes to your workflow:

```bash
pegasus-plan workflow.yml --sites condorpool --output-sites local -v
```

The wrapper prints:

```
[pegasus-plan] Runtime prediction enabled — injecting predictor jobs.
```

### Output files (inside each run directory)

```
run00XX/
  runtime_predictions_L0.json   ← Level 0 predictions (before L0 jobs run)
  runtime_predictions_L1.json   ← Level 1 predictions (after L0 jobs finish)
  runtime_predictions_L2.json   ← Level 2 predictions (after L1 jobs finish)
  runtime_predictions_L0.csv
  ...
```

Each JSON file contains:

```json
{
  "level": 0,
  "total_jobs": 2,
  "predictions": [
    {
      "job_id": "ID0000001",
      "transformation": "preprocess",
      "dag_level": 0,
      "status": "NORMAL",
      "predicted_runtime_s": 42.5,
      "lower_bound_s": 31.0,
      "upper_bound_s": 61.2,
      "interval_pct": 90,
      "min_wall_time_mins": 1,
      "max_wall_time_mins": 2
    }
  ]
}
```

**Status values:**
| Status | Meaning |
|--------|---------|
| `NORMAL` | Transformation seen in training data, full model used |
| `SPARSE` | Transformation seen rarely in training, uses smart-rule fallback |
| `RULE_BASED` | System/infrastructure transformation, uses fixed rule |
| `ZERO_SHOT` | Transformation not in training data, uses feature-based prediction |

---

## Enable / Disable

### Via `pegasus.properties` (recommended)

Add to your `pegasus.properties` file:

```properties
# Enable runtime prediction (default: true)
pegasus.runtime.prediction.enable = true

# Set to false to disable
# pegasus.runtime.prediction.enable = false
```

### Via environment variable

```bash
# Disable for a single run
PEGASUS_RUNTIME_PREDICTION=false pegasus-plan workflow.yml --sites condorpool --output-sites local

# Or export to disable for the session
export PEGASUS_RUNTIME_PREDICTION=false
```

The environment variable takes precedence over `pegasus.properties`.

---

## Prediction Job Names

Prediction jobs are named:

```
pegasus_runtime_predictor_L0
pegasus_runtime_predictor_L1
pegasus_runtime_predictor_L2
...
```

They appear in `pegasus-status` as type `auxillary`, same as cleanup and dirmanager jobs.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `which pegasus-plan` shows `/usr/bin/pegasus-plan` | Wrapper not in PATH | Prepend `wrappers/` to `PATH` before Pegasus bin |
| All jobs show `ZERO_SHOT` | Transformation names not in training data | Normal for new workflows; model still predicts using hardware features |
| No prediction jobs in `pegasus-status` | Workflow not yet run | Jobs appear after `pegasus-run` starts and monitord tracks them |
| Predictions use wrong file sizes | Actual output files not found | Check scratch directory is accessible from submit machine |
