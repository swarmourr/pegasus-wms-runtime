# Pegasus WMS — Runtime Prediction System

> **Version:** Pegasus 5.1.2-dev + PegasusOracle ML extension  
> **Model:** `pegasus_oracle_model.pkl` — trained on 42,158 job executions across 203 transformations  
> **Status:** Production-ready · Graceful fallback · Zero workflow changes required

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [How the Model Works](#how-the-model-works)
4. [Input Features](#input-features)
5. [Prediction Pipeline](#prediction-pipeline)
6. [Output Reference](#output-reference)
7. [VAE Interpretation](#vae-interpretation)
8. [Anomaly Types](#anomaly-types)
9. [Prediction Status Levels](#prediction-status-levels)
10. [Setup & Deployment](#setup--deployment)
11. [Configuration](#configuration)
12. [Patching HTCondor Submit Files](#patching-htcondor-submit-files)
13. [Getting Real Runtimes](#getting-real-runtimes)
14. [Troubleshooting](#troubleshooting)

---

## Overview

The runtime prediction system adds **one infrastructure job per DAG level** to every Pegasus workflow — the same way stage-in and cleanup jobs are added. Each predictor job:

- Runs **before** the user jobs at that level on the submit machine
- Uses a trained deep learning model to estimate each job's runtime
- Writes predictions to JSON/CSV output files
- Patches HTCondor `.sub` files with `+PredictedRuntime` ClassAds before DAGMan submits the jobs
- Never breaks your workflow — errors produce fallback empty outputs and the workflow continues

```
[predictor-L0] ──► [user jobs L0 — preprocess ...]
                            │
                   [predictor-L1] ──► [user jobs L1 — findrange ...]
                                               │
                                      [predictor-L2] ──► [user jobs L2 — analyze ...]
```

**Key properties:**
- The abstract `workflow.yml` is **never modified**
- Predictor jobs appear in `pegasus-status` and `pegasus-analyzer` as `auxillary` type
- Predictor at **L0** uses static file sizes from `workflow.yml`
- Predictor at **LN** (N ≥ 1) reads **actual file sizes on disk** written by L(N-1) jobs — more accurate
- If no trained model is found, prediction injection is silently disabled

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     pegasus-plan (wrapper)                          │
│  wrappers/pegasus-plan                                              │
│                                                                     │
│  1. Reads workflow.yml                                              │
│  2. Topological-sorts jobs into levels                              │
│  3. Injects one pegasus-runtime-predictor job per level             │
│  4. Writes temp workflow YAML                                       │
│  5. Calls real pegasus-plan with temp file                          │
│  6. Deletes temp file                                               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  (planning time)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Java Planner (real pegasus-plan)                       │
│  - Creates .sub files in run00XX/00/00/                             │
│  - Registers jobs in workflow DB                                    │
│  - Generates Graphviz DAG visualization                             │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  (execution time — DAGMan)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│         pegasus-runtime-predictor (per level, at DAG runtime)       │
│  bin/pegasus-runtime-predictor                                      │
│                                                                     │
│  Step 1 ── Discover submit dir (via PEGASUS_WF_UUID)                │
│  Step 2 ── Scan .sub files for request_cpus / request_memory        │
│  Step 3 ── Scan disk for actual output file sizes                   │
│  Step 4 ── Build feature matrix (one row per job at this level)     │
│  Step 5 ── Forward pass through PegasusOracle neural network        │
│  Step 6 ── VAE anomaly analysis                                     │
│  Step 7 ── Write runtime_predictions_LN.json + .csv                 │
│  Step 8 ── Patch .sub files with +PredictedRuntime ClassAds         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## How the Model Works

### Neural Network: PegasusOracle

PegasusOracle is a deep regression model with a **VAE (Variational Autoencoder)** auxiliary head. It predicts three outputs per job: median runtime, lower bound, and upper bound.

```
Input (17 features)
    │
    ├──► Backbone (ResNet-style, 512 units × 4 residual blocks)
    │        │
    │        ├──► median_head  ──► predicted_runtime_s
    │        ├──► low_offset   ──► lower_bound_s
    │        └──► high_offset  ──► upper_bound_s
    │
    └──► VAE Encoder (17 → 128 → 64 → μ/σ, 8 dims)
             │
             └──► VAE Decoder (8 → 64 → 128 → 17)
                      └──► reconstruction_error (anomaly signal)
```

**Training data:** 42,158 job executions · 203 unique transformations · Multiple sites (ISI, WSU/Hellbender, Anvil, Stampede, Expanse, OSG grid)

**Target variable:** `log1p(runtime_seconds)` — predicted in log space then exponentiated back.

**Residual blocks** use Mish activation + LayerNorm for stable training on heterogeneous workflow data.

### Anchor Runtime

Before the neural network runs, a transformation-specific *anchor* is looked up from the model's stored statistics to seed the `log_anchor_runtime` feature:

```
anchor_runtime =
  trans_map[exact_name]           # per-transformation median (most accurate)
  → synth_map[job_id_prefix]      # numeric job-id cluster median
  → bucket_map[(bytes_bin, cpus, site)]  # resource-bucket median (75 buckets)
  → global_median (164 s)         # last resort
```

### Name Embedding

The transformation name is encoded as an 8-dimensional vector using:
1. **TF-IDF** character n-grams (2–4 chars, 512 vocabulary, sublinear TF)
2. **TruncatedSVD** (8 components) trained on all 203 known transformation names

This allows the model to find similar known transformations even for completely new job names via cosine similarity in character space.

### Transformation Name Resolution

Before feature engineering, raw names are resolved to canonical training names via a four-step lookup:

| Step | Example | Result |
|------|---------|--------|
| 1. Exact match | `"diamond::preprocess:4.0"` | → direct hit |
| 2. Strip namespace | `"diamond::preprocess:4.0"` → `"preprocess:4.0"` | → check again |
| 3. Strip version | `"preprocess:4.0"` → `"preprocess"` | → check again |
| 4. Reverse bare-name lookup | `"preprocess"` | → `"diamond::preprocess:4.0"` (301 samples, highest count) |

---

## Input Features

The model takes **17 features** per job: 9 numeric + 8 name-embedding dimensions.

| # | Feature | Source at prediction time | Description |
|---|---------|--------------------------|-------------|
| 0 | `log_anchor_runtime` | pkl `trans_map` / `bucket_map` | log1p of the per-transformation median runtime from training |
| 1 | `log_cpu_power` | condor_status slot | log1p(machine_cpu_count × cpu_speed_MHz) |
| 2 | `log_ram` | .sub file `request_memory` | log1p(requested memory in kB) |
| 3 | `log_input_bytes` | disk scan + YAML File.size | log1p(total input file bytes) |
| 4 | `log_io_intensity` | derived | log1p(input_bytes / input_files_count) — bytes per file |
| 5 | `log_compute_intensity` | derived | log1p(input_bytes / cpu_power) — data per compute unit |
| 6 | `memory_pressure` | derived | request_memory / request_cpus — memory per requested CPU |
| 7 | `site_encoded` | condor_status hostname | Integer site bucket (see table below) |
| 8 | `log_input_files_count` | workflow definition | log1p(number of input files) |
| 9–16 | name embedding dims 0–7 | TF-IDF + SVD on job name | 8-dimensional character n-gram embedding |

> **Note on `cpu_count`:** Training data recorded the actual machine CPU count from kickstart output (whole-node allocations). The predictor therefore uses `slot["cpu_count"]` from `condor_status` for this feature — not `request_cpus`. The `request_cpus` value is used only for `memory_pressure`.

### Site Encoding

| Code | Sites |
|------|-------|
| 0 | ISI (`*.isi.edu`) |
| 1 | WSU / Hellbender (`*wsu*`, `*hellbender*`, `*wustl*`) |
| 2 | Anvil / Purdue (`*anvil*`, `*purdue*`) |
| 3 | Stampede / TACC / Frontera (`*stampede*`, `*tacc*`, `*frontera*`) |
| 4 | Expanse / SDSC (`*expanse*`, `*sdsc*`) |
| 5 | All other / OSG / unknown (73 % of training data) |

---

## Prediction Pipeline

At execution time, for each DAG level:

```
1. find_submit_dir()
     └── Uses PEGASUS_WF_UUID env var to UUID-match braindump.yml
         → locates exact current run's submit directory

2. scan_sub_files(submit_dir)
     └── rglob("*.sub") through run00XX/00/00/
         → extracts {dax_job_id: {request_cpus, request_memory_mb, sub_path}}

3. _scan_actual_file_sizes(output_dir)
     └── Walks disk for files matching known LFNs
         → overrides YAML static sizes with real sizes

4. _extract_features(workflow, slot, arts, sub_resources)
     └── One row per job at this level:
         cpu_count    = slot["cpu_count"]  (machine total — matches training)
         request_cpus = from .sub file or YAML profile
         ram          = from .sub file (request_memory) or slot default
         input_bytes  = disk scan → YAML → 0
         hostname     = job selector profile → slot hostname

5. _make_features(df, arts)
     └── Resolves transformation names → canonical training names
         Computes anchor_runtime (4-level fallback)
         Computes all 9 numeric features
         Generates 8-dim name embedding via TF-IDF + SVD

6. PegasusOracle.forward(X)
     └── Returns (median, low, high), reconstruction, mu, logvar

7. VAE anomaly analysis
     └── per_feat_err = (X - recon)²  →  anomaly_type per job
         kl           = 0.5 Σ(μ² + e^σ - σ - 1)
         anomaly_score = sigmoid-normalised(recon_err + 0.1 × kl)
         confidence   = 1 - anomaly_score
         interval widened by up to 2× for high-anomaly jobs

8. Write runtime_predictions_LN.json + .csv

9. patch_sub_file() for each job
     └── Inserts ClassAds before the "queue" line in each .sub file
```

---

## Output Reference

### JSON (`runtime_predictions_LN.json`)

```json
{
  "level": 0,
  "total_jobs": 2,
  "predictions": [
    {
      "job_id":              "preprocess_0",
      "transformation":      "diamond::preprocess:4.0",
      "dag_level":           0,
      "status":              "NORMAL",

      "predicted_runtime_s": 84.2,
      "lower_bound_s":       53.1,
      "upper_bound_s":       198.4,
      "interval_pct":        90,
      "min_wall_time_mins":  1,
      "max_wall_time_mins":  4,

      "input_files_count":   1,
      "input_bytes_total":   1048576,
      "cpu_count":           4,
      "hostname":            "compute-3.isi.edu",

      "vae_anomaly_score":   0.181,
      "vae_confidence":      0.819,
      "vae_anomaly_types": [
        {"type": "unknown_transformation_name", "contribution_pct": 64.0},
        {"type": "unusual_site",                "contribution_pct": 18.6},
        {"type": "unusual_data_size",           "contribution_pct": 7.0}
      ],
      "vae_latent_mu": [0.0101, -0.0063, 1.2183, 0.4521, -0.1234, 0.8901, 0.2345, -0.5678]
    }
  ]
}
```

### Field Reference

| Field | Type | Description |
|-------|------|-------------|
| `job_id` | string | Pegasus DAX job ID |
| `transformation` | string | Resolved canonical transformation name |
| `dag_level` | int | DAG topological level (0 = root) |
| `status` | string | Prediction confidence tier (see [Status Levels](#prediction-status-levels)) |
| `predicted_runtime_s` | float | Median predicted wall-clock runtime in seconds |
| `lower_bound_s` | float | Lower bound of prediction interval |
| `upper_bound_s` | float | Upper bound (widened by VAE anomaly score) |
| `interval_pct` | int | Confidence interval percentage (default: 90) |
| `min_wall_time_mins` | int | Minimum wall time to request (ceil of lower_bound) |
| `max_wall_time_mins` | int | Maximum wall time to request (ceil of upper_bound) |
| `input_files_count` | int | Number of input files |
| `input_bytes_total` | float | Total input data size in bytes |
| `cpu_count` | int | Machine CPU count used for prediction |
| `hostname` | string | Execution site hostname or site name |
| `vae_anomaly_score` | float 0–1 | How far this job is from the training distribution |
| `vae_confidence` | float 0–1 | Prediction reliability (1 = fully in-distribution) |
| `vae_anomaly_types` | list | Ranked feature groups driving the anomaly (see below) |
| `vae_latent_mu` | list[8] | Job position in VAE latent space (8-dim fingerprint) |

### CSV (`runtime_predictions_LN.csv`)

Same fields as JSON predictions, one row per job. `vae_latent_mu` and `vae_anomaly_types` are serialised as strings.

---

## VAE Interpretation

The VAE (Variational Autoencoder) auxiliary head gives the model self-awareness about how reliable its own predictions are.

### How it works

During inference, the VAE reconstructs the 17 input features from an 8-dimensional latent representation. The **reconstruction error** measures how well the model "understands" this job:

```
reconstruction_error = MSE(input_features, reconstructed_features)
kl_divergence        = 0.5 × Σ(μ² + e^logvar - logvar - 1)
anomaly_score        = sigmoid-normalise(recon_error + 0.1 × kl)
```

### Confidence thresholds

| `vae_confidence` | Meaning | Recommended action |
|-----------------|---------|-------------------|
| ≥ 0.8 | High confidence — job closely matches training data | Trust the prediction |
| 0.5 – 0.8 | Medium confidence | Use `upper_bound_s` for wall time requests |
| 0.2 – 0.5 | Low confidence — unusual job | Add extra buffer; consider retraining |
| < 0.2 | Very low — job is out-of-distribution | Use conservative fallback; retrain with this job type |

### Latent space (`vae_latent_mu`)

The 8-dimensional `vae_latent_mu` vector is the job's "fingerprint". Jobs with similar latent vectors have similar runtime characteristics regardless of their name. You can use these vectors to:
- Cluster jobs by behaviour
- Find similar historical jobs across workflows
- Detect when new workflows resemble known patterns

### Interval widening

For anomalous jobs, the `[lower_bound_s, upper_bound_s]` interval is **automatically widened** proportionally to the anomaly score — up to **2× wider** when `anomaly_score → 1`. This keeps the interval calibrated even for completely unseen jobs.

---

## Anomaly Types

`vae_anomaly_types` lists the feature groups most responsible for the anomaly, sorted by contribution:

```json
"vae_anomaly_types": [
  {"type": "unknown_transformation_name", "contribution_pct": 60.1},
  {"type": "unusual_memory_cpu_ratio",    "contribution_pct": 15.4}
]
```

Only types contributing > 5 % of total reconstruction error are included.

| Anomaly Type | Root Cause | What to Do |
|---|---|---|
| `unknown_transformation_name` | Job name never seen in training data (character n-gram mismatch) | Run more workflows with this transformation type to retrain |
| `unknown_transformation` | No historical runtime anchor (`trans_map` miss at all levels) | Same — collect execution data and retrain |
| `unusual_cpu` | CPU count or speed outside training range | Expected when moving to a new cluster; collect data |
| `unusual_memory` | Memory request outside training range | Check if `request_memory` is set correctly in the workflow |
| `unusual_memory_cpu_ratio` | Unusual ratio of memory to requested CPUs | Verify job resource profiles are correct |
| `unusual_data_size` | Input file size much larger or smaller than training jobs | Normal for different datasets; no action needed |
| `unusual_io_pattern` | Unusual bytes-per-file ratio | Expected for jobs with many small or few large files |
| `unusual_compute_pattern` | Unusual bytes-per-CPU ratio | Expected when data volume or parallelism changes |
| `unusual_site` | Execution site not well-represented in training data | Add site to training data collection |
| `unusual_file_count` | Number of input files outside training distribution | Normal for different workflow configurations |

---

## Prediction Status Levels

| Status | Meaning | Confidence |
|--------|---------|-----------|
| `NORMAL` | Transformation has ≥ 10 training samples; full neural network prediction | Highest |
| `SPARSE` | Transformation has < 10 training samples; uses per-transformation median from `smart_rule` | Medium |
| `RULE_BASED` | System/infrastructure transformation (chmod, transfer, cleanup); uses fixed rule | Medium |
| `ZERO_SHOT` | Transformation never seen in training; uses feature-based neural prediction + name similarity | Lowest |

> All statuses produce predictions. `ZERO_SHOT` jobs still get full VAE anomaly analysis, and their prediction interval is widened automatically.

---

## Setup & Deployment

### Prerequisites

```
Python ≥ 3.9
PyTorch ≥ 2.0
scikit-learn ≥ 1.7
pandas, numpy, pyyaml
Pegasus WMS ≥ 5.1.0 installed separately
```

### Installation

```bash
# Clone the runtime prediction extension
git clone https://github.com/swarmourr/pegasus-wms-runtime.git
cd pegasus-wms-runtime

# Add the wrapper to PATH — must come BEFORE the real Pegasus bin
export PATH=$(pwd)/wrappers:$PATH

# Add to ~/.bashrc for persistence
echo 'export PATH=/path/to/pegasus-wms-runtime/wrappers:$PATH' >> ~/.bashrc
```

### Place the trained model

```bash
cp pegasus_oracle_model.pkl \
   packages/pegasus-python/src/Pegasus/models/pegasus_oracle_model.pkl
```

### Verify

```bash
which pegasus-plan
# Expected: /path/to/pegasus-wms-runtime/wrappers/pegasus-plan

pegasus-plan --help 2>&1 | head -2
# Expected: [pegasus-plan] Runtime prediction enabled (model: ...) ...
```

### Remote machine deployment

```bash
# Pull latest changes on the remote machine
cd /home/hamza/Desktop/runtime_pegasus/pegasus-5.1.2/pegasus-wms-runtime-src
git pull origin main

# Ensure PATH is set correctly
export PATH=$(pwd)/wrappers:$PATH
```

---

## Configuration

All settings go in `pegasus.properties` in your workflow directory:

```properties
# ── Core ───────────────────────────────────────────────────────
# Enable / disable prediction (default: true)
pegasus.runtime.prediction.enable = true

# Path to trained model (default: auto-discovered from package)
pegasus.runtime.prediction.model.path = /path/to/pegasus_oracle_model.pkl

# Output directory for prediction files (default: workflow output dir)
pegasus.runtime.prediction.output.dir = /path/to/output

# ── Prediction interval ────────────────────────────────────────
# Quantiles for lower/upper bounds (default: 0.05 and 0.95)
pegasus.runtime.prediction.interval.low  = 0.05
pegasus.runtime.prediction.interval.high = 0.95

# ── Model tuning ───────────────────────────────────────────────
# Min training samples to use NORMAL instead of SPARSE status (default: 10)
pegasus.runtime.prediction.sparse.threshold = 10

# ── Slot override (skip condor_status query) ───────────────────
pegasus.runtime.prediction.slot.hostname  = compute-1.isi.edu
pegasus.runtime.prediction.slot.cpu.count = 8
pegasus.runtime.prediction.slot.cpu.speed = 3000
pegasus.runtime.prediction.slot.ram       = 8388608
```

### Environment variable override

```bash
# Disable prediction for a single run
PEGASUS_RUNTIME_PREDICTION=false pegasus-plan workflow.yml ...

# Point to a specific model
PEGASUS_RUNTIME_PREDICTION_MODEL=/path/to/model.pkl pegasus-plan workflow.yml ...
```

---

## Patching HTCondor Submit Files

After prediction, each job's `.sub` file is patched in-place with ClassAds before DAGMan submits the job. This allows HTCondor and schedulers to use the predictions for:
- Resource accounting
- Priority decisions
- Timeout / eviction policies

**ClassAds added to each `.sub` file:**

```condor
+PredictedRuntime     = 84        # median predicted seconds
+PredictedRuntimeLow  = 53        # lower bound seconds
+PredictedRuntimeHigh = 198       # upper bound seconds (auto-widened for anomalous jobs)
+PredictionStatus     = "NORMAL"  # NORMAL / SPARSE / ZERO_SHOT / RULE_BASED
```

**`periodic_remove` is also updated:**

```condor
periodic_remove = (<existing condition>) ||
  ((JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > 594))
  # kills job if it runs > 3x the upper bound (min 1 hour)
```

---

## Getting Real Runtimes

After a workflow completes, actual runtimes are in the Pegasus workflow database:

```python
from sqlalchemy import create_engine, text

engine = create_engine("sqlite:////path/to/submit/run00XX/workflow.db")

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT
            j.exec_job_id       AS job_id,
            t.transformation    AS transformation,
            ji.local_duration   AS runtime_s,
            ji.site             AS site,
            h.hostname          AS hostname
        FROM job_instance ji
        JOIN job   j ON j.job_id  = ji.job_id
        JOIN task  t ON t.job_id  = j.job_id
        LEFT JOIN host h ON h.host_id = ji.host_id
        WHERE ji.exitcode = 0
          AND t.transformation NOT LIKE 'pegasus::%'
          AND t.transformation NOT LIKE 'system::%'
    """)).fetchall()
```

Or via the CLI:
```bash
pegasus-statistics -s jobs /path/to/submit/run00XX
```

**Runtime columns:**

| Column | Table | Description |
|--------|-------|-------------|
| `local_duration` | `job_instance` | Wall-clock time (HTCondor view) |
| `remote_duration` | `invocation` | Actual execution time inside kickstart (most accurate) |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `which pegasus-plan` shows system path | Wrapper not first in PATH | `export PATH=/path/to/wrappers:$PATH` |
| `pegasus-config: not found` error | Original Pegasus shell script running instead of wrapper | See above — PATH not set correctly |
| All jobs show `ZERO_SHOT` | Transformation names not in training data | Normal; model still predicts. Collect execution data and retrain |
| Predictions missing from `.sub` files | Submit dir not found at runtime | Check `PEGASUS_WF_UUID` is set; ensure braindump.yml exists |
| Low `vae_confidence` on all jobs | Running on a new site or with unusual resource requests | Expected; use `max_wall_time_mins` for conservative scheduling |
| `No model found` warning | `.pkl` file not placed in models directory | Copy model to `packages/pegasus-python/src/Pegasus/models/` |
| Stale predictions (wrong run) | Multiple workflow runs in same directory | Fixed by UUID matching in `find_submit_dir()` |
| `import torch` fails | PyTorch not installed | `pip install torch` — prediction silently disabled without it |

---

## File Reference

```
pegasus-wms-runtime/
├── wrappers/
│   └── pegasus-plan                         # Python wrapper — intercepts pegasus-plan
├── bin/
│   └── pegasus-runtime-predictor            # Predictor binary — runs at DAG runtime
└── packages/pegasus-python/src/Pegasus/
    ├── runtime_predictor.py                 # Core: model, features, VAE, patch logic
    └── models/
        └── pegasus_oracle_model.pkl         # Trained model (place here)
```

---

*Generated for Pegasus WMS 5.1.2-dev with PegasusOracle runtime prediction extension.*  
*Model trained on 203 transformations · ISI, WSU, Anvil, Stampede, Expanse, OSG.*
