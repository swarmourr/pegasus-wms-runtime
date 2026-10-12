# Pegasus Runtime Prediction — Prescript Integration

> Deep technical reference for the DAGMan SCRIPT PRE integration.
> Two approaches: **Post-processor** (recommended) and **Wrapper** (legacy).

---

## Table of Contents

1. [Why Prescripts Instead of Injected Jobs](#1-why-prescripts-instead-of-injected-jobs)
2. [Two Approaches Compared](#2-two-approaches-compared)
3. [Approach A — Post-processor (Recommended)](#3-approach-a--post-processor-recommended)
   - 3.1 [Full Flow](#31-full-flow)
   - 3.2 [How `pegasus-inject-prescripts` Works](#32-how-pegasus-inject-prescripts-works)
   - 3.3 [Reading `.sub` Files for Job ID Mapping](#33-reading-sub-files-for-job-id-mapping)
   - 3.4 [Topological Sort from PARENT/CHILD Lines](#34-topological-sort-from-parentchild-lines)
   - 3.5 [What the Modified `.dag` Looks Like](#35-what-the-modified-dag-looks-like)
4. [Approach B — Wrapper (Legacy)](#4-approach-b--wrapper-legacy)
   - 4.1 [Full Flow](#41-full-flow)
   - 4.2 [How `bin/pegasus-plan` Works](#42-how-binpegasus-plan-works)
5. [Execution Phase — Same for Both Approaches](#5-execution-phase--same-for-both-approaches)
   - 5.1 [Per-Job DAGMan Flow](#51-per-job-dagman-flow)
   - 5.2 [Predictor Steps](#52-predictor-steps)
6. [Scalability — 1000 Parallel Jobs at One Level](#6-scalability--1000-parallel-jobs-at-one-level)
7. [Concurrency Safety — Atomic Write](#7-concurrency-safety--atomic-write)
8. [Accuracy Improves Level by Level](#8-accuracy-improves-level-by-level)
9. [Error Handling — Workflow Never Breaks](#9-error-handling--workflow-never-breaks)
10. [HTCondor `.sub` File — Before and After Patching](#10-htcondor-sub-file--before-and-after-patching)
11. [Installation Layouts](#11-installation-layouts)
12. [Configuration Reference](#12-configuration-reference)
13. [Code Map](#13-code-map)

---

## 1. Why Prescripts Instead of Injected Jobs

The first design injected `pegasus-runtime-predictor` as a regular DAG job (like stage-in or cleanup jobs). This had a critical problem:

```
INJECTED JOB APPROACH (old — broken)
─────────────────────────────────────────────────────────────────
  pegasus-plan workflow.yml
        │
        ▼
  Java Planner reads TC ──► "pegasus-runtime-predictor" not found
        │                    for site condorpool
        ▼
  ╔══════════════════════════════════════════════════╗
  ║  ERROR: no entries for pegasus-runtime-predictor ║
  ║         in TC for sites [condorpool]             ║
  ╚══════════════════════════════════════════════════╝
        │
        ▼
  Workflow planning FAILS before any job runs


PRESCRIPT APPROACH (current — correct)
─────────────────────────────────────────────────────────────────
  pegasus-plan (real, untouched)
        │
        ▼
  pegasus-inject-prescripts submit/run0001/
  (reads .dag, injects SCRIPT PRE lines, writes .dag back)
        │
        ▼
  DAGMan runs predictor LOCALLY on submit host
  before each job is submitted to HTCondor
        │
        ▼
  Workflow runs normally + .sub files have +PredictedRuntime
```

| Problem | Injected Job | Prescript |
|---------|-------------|-----------|
| TC entry required | YES — planning fails if missing | NO — runs locally |
| Site selection | YES — must map to a compute site | NO — submit host only |
| Failure mode | Planning error, workflow never starts | `sys.exit(0)`, workflow continues |
| HTCondor slot cost | YES — uses a slot | Zero |
| Local execution guarantee | No | Yes — always submit host |

---

## 2. Two Approaches Compared

```
┌─────────────────────────────────────┬──────────────────────────────────────┐
│  APPROACH A — Post-processor        │  APPROACH B — Wrapper                │
│  (Recommended)                      │  (Legacy)                            │
├─────────────────────────────────────┼──────────────────────────────────────┤
│                                     │                                      │
│  pegasus-plan workflow.yml          │  bin/pegasus-plan (Python wrapper)   │
│  (real Java planner, untouched)     │    reads workflow.yml                │
│           │                         │    topo-sorts jobs                   │
│           ▼                         │    adds dagman.PRE profiles          │
│  submit/run0001/workflow.dag        │    writes _pred_workflow.yml (temp)  │
│  (clean, no prescripts yet)         │    calls bin/pegasus-plan-real       │
│           │                         │    deletes temp file                 │
│           ▼                         │           │                          │
│  pegasus-inject-prescripts          │           ▼                          │
│    reads .dag                       │  submit/run0001/workflow.dag         │
│    reads .sub → maps job IDs        │  (already has SCRIPT PRE lines)      │
│    topo-sorts from PARENT/CHILD     │                                      │
│    injects SCRIPT PRE lines         │                                      │
│    writes .dag back                 │                                      │
│           │                         │                                      │
│           ▼                         │           ▼                          │
│  condor_submit_dag workflow.dag     │  condor_submit_dag workflow.dag      │
│                                     │                                      │
├─────────────────────────────────────┼──────────────────────────────────────┤
│  workflow.yml modified?    NO       │  workflow.yml modified?    NO        │
│  pegasus-plan wrapped?     NO       │  pegasus-plan wrapped?     YES       │
│  pegasus-plan renamed?     NO       │  pegasus-plan renamed?     YES (real)│
│  Extra step needed?        YES (1)  │  Extra step needed?        NO        │
└─────────────────────────────────────┴──────────────────────────────────────┘
```

---

## 3. Approach A — Post-processor (Recommended)

### 3.1 Full Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│  USER RUNS:  pegasus-plan workflow.yml --dir submit/ --sites condorpool  │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
              ─────────────────▼──────────────────
             │   STEP 1 — PLANNING (unchanged)     │
              ────────────────────────────────────
                               │
              ┌────────────────▼────────────────────┐
              │  bin/pegasus-plan  (real Java)       │
              │                                     │
              │  reads workflow.yml                 │
              │  maps jobs to sites                 │
              │  generates .sub files               │
              │  writes workflow.dag                │
              │                                     │
              │  workflow.dag has NO prescripts yet │
              └────────────────┬────────────────────┘
                               │
              ─────────────────▼──────────────────
             │   STEP 2 — POST-PROCESS              │
              ────────────────────────────────────
                               │
              ┌────────────────▼────────────────────┐
              │  pegasus-inject-prescripts           │
              │  submit/run0001/                    │
              │                                     │
              │  1. Find .dag via braindump.yml      │
              │  2. Parse JOB lines                 │
              │  3. Read .sub → get dax_job_id      │
              │  4. Build PARENT/CHILD graph        │
              │  5. Skip system jobs                │
              │  6. Topo-sort → levels              │
              │  7. Inject SCRIPT PRE per job       │
              │  8. Write .dag back                 │
              └────────────────┬────────────────────┘
                               │
              ─────────────────▼──────────────────
             │   STEP 3 — SUBMIT                    │
              ────────────────────────────────────
                               │
              ┌────────────────▼────────────────────┐
              │  condor_submit_dag workflow.dag      │
              │                                     │
              │  DAGMan reads SCRIPT PRE lines      │
              │  Runs predictor before each job     │
              │  Submits .sub to HTCondor            │
              └─────────────────────────────────────┘
```

### 3.2 How `pegasus-inject-prescripts` Works

```
pegasus-inject-prescripts submit/run0001/
          │
          ├── Find braindump.yml in submit/run0001/
          │     braindump.yml contains:
          │       dag: "workflow.dag"
          │       dax: "/home/user/wf/workflow.yml"    ← auto-discovered
          │
          ├── Open workflow.dag
          │     Parse JOB lines  → {dag_id: sub_file}
          │     Parse PARENT/CHILD → dependency graph
          │     Check SCRIPT PRE → skip already-injected jobs
          │
          ├── For each JOB line:
          │     Read <sub_file> → extract +pegasus_wf_dax_job_id
          │     Example:
          │       JOB preprocess_ID0000001  00/00/preprocess_ID0000001.sub
          │                                          │
          │                                          ▼
          │                           read .sub file:
          │                             +pegasus_wf_dax_job_id = "preprocess_0"
          │                                          │
          │                                          ▼
          │                           dax_job_id = "preprocess_0"
          │
          ├── Filter system jobs (no dax_job_id, or prefix in create_dir_/stage_in_/...)
          │
          ├── Topo-sort user jobs into levels (from PARENT/CHILD graph)
          │
          └── Rewrite workflow.dag with SCRIPT PRE lines injected
```

### 3.3 Reading `.sub` Files for Job ID Mapping

The `.dag` file uses internal DAG IDs like `preprocess_ID0000001`. The predictor needs the original DAX job ID like `preprocess_0` to match against prediction results.

Pegasus embeds the original ID as a ClassAd inside every `.sub` file:

```condor
# Inside 00/00/preprocess_ID0000001.sub:
...
+pegasus_wf_dax_job_id = "preprocess_0"    ← this is what we read
...
```

```
dag file ID         →    .sub file         →    dax job ID
───────────────────────────────────────────────────────────
preprocess_ID0000001    preprocess_ID0000001.sub    preprocess_0
findrange_0_ID0000002   findrange_0_ID0000002.sub   findrange_0
findrange_1_ID0000003   findrange_1_ID0000003.sub   findrange_1
analyze_ID0000004       analyze_ID0000004.sub        analyze_0

System jobs (skipped):
create_dir_condorpool_0_ID0000005   →  +pegasus_wf_dax_job_id = "null"  → SKIP
stage_in_local_local_0_ID0000006    →  no ClassAd                        → SKIP
```

### 3.4 Topological Sort from PARENT/CHILD Lines

```
workflow.dag PARENT/CHILD lines:
  PARENT preprocess_ID0000001  CHILD findrange_0_ID0000002
  PARENT preprocess_ID0000001  CHILD findrange_1_ID0000003
  PARENT findrange_0_ID0000002 CHILD analyze_ID0000004
  PARENT findrange_1_ID0000003 CHILD analyze_ID0000004

BFS sort (user jobs only):
  Level 0: [preprocess_ID0000001]           → --level=0
  Level 1: [findrange_0_ID0000002,          → --level=1
             findrange_1_ID0000003]         → --level=1
  Level 2: [analyze_ID0000004]              → --level=2
```

### 3.5 What the Modified `.dag` Looks Like

```
BEFORE injection:                    AFTER injection:
─────────────────────────────────    ────────────────────────────────────────────

                                     SCRIPT PRE preprocess_ID0000001 \
JOB preprocess_ID0000001 \             /usr/bin/pegasus-runtime-predictor \
    00/00/preprocess.sub               /home/user/wf/workflow.yml \
SCRIPT POST preprocess_ID0000001 \     /home/user/wf/output \
    exitcode.sh preprocess.out         --level=0 --job-id=preprocess_0
RETRY preprocess_ID0000001 3
                                     JOB preprocess_ID0000001 \
                                         00/00/preprocess.sub
                                     SCRIPT POST preprocess_ID0000001 \
                                         exitcode.sh preprocess.out
                                     RETRY preprocess_ID0000001 3

                                     SCRIPT PRE findrange_0_ID0000002 \
JOB findrange_0_ID0000002 \            /usr/bin/pegasus-runtime-predictor \
    00/00/findrange_0.sub              --level=1 --job-id=findrange_0
RETRY findrange_0_ID0000002 3
                                     JOB findrange_0_ID0000002 \
                                         00/00/findrange_0.sub
                                     RETRY findrange_0_ID0000002 3

JOB create_dir_condorpool_ID5 \      JOB create_dir_condorpool_ID5 \   ← system job
    00/00/create_dir.sub                 00/00/create_dir.sub              no SCRIPT PRE

PARENT preprocess_ID0000001  \       PARENT preprocess_ID0000001  \
    CHILD findrange_0_ID0000002          CHILD findrange_0_ID0000002   ← unchanged
```

---

## 4. Approach B — Wrapper (Legacy)

### 4.1 Full Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│  USER RUNS:  pegasus-plan workflow.yml --dir submit/ --sites condorpool  │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
              ┌────────────────▼────────────────────┐
              │  bin/pegasus-plan (Python wrapper)   │
              │                                     │
              │  1. Find workflow.yml in args        │
              │  2. Check model exists               │
              │  3. Read workflow.yml (YAML)         │
              │  4. Topo-sort jobs → levels          │
              │  5. Add dagman.PRE to each job       │
              │  6. Write _pred_workflow.yml (temp)  │
              │  7. subprocess → pegasus-plan-real   │
              │  8. Delete _pred_workflow.yml        │
              └────────────────┬────────────────────┘
                               │
              ┌────────────────▼────────────────────┐
              │  bin/pegasus-plan-real (Java)        │
              │  (renamed from bin/pegasus-plan)     │
              │                                     │
              │  reads _pred_workflow.yml            │
              │  sees dagman.PRE on each job         │
              │  writes SCRIPT PRE in .dag file      │
              └────────────────┬────────────────────┘
                               │
              ┌────────────────▼────────────────────┐
              │  condor_submit_dag workflow.dag      │
              └─────────────────────────────────────┘
```

### 4.2 How `bin/pegasus-plan` Works

```
bin/pegasus-plan
  │
  ├── _find_workflow_yaml(args)
  │     scan args for *.yml / *.yaml that exists on disk
  │
  ├── _model_path()
  │     1. PEGASUS_RUNTIME_PREDICTION_MODEL env var
  │     2. pegasus.properties
  │     3. importlib.resources → Pegasus.models package
  │     4. source tree fallback
  │     returns None → skip injection, call real planner directly
  │
  ├── _build_temp_workflow(workflow_yml)
  │     read workflow.yml (YAML)
  │     strip any old predictor jobs / PRE profiles
  │     _topo_levels() → levels
  │     for each job: add dagman.PRE profile
  │     write _pred_workflow.yml
  │
  ├── subprocess.run([pegasus-plan-real] + modified_args)
  │     (workflow.yml replaced with _pred_workflow.yml in args)
  │
  └── delete _pred_workflow.yml
```

---

## 5. Execution Phase — Same for Both Approaches

Once the `.dag` file has `SCRIPT PRE` lines (however they got there), DAGMan behaviour is identical.

### 5.1 Per-Job DAGMan Flow

```
DAGMan: "findrange_0 is ready to run"
         │
         ▼
┌────────────────────────────────────────────────────────────┐
│  SCRIPT PRE runs on submit host (local)                    │
│                                                            │
│  pegasus-runtime-predictor                                 │
│    workflow.yml output/ --level=1 --job-id=findrange_0     │
│                                                            │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Step 1 — Compute-Once                               │   │
│  │                                                     │   │
│  │  runtime_predictions_L1.json exists?                │   │
│  │    NO  → run model, write JSON (atomic)             │   │
│  │    YES → skip (another prescript already did this)  │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                            │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Step 2 — Patch own .sub                             │   │
│  │                                                     │   │
│  │  wait for runtime_predictions_L1.json (≤60s)        │   │
│  │  read JSON → find entry for "findrange_0"           │   │
│  │  scan_sub_files() → find findrange_0.sub path       │   │
│  │  patch_sub_file() → insert +PredictedRuntime        │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                            │
│  sys.exit(0)   ← always, even on errors                   │
└────────────────────────────────────────────────────────────┘
         │ exit 0
         ▼
HTCondor submits findrange_0.sub
(now has +PredictedRuntime = 47)
```

### 5.2 Predictor Steps

```
main()
  │
  ├── Parse:  workflow.yml, output_dir, --level=1, --job-id=findrange_0
  │
  ├── json_path = output/runtime_predictions_L1.json
  │
  ├── NOT EXISTS?
  │     ├── _load_workflow_from_yaml(workflow.yml)
  │     ├── _scan_actual_file_sizes(output_dir)   → real disk sizes for L1 inputs
  │     ├── find_submit_dir(workflow.yml)          → locate run dir via UUID
  │     ├── scan_sub_files(submit_dir)             → request_cpus / request_memory
  │     ├── WorkflowRuntimePredictor.predict(wf)   → PegasusOracle neural network
  │     ├── write predictions_L1.json.tmp
  │     └── os.replace(tmp → json)                 → ATOMIC, safe for parallel callers
  │
  ├── EXISTS?
  │     └── "already computed — skipping"
  │
  ├── Wait ≤ 60s for json_path
  │
  └── Patch own .sub:
        read json → find {job_id: "findrange_0"}
        scan_sub_files() → sub_path for findrange_0
        patch_sub_file(sub_path, pred) → ClassAds inserted
```

---

## 6. Scalability — 1000 Parallel Jobs at One Level

```
Level 1: 1000 jobs ready simultaneously
         │
         ├── prescript job_0001  ──► json exists? NO
         │     Load model, run inference for all 1000 jobs
         │     os.replace() → runtime_predictions_L1.json  [ATOMIC]
         │     Patch job_0001.sub
         │     Total: ~500ms
         │
         ├── prescript job_0002  ──► json exists? YES (or races → wait loop)
         │     Skip computation
         │     Read json
         │     Patch job_0002.sub
         │     Total: ~5ms
         │
         ├── prescript job_0003  ... same as 0002 ...
         │
         └── prescript job_1000  ... same as 0002 ...

Result:
  Model loaded:   1 time   (not 1000)
  Inference:      1 time   (not 1000)
  .sub patching: 1000 times (each patches only its own file — no conflict)

Time comparison:
  Naive:   1000 × 500ms = ~500 seconds of ML inference
  Actual:  500ms (first) + ~5ms × 999 (read JSON + patch) ≈ 5 seconds
```

---

## 7. Concurrency Safety — Atomic Write

```
Without atomic write:                   With atomic write (os.replace):
──────────────────────────────────      ─────────────────────────────────────────
writer opens runtime_predictions.json   writer opens runtime_predictions.json.tmp
writer writes {"level":1, "pred         writer writes complete JSON
                                        writer: os.replace(.tmp → .json)
reader opens runtime_predictions.json              │
reader reads {"level":1, "pred          └──► POSIX atomic rename:
                                              readers see either absent file
reader: JSON PARSE ERROR                      or the complete new file
        (truncated mid-write!)                NEVER a partial write

Race scenario (two prescripts both see json absent):
  t=0ms    prescript A: exists? NO
  t=1ms    prescript B: exists? NO   ← both enter compute branch
  t=500ms  prescript A: os.replace() → json written
  t=501ms  prescript B: os.replace() → overwrites A  ← harmless: same model, same inputs
  t=502ms  prescript C: exists? YES  → skips compute
  All three: read json, patch own .sub
```

---

## 8. Accuracy Improves Level by Level

```
LEVEL 0 — root jobs
  File sizes: YAML static values only
  ┌──────────────────────────────────────────────────────┐
  │  workflow.yml says: input.txt = 1,048,576 bytes      │
  │  (estimated at workflow definition time)             │
  └──────────────────────────────────────────────────────┘
  Accuracy: LOWER

LEVEL 1 — after Level 0 completes
  Level 0 outputs are now real files on disk.
  _scan_actual_file_sizes() walks output/ and scratch/ directories:
  ┌──────────────────────────────────────────────────────┐
  │  scratch/f.a1 → 3,847,291 bytes  (real)              │
  │  scratch/f.a2 → 2,103,445 bytes  (real)              │
  │  These override the YAML static values               │
  └──────────────────────────────────────────────────────┘
  Accuracy: HIGHER

Level N — each level uses real sizes from all completed predecessors
  ┌─────────┬────────────────────────────────────────────┐
  │ Level 0 │ YAML static sizes only                     │
  │ Level 1 │ Real sizes from Level 0 outputs            │
  │ Level 2 │ Real sizes from Level 0 + 1 outputs        │
  │ Level N │ Real sizes from all N predecessor levels   │
  └─────────┴────────────────────────────────────────────┘

How file sizes are found at runtime:
  _scan_actual_file_sizes() knows all LFN names from workflow.yml
  Walks: output_dir, scratch/, data/, up to 4 parent directories
  Match: if filename == known LFN → record real size
  If not found: fall back to YAML size, or 0 (VAE widens interval)
```

---

## 9. Error Handling — Workflow Never Breaks

```
_run() entry point
  │
  └── try:
        main()
      except Exception:
        │
        ├── print error + traceback to stderr (visible in DAGMan log)
        │
        ├── write FALLBACK outputs:
        │     runtime_predictions_L1.json → {"predictions": [], "error": "..."}
        │     runtime_predictions_L1.csv  → header only
        │     (so waiting prescripts don't hang on poll loop)
        │
        └── sys.exit(0)   ← EXIT ZERO — DAGMan does NOT fail the job

Failures handled gracefully:
  ┌─────────────────────────────────┬───────────────────────────────────┐
  │ torch not installed             │ workflow continues, no prediction │
  │ model .pkl missing              │ injection skipped at planning     │
  │ workflow.yml parse error        │ empty predictions, exit 0         │
  │ submit dir not found            │ uses YAML profiles instead        │
  │ 60s timeout for json            │ skips .sub patch, exit 0          │
  │ .sub file not writable          │ logs error, exit 0                │
  └─────────────────────────────────┴───────────────────────────────────┘
```

---

## 10. HTCondor `.sub` File — Before and After Patching

```condor
# BEFORE — generated by Java planner
# ──────────────────────────────────────────────────────
universe              = vanilla
executable            = /usr/lib/pegasus/bin/pegasus-kickstart
arguments             = -n diamond::findrange:4.0 /path/to/findrange -i f.b1 -o f.c1
request_cpus          = 1
request_memory        = 1024 MB
request_disk          = 1024 KB
output                = findrange_0_ID0000002.out
error                 = findrange_0_ID0000002.err
log                   = workflow.log
should_transfer_files = YES
+pegasus_wf_dax_job_id = "findrange_0"
periodic_remove       = ( JobStatus == 5 ) || ( JobStatus == 6 )
queue


# AFTER — patched by prescript before HTCondor submits the job
# ──────────────────────────────────────────────────────
universe              = vanilla
executable            = /usr/lib/pegasus/bin/pegasus-kickstart
arguments             = -n diamond::findrange:4.0 /path/to/findrange -i f.b1 -o f.c1
request_cpus          = 1
request_memory        = 1024 MB
request_disk          = 1024 KB
output                = findrange_0_ID0000002.out
error                 = findrange_0_ID0000002.err
log                   = workflow.log
should_transfer_files = YES
+pegasus_wf_dax_job_id = "findrange_0"
+PredictedRuntime     = 47         ← injected by prescript
+PredictedRuntimeLow  = 31         ← injected by prescript
+PredictedRuntimeHigh = 94         ← injected by prescript
+PredictionStatus     = "NORMAL"   ← injected by prescript
periodic_remove       = ( JobStatus == 5 ) || ( JobStatus == 6 ) || \
  ((JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > 282))
  ↑ extended: kills job if runtime > 3× upper bound (min 1 hour)
queue
```

Note: `+pegasus_wf_dax_job_id` is how `pegasus-inject-prescripts` finds the original job ID.

---

## 11. Installation Layouts

### Ant dist (`ant dist`)

```
dist/pegasus-5.1.2/
  bin/
    pegasus-plan                  ← real Java planner shell script (untouched)
    pegasus-plan-real             ← only present if using Approach B (wrapper)
    pegasus-runtime-predictor     ← predictor launcher
    pegasus-inject-prescripts     ← NEW: post-processor for Approach A
    pegasus-analyzer
    pegasus-status
    ...
  lib/
    python3.x/site-packages/
      Pegasus/
        cli/
          pegasus_runtime_predictor.py
        runtime_predictor.py
        models/
          pegasus_oracle_model.pkl
```

### pip install from git

```bash
pip install \
  "git+https://github.com/swarmourr/pegasus-wms-runtime.git@integration#subdirectory=packages/pegasus-common" \
  "git+https://github.com/swarmourr/pegasus-wms-runtime.git@integration#subdirectory=packages/pegasus-api" \
  "git+https://github.com/swarmourr/pegasus-wms-runtime.git@integration#subdirectory=packages/pegasus-worker" \
  "git+https://github.com/swarmourr/pegasus-wms-runtime.git@integration#subdirectory=packages/pegasus-python"
# ML deps (torch, scikit-learn, numpy, pandas) installed automatically
```

---

## 12. Configuration Reference

`pegasus.properties` in your workflow directory:

```properties
pegasus.runtime.prediction.enable            = true
pegasus.runtime.prediction.model.path        = /path/to/pegasus_oracle_model.pkl
pegasus.runtime.prediction.output.dir        = /path/to/output
pegasus.runtime.prediction.interval.low      = 0.05
pegasus.runtime.prediction.interval.high     = 0.95
pegasus.runtime.prediction.sparse.threshold  = 10
pegasus.runtime.prediction.slot.hostname     = compute-1.isi.edu
pegasus.runtime.prediction.slot.cpu.count    = 8
pegasus.runtime.prediction.slot.cpu.speed    = 3000
pegasus.runtime.prediction.slot.ram          = 8388608
```

Environment variable overrides:

```bash
PEGASUS_RUNTIME_PREDICTION=false      pegasus-plan workflow.yml ...
PEGASUS_RUNTIME_PREDICTION_MODEL=...  pegasus-plan workflow.yml ...
```

---

## 13. Code Map

```
bin/pegasus-inject-prescripts          ← NEW (Approach A)
  Role:    Post-processor — reads .dag, injects SCRIPT PRE, writes .dag back
  Called:  After pegasus-plan, before condor_submit_dag
  Does:    braindump.yml discovery → .dag parse → .sub read → topo-sort →
           SCRIPT PRE injection → .dag rewrite

bin/pegasus-plan                       ← Approach B wrapper (or real planner if unused)
  Role:    Intercepts pegasus-plan, injects dagman.PRE into temp workflow.yml
  Called:  User command line (replaces real planner)
  Does:    workflow.yml read → topo-sort → PRE profiles → temp file → pegasus-plan-real

bin/pegasus-plan-real                  ← Approach B only
  Role:    Original Java planner shell script (renamed)
  Called:  bin/pegasus-plan via subprocess

bin/pegasus-runtime-predictor
  Role:    Thin shim — sets sys.path then delegates to Pegasus package
  Called:  DAGMan SCRIPT PRE at workflow runtime (both approaches)
  Does:    sys.path insert → import → _run()

packages/pegasus-python/src/Pegasus/cli/pegasus_runtime_predictor.py
  Role:    Canonical predictor implementation
  Entry:   _run() → main()
  Does:    compute-once → atomic JSON write → poll-wait → patch own .sub

packages/pegasus-python/src/Pegasus/runtime_predictor.py
  Role:    Core ML logic and utilities
  Provides:
    WorkflowRuntimePredictor.predict()  → PegasusOracle forward pass
    find_submit_dir()                   → UUID-match braindump.yml → run dir
    scan_sub_files()                    → reads +pegasus_wf_dax_job_id from .sub
    patch_sub_file()                    → inserts +PredictedRuntime ClassAds
    read_meta_sizes()                   → reads .meta files for real file sizes

packages/pegasus-python/src/Pegasus/models/pegasus_oracle_model.pkl
  Role:    Trained PyTorch model + preprocessing artifacts
  Contains: PegasusOracle weights, TF-IDF vectorizer, SVD, trans_map, bucket_map
```

---

*Pegasus WMS 5.1.2-dev · PegasusOracle runtime prediction · prescript integration*
*Branch: `integration` — https://github.com/swarmourr/pegasus-wms-runtime*
