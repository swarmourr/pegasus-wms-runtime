"""
PegasusOracle — Level-Aware Runtime Predictor

New prediction approach: predict ONLY the jobs at a requested DAG level,
not the whole workflow.

Key difference from predictor.py (WorkflowRuntimePredictor):
  - OLD: loads workflow → builds features for ALL jobs → runs model on ALL → filters to level N
  - NEW: loads workflow → builds DAG levels → propagates file sizes up to level N
         → builds features ONLY for level N jobs → runs model ONLY on level N jobs

Why we still need to walk levels 0 … N-1:
  File sizes propagate from parent outputs to child inputs. To know how many
  bytes a level-2 job receives, we must know what level-1 jobs produced —
  which in turn depends on what level-0 jobs produced.  We do NOT run the
  neural network on levels 0 … N-1; we only propagate the file_size_map.

Result: for a 500-job workflow with 5 levels of ~100 jobs each, predicting
level 2 runs the neural network on ~100 jobs instead of 500 — a 5× speedup.

Same output format as predictor.py so it is a drop-in alternative.
"""

import json
import math
import os
import pickle
import re
import warnings
from collections import Counter
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# Re-use shared helpers from predictor.py — no duplication
from Pegasus.runtime.predictor import (
    ModelContext,
    PegasusOracle,
    RuntimePredictionConfig,
    _build_dag_levels,
    _condor_profiles,
    _full_trans_name,
    _get_condor_slot,
    _job_execution_site,
    _make_features,
    _assemble_X,
    NUMERIC_COLS,
    _DEFAULT_MODEL,
    read_meta_sizes,
    patch_sub_file,
    scan_sub_files,
    find_submit_dir,
)

_MODELS_DIR = Path(__file__).parent / "models"


# ─────────────────────────────────────────────────────────────
# 1. LEVEL-FILTERED FEATURE EXTRACTION
# ─────────────────────────────────────────────────────────────

def _extract_features_for_level(
    workflow,
    slot: dict,
    arts: dict,
    target_level: int,
    sub_resources: Optional[dict] = None,
    submit_dir: Optional[str] = None,
) -> pd.DataFrame:
    """
    Build feature rows for ONLY the jobs at ``target_level``.

    Walks levels 0 … target_level in topological order so that file sizes
    are propagated correctly from parent outputs to child inputs — but only
    builds the feature DataFrame for jobs sitting at exactly ``target_level``.

    Parameters
    ----------
    workflow      : Pegasus Workflow object
    slot          : condor slot dict (cpu_count, cpu_speed, ram, hostname)
    arts          : model artifacts dict from the .pkl (trans_map, output_ratio …)
    target_level  : 0-based DAG level index to predict
    sub_resources : optional {dax_job_id: {request_cpus, request_memory_mb}}
    submit_dir    : path to submit dir — used to read .meta sizes for completed jobs

    Returns
    -------
    pd.DataFrame with one row per job at target_level, or empty DataFrame if
    target_level is out of range.
    """
    sub_resources = sub_resources or {}

    # ── Seed file_size_map from YAML static sizes ───────────────────────────
    file_size_map: dict = {}
    for jid, job in workflow.jobs.items():
        for f in (job.get_inputs()  if hasattr(job, "get_inputs")  else []):
            if hasattr(f, "size") and f.size:
                file_size_map[f.lfn] = float(f.size)
        for f in (job.get_outputs() if hasattr(job, "get_outputs") else []):
            if hasattr(f, "size") and f.size:
                file_size_map[f.lfn] = float(f.size)

    # ── Override with real .meta sizes for already-completed jobs ───────────
    if submit_dir:
        file_size_map.update(read_meta_sizes(submit_dir))

    output_ratio = arts.get("output_ratio", {})
    levels       = _build_dag_levels(workflow)

    if target_level >= len(levels):
        return pd.DataFrame()

    rows = []

    for level_idx, level in enumerate(levels):
        for jid, job in level:
            if not hasattr(job, "transformation"):
                continue  # SubWorkflow node
            ns = getattr(job, "namespace", None)
            if ns in ("pegasus", "system"):
                continue  # Pegasus infrastructure job

            condor  = _condor_profiles(job)
            sub_res = sub_resources.get(jid, {})

            req_cpus = (
                sub_res.get("request_cpus")
                or int(condor.get("request_cpus", 0))
                or slot["cpu_count"]
            )
            req_mem_mb = (
                sub_res.get("request_memory_mb")
                or float(condor.get("request_memory", 0))
            )
            ram_kb     = req_mem_mb * 1_024 if req_mem_mb > 0 else slot["ram"]
            exec_site  = _job_execution_site(job) or slot["hostname"]

            inputs            = job.get_inputs()  if hasattr(job, "get_inputs")  else []
            input_files_count = len(inputs)
            input_bytes_total = sum(file_size_map.get(f.lfn, 0.0) for f in inputs)

            # ── Only add a feature row for the target level ─────────────────
            if level_idx == target_level:
                rows.append({
                    "job_id":            jid,
                    "transformation":    _full_trans_name(job),
                    "dag_level":         level_idx,
                    "cpu_count":         slot["cpu_count"],
                    "request_cpus":      req_cpus,
                    "cpu_speed":         slot["cpu_speed"],
                    "ram":               ram_kb,
                    "input_bytes_total": float(input_bytes_total),
                    "input_files_count": input_files_count,
                    "hostname":          exec_site,
                })

            # ── Always propagate output sizes (needed for downstream levels) ─
            trans      = _full_trans_name(job)
            ratio      = output_ratio.get(trans, 1.0)
            est_output = input_bytes_total * ratio
            for f in (job.get_outputs() if hasattr(job, "get_outputs") else []):
                if f.lfn not in file_size_map:
                    file_size_map[f.lfn] = est_output

        # ── Stop walking once we have passed target_level ───────────────────
        if level_idx >= target_level:
            break

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ─────────────────────────────────────────────────────────────
# 2. LEVEL-AWARE PREDICTOR  (main class)
# ─────────────────────────────────────────────────────────────

class LevelRuntimePredictor:
    """
    Level-aware runtime predictor — runs the neural network only on the
    jobs at the requested DAG level instead of the whole workflow.

    Usage::

        predictor = LevelRuntimePredictor(cfg)
        results   = predictor.predict(workflow, target_level=1)

    Or via the CLI ``pegasus-runtime-predictor`` with ``--level=N`` — the
    CLI selects this predictor automatically when ``--strategy=level``
    (or will in a future update).

    Output files written to ``output_dir``:
        runtime_predictions_L{N}.json
        runtime_predictions_L{N}.csv
    """

    def __init__(self, config: RuntimePredictionConfig):
        if not config.model_path:
            if _DEFAULT_MODEL.exists():
                config.model_path = str(_DEFAULT_MODEL)
            else:
                raise ValueError(
                    f"No model found. Place pegasus_oracle_model.pkl in {_MODELS_DIR} "
                    "or set pegasus.runtime.prediction.model.path."
                )
        if not os.path.isfile(config.model_path):
            raise FileNotFoundError(f"Model not found: {config.model_path}")

        self._cfg = config
        self._ctx = ModelContext(config.model_path)

        if config.sparse_threshold is not None:
            self._ctx.sparse_thr = config.sparse_threshold
        if config.q_lo is not None:
            self._ctx.q_lo = config.q_lo
        if config.q_hi is not None:
            self._ctx.q_hi = config.q_hi

    def predict(
        self,
        workflow,
        target_level: int,
        sub_resources: Optional[dict] = None,
        submit_dir: Optional[str] = None,
    ) -> List[dict]:
        """
        Predict runtimes for all jobs at ``target_level`` only.

        Parameters
        ----------
        workflow      : Pegasus Workflow object
        target_level  : 0-based DAG level (0 = source jobs, 1 = next layer …)
        sub_resources : optional resource dict from scan_sub_files()
        submit_dir    : path to submit directory (for reading .meta sizes)

        Returns
        -------
        List of prediction dicts — one per job at target_level.
        Same schema as WorkflowRuntimePredictor.predict() output.
        """
        if not self._cfg.enabled:
            return []

        output_dir = (
            self._cfg.output_dir
            or getattr(workflow, "_submit_dir", None)
            or str(Path.cwd())
        )
        output_dir = str(output_dir)
        os.makedirs(output_dir, exist_ok=True)

        # ── Step 1: resolve compute slot ────────────────────────────────────
        slot = _get_condor_slot()
        if self._cfg.slot_hostname  is not None: slot["hostname"]  = self._cfg.slot_hostname
        if self._cfg.slot_cpu_count is not None: slot["cpu_count"] = self._cfg.slot_cpu_count
        if self._cfg.slot_cpu_speed is not None: slot["cpu_speed"] = self._cfg.slot_cpu_speed
        if self._cfg.slot_ram       is not None: slot["ram"]       = self._cfg.slot_ram

        # ── Step 2: extract features for target_level ONLY ──────────────────
        # (walks 0 … target_level for file-size propagation, but only
        #  builds feature rows for target_level — no neural network on others)
        df = _extract_features_for_level(
            workflow, slot, self._ctx.arts,
            target_level  = target_level,
            sub_resources = sub_resources,
            submit_dir    = submit_dir or output_dir,
        )
        if df.empty:
            print(f"[LevelRuntimePredictor] No user jobs found at level {target_level}")
            return []

        print(
            f"[LevelRuntimePredictor] Level {target_level}: "
            f"{len(df)} job(s) — running inference on these only"
        )

        # ── Step 3: batch forward pass on level-N jobs only ─────────────────
        p_med, p_low, p_high, statuses, anomaly, confidence, latent_mu, anomaly_types = \
            self._ctx.predict(df)
        ip_pct = int(round((self._ctx.q_hi - self._ctx.q_lo) * 100))

        # ── Step 4: assemble result rows ─────────────────────────────────────
        results = []
        for i, row in enumerate(df.itertuples(index=False)):
            results.append({
                "job_id":              row.job_id,
                "transformation":      row.transformation,
                "dag_level":           int(row.dag_level),
                "status":              statuses[i],
                "predicted_runtime_s": round(float(p_med[i]), 1),
                "lower_bound_s":       round(float(p_low[i]),  1),
                "upper_bound_s":       round(float(p_high[i]), 1),
                "interval_pct":        ip_pct,
                "min_wall_time_mins":  max(1, math.ceil(float(p_low[i])  / 60)),
                "max_wall_time_mins":  max(1, math.ceil(float(p_high[i]) / 60)),
                "input_files_count":   int(row.input_files_count),
                "input_bytes_total":   float(row.input_bytes_total),
                "cpu_count":           int(row.cpu_count),
                "hostname":            row.hostname,
                "vae_anomaly_score":   round(float(anomaly[i]),    4),
                "vae_confidence":      round(float(confidence[i]), 4),
                "vae_anomaly_types":   anomaly_types[i],
                "vae_latent_mu":       [round(float(v), 4) for v in latent_mu[i]],
            })

        # ── Step 5: write level-specific output files ─────────────────────────
        suffix   = f"_L{target_level}"
        csv_path = os.path.join(output_dir, f"runtime_predictions{suffix}.csv")
        pd.DataFrame(results).to_csv(csv_path, index=False)

        summary = {
            "level":         target_level,
            "total_jobs":    len(results),
            "status_counts": dict(Counter(statuses)),
            "interval_pct":  ip_pct,
            "model_path":    self._cfg.model_path,
            "slot_used":     slot,
            "predictions":   results,
        }
        json_path = os.path.join(output_dir, f"runtime_predictions{suffix}.json")
        with open(json_path, "w") as fh:
            json.dump(summary, fh, indent=2)

        print(f"[LevelRuntimePredictor] Written → {json_path}")
        return results
