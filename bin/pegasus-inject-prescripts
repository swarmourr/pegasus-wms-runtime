#!/usr/bin/env python3
"""
pegasus-inject-prescripts — post-process a Pegasus submit directory.

Reads the generated .dag file AFTER pegasus-plan runs, computes topological
levels from PARENT/CHILD lines, and injects SCRIPT PRE lines so DAGMan runs
pegasus-runtime-predictor before each user job.

Benefits over the pegasus-plan wrapper:
  - pegasus-plan is NEVER wrapped or replaced
  - workflow.yml is NEVER modified or even read
  - No pegasus-plan-real rename needed
  - Pure post-processing: reads .dag, writes .dag

Usage:
    pegasus-inject-prescripts <submit_dir> [workflow_yml] [output_dir]

    submit_dir   — path to the Pegasus submit directory (contains braindump.yml)
    workflow_yml — path to workflow.yml (optional: auto-discovered from braindump.yml)
    output_dir   — where to write prediction JSON/CSV (optional: <workflow_dir>/output)

Typical workflow:
    pegasus-plan workflow.yml --dir submit/ --sites condorpool ...
    pegasus-inject-prescripts submit/run0001/
    condor_submit_dag submit/run0001/<workflow>.dag
"""

import os
import re
import sys
from pathlib import Path

try:
    import yaml as _yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False


# ── Pegasus system job prefixes — these get no prescript ─────────────────────
_SYSTEM_PREFIXES = (
    "create_dir_",
    "stage_in_",
    "stage_out_",
    "cleanup_",
    "register_",
    "chmod_",
    "pegasus_",
)


def _find_predictor() -> str:
    import shutil
    found = shutil.which("pegasus-runtime-predictor")
    if found:
        return found
    this_bin = Path(__file__).resolve().parent
    local = this_bin / "pegasus-runtime-predictor"
    if local.exists():
        return str(local)
    raise FileNotFoundError(
        "pegasus-runtime-predictor not found. "
        "Install it via pip or ensure it is on PATH."
    )


def _find_dag_and_braindump(submit_dir: Path):
    """
    Locate the .dag file and braindump.yml inside submit_dir.
    Returns (dag_path, braindump_path) or raises.
    """
    braindump = submit_dir / "braindump.yml"
    if not braindump.exists():
        raise FileNotFoundError(f"braindump.yml not found in {submit_dir}")

    dag_path = None
    if _HAS_YAML:
        with open(braindump) as f:
            bd = _yaml.safe_load(f)
        dag_name = bd.get("dag")
        if dag_name:
            dag_path = submit_dir / dag_name
    else:
        # fallback: find first .dag file
        dags = list(submit_dir.glob("*.dag"))
        if dags:
            dag_path = dags[0]

    if not dag_path or not dag_path.exists():
        raise FileNotFoundError(f"Could not locate .dag file in {submit_dir}")

    return dag_path, braindump


def _workflow_yml_from_braindump(braindump: Path) -> str | None:
    """Read workflow.yml path from braindump.yml 'dax' field."""
    if not _HAS_YAML:
        return None
    try:
        with open(braindump) as f:
            bd = _yaml.safe_load(f)
        dax = bd.get("dax")
        if dax and Path(dax).exists():
            return str(Path(dax).resolve())
    except Exception:
        pass
    return None


def _dax_job_id_from_sub(sub_path: Path) -> str | None:
    """
    Read +pegasus_wf_dax_job_id from a .sub file.
    Returns the original DAX job ID (e.g. 'preprocess_0') or None for system jobs.
    """
    try:
        for line in sub_path.read_text(errors="replace").splitlines():
            m = re.search(
                r'\+pegasus_wf_dax_job_id\s*=\s*"?([^"\s]+)"?',
                line,
                re.IGNORECASE,
            )
            if m:
                val = m.group(1).strip('"').strip()
                if val and val.lower() != "null":
                    return val
    except (OSError, PermissionError):
        pass
    return None


def _parse_dag(dag_path: Path, sub_dir: Path):
    """
    Parse the .dag file.

    Returns:
        jobs      — {dag_id: {"sub_file": str, "dax_id": str|None}}
        parents   — {dag_id: set_of_parent_dag_ids}
        lines     — original lines list
        has_pre   — set of dag_ids that already have SCRIPT PRE
    """
    lines   = dag_path.read_text(errors="replace").splitlines(keepends=True)
    jobs    = {}   # dag_id → {sub_file, dax_id}
    parents = {}   # dag_id → set of parent dag_ids
    has_pre = set()

    for line in lines:
        stripped = line.strip()

        # JOB <id> <sub_file> [DONE]
        m = re.match(r'^JOB\s+(\S+)\s+(\S+)', stripped, re.IGNORECASE)
        if m:
            dag_id   = m.group(1)
            sub_file = m.group(2)
            sub_path = (sub_dir / sub_file) if not Path(sub_file).is_absolute() else Path(sub_file)
            dax_id   = _dax_job_id_from_sub(sub_path)
            jobs[dag_id] = {"sub_file": sub_file, "dax_id": dax_id}
            parents.setdefault(dag_id, set())
            continue

        # PARENT <id> [<id> ...] CHILD <id> [<id> ...]
        m = re.match(r'^PARENT\s+(.+?)\s+CHILD\s+(.+)', stripped, re.IGNORECASE)
        if m:
            parent_ids = m.group(1).split()
            child_ids  = m.group(2).split()
            for child in child_ids:
                parents.setdefault(child, set()).update(parent_ids)
            for pid in parent_ids:
                parents.setdefault(pid, set())
            continue

        # SCRIPT PRE already present
        m = re.match(r'^SCRIPT\s+PRE\s+(\S+)', stripped, re.IGNORECASE)
        if m:
            has_pre.add(m.group(1))

    return jobs, parents, lines, has_pre


def _user_jobs(jobs: dict) -> dict:
    """Filter out Pegasus system jobs — keep only user jobs."""
    result = {}
    for dag_id, info in jobs.items():
        dax_id = info.get("dax_id")
        if dax_id is None:
            continue  # no +pegasus_wf_dax_job_id → system job
        if any(dag_id.startswith(p) for p in _SYSTEM_PREFIXES):
            continue
        result[dag_id] = info
    return result


def _topo_levels(user_jobs: dict, parents: dict) -> dict:
    """
    Topological BFS sort of user jobs.
    Returns {dag_id: level_index}.
    """
    user_set = set(user_jobs)

    # Only consider parent edges between user jobs
    user_parents = {
        jid: {p for p in parents.get(jid, set()) if p in user_set}
        for jid in user_set
    }

    level_map = {}
    assigned  = set()
    remaining = set(user_set)

    level_idx = 0
    while remaining:
        ready = [j for j in remaining if user_parents[j].issubset(assigned)]
        if not ready:
            ready = list(remaining)  # cycle fallback
        for jid in ready:
            level_map[jid] = level_idx
        assigned.update(ready)
        remaining -= set(ready)
        level_idx += 1

    return level_map


def _inject(dag_path: Path, sub_dir: Path, workflow_yml: str, output_dir: str, predictor: str):
    """
    Read the .dag file, inject SCRIPT PRE lines for user jobs, write back.
    Returns (injected_count, skipped_count).
    """
    jobs, parents, lines, has_pre = _parse_dag(dag_path, sub_dir)
    user_jobs = _user_jobs(jobs)

    if not user_jobs:
        print("[pegasus-inject-prescripts] No user jobs found — nothing to inject.")
        return 0, 0

    level_map = _topo_levels(user_jobs, parents)

    injected = 0
    skipped  = 0
    new_lines = []

    for line in lines:
        stripped = line.strip()

        m = re.match(r'^JOB\s+(\S+)\s+(\S+)', stripped, re.IGNORECASE)
        if m:
            dag_id = m.group(1)

            if dag_id in user_jobs and dag_id not in has_pre:
                dax_id    = user_jobs[dag_id]["dax_id"]
                level_idx = level_map.get(dag_id, 0)

                pre_cmd = (
                    f"SCRIPT PRE {dag_id} {predictor}"
                    f" {workflow_yml}"
                    f" {output_dir}"
                    f" --level={level_idx}"
                    f" --job-id={dax_id}\n"
                )
                new_lines.append(pre_cmd)
                injected += 1
            elif dag_id not in user_jobs:
                skipped += 1

        new_lines.append(line if line.endswith("\n") else line + "\n")

    dag_path.write_text("".join(new_lines))
    return injected, skipped


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: pegasus-inject-prescripts <submit_dir> [workflow_yml] [output_dir]",
            file=sys.stderr,
        )
        sys.exit(1)

    submit_dir = Path(sys.argv[1]).resolve()
    if not submit_dir.is_dir():
        print(f"ERROR: {submit_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    # Locate .dag and braindump
    dag_path, braindump = _find_dag_and_braindump(submit_dir)
    sub_dir = dag_path.parent

    # workflow.yml — from arg, braindump, or error
    if len(sys.argv) >= 3:
        workflow_yml = str(Path(sys.argv[2]).resolve())
    else:
        workflow_yml = _workflow_yml_from_braindump(braindump)
        if not workflow_yml:
            print(
                "ERROR: workflow.yml not found. Pass it as a second argument or "
                "ensure braindump.yml contains a valid 'dax' field.",
                file=sys.stderr,
            )
            sys.exit(1)

    # output_dir — from arg or beside workflow.yml
    if len(sys.argv) >= 4:
        output_dir = str(Path(sys.argv[3]).resolve())
    else:
        output_dir = str(Path(workflow_yml).parent / "output")

    # Find predictor
    try:
        predictor = _find_predictor()
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[pegasus-inject-prescripts] DAG:       {dag_path}")
    print(f"[pegasus-inject-prescripts] Workflow:  {workflow_yml}")
    print(f"[pegasus-inject-prescripts] Output:    {output_dir}")
    print(f"[pegasus-inject-prescripts] Predictor: {predictor}")

    injected, skipped = _inject(dag_path, sub_dir, workflow_yml, output_dir, predictor)

    print(
        f"[pegasus-inject-prescripts] Done — "
        f"{injected} SCRIPT PRE lines injected, "
        f"{skipped} system jobs skipped."
    )


if __name__ == "__main__":
    main()
