"""
Tests for pegasus-inject-prescripts CLI.

Covers DAG parsing, level computation, system-job filtering,
and SCRIPT PRE injection — all without requiring torch or a real
Pegasus submit directory.
"""

import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from Pegasus.cli.pegasus_inject_prescripts import (
    _inject,
    _parse_dag,
    _topo_levels,
    _user_jobs,
    main,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_dag(path: Path, content: str) -> Path:
    path.write_text(textwrap.dedent(content))
    return path


def _write_sub(path: Path, dax_id: str = "job0"):
    path.write_text(f'+pegasus_wf_dax_job_id = "{dax_id}"\nqueue\n')


def _write_system_sub(path: Path):
    """Sub file with null dax_id — simulates a system job."""
    path.write_text('+pegasus_wf_dax_job_id = "null"\nqueue\n')


# ---------------------------------------------------------------------------
# _user_jobs
# ---------------------------------------------------------------------------

class TestUserJobs:
    def test_filters_system_prefix(self):
        jobs = {
            "create_dir_local_0": {"sub_file": "a.sub", "dax_id": None},
            "stage_in_local_0":   {"sub_file": "b.sub", "dax_id": None},
            "preprocess_0":       {"sub_file": "c.sub", "dax_id": "preprocess_0"},
        }
        result = _user_jobs(jobs)
        assert "preprocess_0" in result
        assert "create_dir_local_0" not in result
        assert "stage_in_local_0" not in result

    def test_filters_null_dax_id(self):
        jobs = {"myjob": {"sub_file": "m.sub", "dax_id": None}}
        assert _user_jobs(jobs) == {}

    def test_keeps_valid_user_job(self):
        jobs = {"analyze_0": {"sub_file": "a.sub", "dax_id": "analyze_0"}}
        assert "analyze_0" in _user_jobs(jobs)


# ---------------------------------------------------------------------------
# _topo_levels
# ---------------------------------------------------------------------------

class TestTopoLevels:
    def test_single_job(self):
        jobs    = {"A": {"dax_id": "A"}}
        parents = {}
        levels  = _topo_levels(jobs, parents)
        assert levels["A"] == 0

    def test_linear_chain(self):
        jobs = {
            "A": {"dax_id": "A"},
            "B": {"dax_id": "B"},
            "C": {"dax_id": "C"},
        }
        parents = {"B": {"A"}, "C": {"B"}}
        levels  = _topo_levels(jobs, parents)
        assert levels["A"] == 0
        assert levels["B"] == 1
        assert levels["C"] == 2

    def test_parallel_jobs_same_level(self):
        jobs    = {"A": {"dax_id": "A"}, "B": {"dax_id": "B"}, "C": {"dax_id": "C"}}
        parents = {"B": {"A"}, "C": {"A"}}
        levels  = _topo_levels(jobs, parents)
        assert levels["A"] == 0
        assert levels["B"] == levels["C"] == 1

    def test_diamond(self):
        jobs = {k: {"dax_id": k} for k in ["A", "B", "C", "D"]}
        parents = {"B": {"A"}, "C": {"A"}, "D": {"B", "C"}}
        levels  = _topo_levels(jobs, parents)
        assert levels["A"] == 0
        assert levels["B"] == levels["C"] == 1
        assert levels["D"] == 2

    def test_empty(self):
        assert _topo_levels({}, {}) == {}


# ---------------------------------------------------------------------------
# _parse_dag
# ---------------------------------------------------------------------------

class TestParseDag:
    def test_parses_job_lines(self, tmp_path):
        _write_sub(tmp_path / "preprocess_0.sub", "preprocess_0")
        dag = _write_dag(tmp_path / "wf.dag", """\
            JOB preprocess_0 preprocess_0.sub
        """)
        jobs, parents, lines, has_pre = _parse_dag(dag, tmp_path)
        assert "preprocess_0" in jobs
        assert jobs["preprocess_0"]["dax_id"] == "preprocess_0"

    def test_parses_parent_child(self, tmp_path):
        _write_sub(tmp_path / "A.sub", "A")
        _write_sub(tmp_path / "B.sub", "B")
        dag = _write_dag(tmp_path / "wf.dag", """\
            JOB A A.sub
            JOB B B.sub
            PARENT A CHILD B
        """)
        jobs, parents, lines, has_pre = _parse_dag(dag, tmp_path)
        assert "A" in parents.get("B", set())

    def test_detects_existing_script_pre(self, tmp_path):
        _write_sub(tmp_path / "j.sub", "j")
        dag = _write_dag(tmp_path / "wf.dag", """\
            JOB j j.sub
            SCRIPT PRE j /bin/pred wf.yml /out --level=0
        """)
        jobs, parents, lines, has_pre = _parse_dag(dag, tmp_path)
        assert "j" in has_pre

    def test_system_job_has_null_dax_id(self, tmp_path):
        _write_system_sub(tmp_path / "stage_in_local_0.sub")
        dag = _write_dag(tmp_path / "wf.dag", """\
            JOB stage_in_local_0 stage_in_local_0.sub
        """)
        jobs, parents, lines, has_pre = _parse_dag(dag, tmp_path)
        assert jobs["stage_in_local_0"]["dax_id"] is None


# ---------------------------------------------------------------------------
# _inject
# ---------------------------------------------------------------------------

class TestInject:
    def test_injects_script_pre_for_user_job(self, tmp_path):
        _write_sub(tmp_path / "preprocess_0.sub", "preprocess_0")
        dag = _write_dag(tmp_path / "wf.dag", """\
            JOB preprocess_0 preprocess_0.sub
        """)
        predictor = "/usr/bin/pegasus-runtime-predictor"
        _inject(dag, tmp_path, "workflow.yml", "/tmp/out", predictor)
        content = dag.read_text()
        assert "SCRIPT PRE preprocess_0" in content
        assert predictor in content

    def test_skips_system_jobs(self, tmp_path):
        _write_system_sub(tmp_path / "stage_in_local_0.sub")
        _write_sub(tmp_path / "preprocess_0.sub", "preprocess_0")
        dag = _write_dag(tmp_path / "wf.dag", """\
            JOB stage_in_local_0 stage_in_local_0.sub
            JOB preprocess_0 preprocess_0.sub
        """)
        _inject(dag, tmp_path, "wf.yml", "/tmp", "/bin/pred")
        content = dag.read_text()
        assert "SCRIPT PRE preprocess_0" in content
        assert "SCRIPT PRE stage_in_local_0" not in content

    def test_level_arg_injected(self, tmp_path):
        _write_sub(tmp_path / "jobA.sub", "jobA")
        _write_sub(tmp_path / "jobB.sub", "jobB")
        dag = _write_dag(tmp_path / "wf.dag", """\
            JOB jobA jobA.sub
            JOB jobB jobB.sub
            PARENT jobA CHILD jobB
        """)
        _inject(dag, tmp_path, "wf.yml", "/tmp", "/bin/pred")
        content = dag.read_text()
        assert "--level=0" in content
        assert "--level=1" in content

    def test_no_user_jobs_no_error(self, tmp_path):
        _write_system_sub(tmp_path / "stage_in_local_0.sub")
        dag = _write_dag(tmp_path / "wf.dag", """\
            JOB stage_in_local_0 stage_in_local_0.sub
        """)
        injected, skipped = _inject(dag, tmp_path, "wf.yml", "/tmp", "/bin/pred")
        assert injected == 0

    def test_existing_script_pre_not_duplicated(self, tmp_path):
        _write_sub(tmp_path / "preprocess_0.sub", "preprocess_0")
        dag = _write_dag(tmp_path / "wf.dag", """\
            JOB preprocess_0 preprocess_0.sub
            SCRIPT PRE preprocess_0 /old/pred wf.yml /out --level=0
        """)
        _inject(dag, tmp_path, "wf.yml", "/tmp", "/bin/pred")
        content = dag.read_text()
        assert content.count("SCRIPT PRE preprocess_0") == 1

    def test_returns_counts(self, tmp_path):
        _write_sub(tmp_path / "j0.sub", "j0")
        _write_sub(tmp_path / "j1.sub", "j1")
        _write_system_sub(tmp_path / "stage_in_local_0.sub")
        dag = _write_dag(tmp_path / "wf.dag", """\
            JOB j0 j0.sub
            JOB j1 j1.sub
            JOB stage_in_local_0 stage_in_local_0.sub
        """)
        injected, skipped = _inject(dag, tmp_path, "wf.yml", "/tmp", "/bin/pred")
        assert injected == 2
        assert skipped == 1


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

class TestMain:
    def test_missing_arg_exits(self):
        with patch.object(sys, "argv", ["pegasus-inject-prescripts"]):
            with pytest.raises(SystemExit) as exc:
                main()
            assert exc.value.code != 0

    def test_nonexistent_dir_exits(self, tmp_path):
        with patch.object(sys, "argv", ["pegasus-inject-prescripts", str(tmp_path / "nope")]):
            with pytest.raises(SystemExit) as exc:
                main()
            assert exc.value.code != 0

    def test_missing_workflow_yml_exits(self, tmp_path):
        # braindump exists with dag field but no dax field → workflow.yml unresolvable
        (tmp_path / "workflow.dag").write_text("JOB j j.sub\n")
        (tmp_path / "braindump.yml").write_text("wf_uuid: abc\ndag: workflow.dag\n")
        with patch.object(sys, "argv", ["pegasus-inject-prescripts", str(tmp_path)]):
            with pytest.raises(SystemExit) as exc:
                main()
            assert exc.value.code != 0

    def test_valid_dir_injects(self, tmp_path):
        # Full minimal submit dir
        _write_sub(tmp_path / "preprocess_0.sub", "preprocess_0")
        (tmp_path / "workflow.dag").write_text("JOB preprocess_0 preprocess_0.sub\n")
        wf_yml = tmp_path / "workflow.yml"
        wf_yml.write_text("pegasus: '5.0'\n")
        (tmp_path / "braindump.yml").write_text(
            f"wf_uuid: abc\ndag: workflow.dag\ndax: {wf_yml}\n"
        )

        with patch.object(sys, "argv", ["pegasus-inject-prescripts", str(tmp_path)]):
            with patch(
                "Pegasus.cli.pegasus_inject_prescripts._find_predictor",
                return_value="/bin/pegasus-runtime-predictor",
            ):
                main()

        content = (tmp_path / "workflow.dag").read_text()
        assert "SCRIPT PRE preprocess_0" in content
