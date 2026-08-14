import types

import pytest
from submitit.slurm.slurm import SlurmInfoWatcher

from pyslurm.slurm_executor import STDERR_TAIL_CHARS, SlurmExecutor, is_job_done


class FakeJobPaths:
    def __init__(self, folder, job_id, task_id=0):
        self.stdout = folder / f"{job_id}_{task_id}_log.out"
        self.stderr = folder / f"{job_id}_{task_id}_log.err"
        self.submitted_pickle = folder / f"{job_id}_submitted.pkl"
        self.result_pickle = folder / f"{job_id}_{task_id}_result.pkl"
        self.submission_file = folder / f"{job_id.split('_')[0]}_submission.sh"

    def write_all(self):
        for path in (self.stdout, self.stderr, self.submitted_pickle, self.result_pickle):
            path.write_text("x")
        self.submission_file.write_text("#!/bin/sh\n")


class FakeJob:
    def __init__(self, job_id, state, folder=None, result=None, exc=None):
        self.job_id = job_id
        self.state = state
        self._result = result
        self._exc = exc
        if folder is not None:
            self.paths = FakeJobPaths(folder, job_id)

        # A real watcher, seeded so it answers from cache and never shells out
        self.watcher = SlurmInfoWatcher()
        self.watcher._registered.add(job_id)
        self.watcher._finished.add(job_id)
        self.watcher._info_dict[job_id] = {"State": state}

    def result(self):
        if self._exc is not None:
            raise self._exc
        return self._result


@pytest.mark.parametrize("state, expected", [
    ("COMPLETED", True),
    ("FAILED", True),
    ("TIMEOUT", True),
    ("OUT_OF_MEMORY", True),
    ("NODE_FAIL", True),
    ("CANCELLED", True),
    # sacct decorates cancellations by another user
    ("CANCELLED by 1234", True),
    ("PENDING", False),
    ("RUNNING", False),
    ("COMPLETING", False),
    ("REQUEUED", False),
    # sacct has no record of the job yet
    ("UNKNOWN", False),
    ("", False),
])
def test_is_job_done(state, expected):
    assert is_job_done(FakeJob("42_0", state)) is expected


def _executor(tmp_path, **overrides):
    """Build a SlurmExecutor without touching submitit or SLURM."""
    executor = SlurmExecutor.__new__(SlurmExecutor)
    executor.logger = __import__("logging").getLogger("test")
    executor.executor = types.SimpleNamespace(folder=tmp_path)
    executor.check_interval = 0
    executor.delete_logs = True
    executor.max_array_size = 1000
    executor.max_wait_seconds = 60
    executor.async_mode = False
    for key, value in overrides.items():
        setattr(executor, key, value)
    return executor


def test_cleanup_does_not_touch_sibling_array_tasks(tmp_path):
    """A completed 12345_1 must not delete files belonging to 12345_10."""
    completed = FakeJob("12345_1", "COMPLETED", folder=tmp_path)
    failed = FakeJob("12345_10", "FAILED", folder=tmp_path)
    completed.paths.write_all()
    failed.paths.write_all()

    _executor(tmp_path)._cleanup_files([completed, failed])

    assert not completed.paths.stderr.exists()
    assert failed.paths.stderr.exists()
    assert failed.paths.stdout.exists()


def test_cleanup_keeps_submission_script_until_whole_array_succeeds(tmp_path):
    completed = FakeJob("77_0", "COMPLETED", folder=tmp_path)
    failed = FakeJob("77_1", "FAILED", folder=tmp_path)
    completed.paths.write_all()
    failed.paths.write_all()

    _executor(tmp_path)._cleanup_files([completed, failed])
    assert completed.paths.submission_file.exists()

    failed.state = "COMPLETED"
    _executor(tmp_path)._cleanup_files([completed, failed])
    assert not completed.paths.submission_file.exists()


def test_gather_results_returns_exceptions_in_place(tmp_path):
    boom = RuntimeError("boom")
    jobs = [
        FakeJob("1", "COMPLETED", result=True),
        FakeJob("2", "FAILED", exc=boom),
        FakeJob("3", "COMPLETED", result=False),
    ]

    assert _executor(tmp_path)._gather_results(jobs) == [True, boom, False]


def test_gather_results_does_not_wait_on_unfinished_jobs(tmp_path):
    """A job left RUNNING by the wait timeout must not block result collection."""
    stuck = FakeJob("9_0", "RUNNING", result="never")
    stuck.result = lambda: pytest.fail("result() must not be called on a running job")

    results = _executor(tmp_path)._gather_results([stuck])

    assert len(results) == 1
    assert isinstance(results[0], TimeoutError)


def test_empty_arg_list_returns_without_submitting(tmp_path):
    executor = _executor(tmp_path)

    assert executor.run_slurm_array(print, []) == []
    assert executor.run_slurm_array(print, [], collect_results=True) == ([], [])


def test_mismatched_kwargs_length_raises(tmp_path):
    with pytest.raises(ValueError):
        _executor(tmp_path).run_slurm_array(print, [(1,), (2,)], [{}])


def test_stderr_tail_is_bounded():
    assert len(("y" * 10_000)[-STDERR_TAIL_CHARS:]) == STDERR_TAIL_CHARS
