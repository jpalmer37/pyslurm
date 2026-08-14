import os
import json
import logging
import submitit
import time
from typing import Optional, Callable, List, Dict, Any


# Characters of a failed job's stderr to include in the log record.
STDERR_TAIL_CHARS = 2000


def is_job_done(job: submitit.Job) -> bool:
    """Return True if the job has left every state in which it could still run."""
    # Anything submitit does not recognise as pending counts as done, so a state
    # sacct decorates ("CANCELLED by 1234") ends the wait instead of extending it.
    return job.watcher.is_done(job.job_id)


class SlurmExecutor():
    """
    A wrapper class for submitting and managing SLURM job arrays using submitit.
    
    This class provides an interface to submit Python functions as SLURM jobs,
    monitor their execution, and manage job logs.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None, logger: Optional[logging.Logger] = None):
        """
        Initialize the SlurmExecutor with configuration options.
        
        Args:
            config: Configuration dictionary with the following optional keys:
                - log_dir: Directory for SLURM job logs (default: "slurm_logs")
                - job_name: Name for SLURM jobs (default: "auto-batch-runner")
                - partition: SLURM partition to use (default: "prod")
                - time: Time limit for jobs (default: "01:00:00")
                - mem: Memory per job (default: "4G")
                - cpus_per_task: CPUs per task (default: 1)
                - array_parallelism: Number of parallel array jobs (default: 8)
                - check_interval: Interval in seconds to check job status (default: 30).
                  Values below 60 have little effect: submitit's own state watcher
                  refreshes at most once a minute.
                - max_array_size: Maximum jobs per submitted array; longer job lists
                  are split into consecutive chunks (default: 1000)
                - max_wait_seconds: Cap on how long to wait for one chunk before
                  giving up and returning (default: 86400)
                - delete_logs: Whether to delete logs for completed jobs (default: True)
                - async_mode: When True, return immediately after job submission without
                  waiting for jobs to complete (default: False)
                - custom_log_path: Path for custom application logs (optional)
            logger: Optional custom logger instance. If not provided, creates a default logger.
        """
        if config is None:
            config = {}
        
        # Setup logging
        self.logger = logger if logger is not None else self._setup_logger(config)
        
        # Get configuration values
        log_dir = config.get("log_dir", "slurm_logs")
        self.check_interval = config.get("check_interval", 30)
        self.delete_logs = config.get("delete_logs", True)
        self.max_array_size = config.get("max_array_size", 1000)
        self.max_wait_seconds = config.get("max_wait_seconds", 86400)
        self.async_mode = config.get("async_mode", False)

        # Log initialization
        self.logger.info(json.dumps({
            "event_type": "slurm_executor_init",
            "log_dir": log_dir,
            "check_interval": self.check_interval,
            "max_wait_seconds": self.max_wait_seconds,
            "delete_logs": self.delete_logs,
            "async_mode": self.async_mode,
        }))
        
        # Initialize submitit executor
        self.executor = submitit.AutoExecutor(folder=log_dir)
        self.executor.update_parameters(
            slurm_job_name=config.get("job_name", "auto-batch-runner"),
            slurm_partition=config.get("partition", "prod"),
            slurm_time=config.get("time", "01:00:00"),
            slurm_mem=config.get("mem", "4G"),
            slurm_cpus_per_task=config.get("cpus_per_task", 1),
            slurm_array_parallelism=config.get("array_parallelism", 8),
        )
        
        self.logger.info(json.dumps({
            "event_type": "slurm_executor_configured",
            "parameters": self.get_executor_parameters()
        }))
    
    def _setup_logger(self, config: Dict[str, Any]) -> logging.Logger:
        """
        Setup a logger with optional custom log path.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger("pyslurm.SlurmExecutor")

        # Only configure if not already configured
        if not logger.handlers:
            logger.setLevel(logging.INFO)

            # Handlers here are a fallback for callers with no logging setup of
            # their own; propagating as well would duplicate every record.
            logger.propagate = False

            # Create formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            
            # Add console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            
            # Add file handler if custom log path is specified
            custom_log_path = config.get("custom_log_path")
            if custom_log_path:
                # Create directory if it doesn't exist
                log_dir = os.path.dirname(custom_log_path)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
                
                file_handler = logging.FileHandler(custom_log_path)
                file_handler.setLevel(logging.INFO)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
        
        return logger
    
    def get_executor_parameters(self) -> Dict[str, Any]:
        """
        Get the current SLURM executor parameters.
        
        Returns:
            Dictionary of executor parameters
        """
        return self.executor.parameters

    def _cleanup_files(self, job_list: List[submitit.Job]) -> None:
        """
        Delete submitit's per-job files for every job that completed successfully.

        Args:
            job_list: List of submitit Job objects
        """
        completed_jobs = [job for job in job_list if job.state == "COMPLETED"]
        deleted_files = 0

        for job in completed_jobs:
            # Paths are built explicitly rather than globbed on the job id, which
            # would match sibling array tasks: "12345_1*" also matches 12345_10.
            for attr in ("stdout", "stderr", "submitted_pickle", "result_pickle"):
                path = getattr(job.paths, attr)
                if path.exists():
                    path.unlink()
                    deleted_files += 1

        # A whole array shares one submission script, so it can only go once
        # every job in that array has succeeded.
        if len(completed_jobs) == len(job_list):
            for path in {job.paths.submission_file for job in completed_jobs}:
                if path.exists():
                    path.unlink()
                    deleted_files += 1

        if deleted_files > 0:
            self.logger.info(json.dumps({
                "event_type": "logs_deleted",
                "num_files": deleted_files,
                "num_completed_jobs": len(completed_jobs)
            }))

    def _gather_results(self, job_list: List[submitit.Job]) -> List[Any]:
        """
        Return each job's return value, or the exception raised in its place.

        Must run before _cleanup_files, which removes the result pickles.

        Args:
            job_list: List of submitit Job objects

        Returns:
            List of results, one per job, in submission order
        """
        results: List[Any] = []
        for job in job_list:
            # job.result() waits indefinitely on a job that never finished
            if not is_job_done(job):
                results.append(TimeoutError(f"job {job.job_id} is still {job.state}"))
                continue
            try:
                results.append(job.result())
            except Exception as e:
                results.append(e)
        return results

    def run_slurm_array(self,
                        function: Callable,
                        function_arg_list: List[tuple],
                        function_kwargs_list: Optional[List[Dict[str, Any]]] = None,
                        collect_results: bool = False) -> Any:
        """
        Submit and execute a SLURM job array.

        Args:
            function: The callable function to execute on each job
            function_arg_list: List of argument tuples, one per job
            function_kwargs_list: Optional list of keyword argument dicts, one per job
            collect_results: When True, also return what each job returned

        Returns:
            List of submitted job objects, or a (jobs, results) tuple when
            collect_results is True. `results` holds each job's return value in
            submission order, or the exception raised in its place, and is empty
            in async_mode. When async_mode is False (default), all jobs will have
            reached a terminal SLURM state before this method returns. When
            async_mode is True, the method returns immediately after submission
            and callers are responsible for monitoring job states.
        """
        self.logger.info(json.dumps({
            "event_type": "slurm_array_start",
            "num_jobs": len(function_arg_list)
        }))

        # Prepare kwargs list if not provided
        if function_kwargs_list is None:
            function_kwargs_list = [{}] * len(function_arg_list)

        # Validate that we have a one-to-one mapping between args and kwargs
        if len(function_kwargs_list) != len(function_arg_list):
            self.logger.error(json.dumps({
                "event_type": "slurm_array_invalid_input",
                "message": "function_kwargs_list length must match function_arg_list length",
                "num_args": len(function_arg_list),
                "num_kwargs": len(function_kwargs_list),
            }))
            raise ValueError(
                f"function_kwargs_list length ({len(function_kwargs_list)}) must match "
                f"function_arg_list length ({len(function_arg_list)})"
            )
        if not function_arg_list:
            self.logger.info(json.dumps({
                "event_type": "slurm_array_empty",
                "message": "No arguments supplied, nothing to submit"
            }))
            return ([], []) if collect_results else []

        arg_list_chunks = [ function_arg_list[i: i+ self.max_array_size] for i in range(0, len(function_arg_list), self.max_array_size)]
        kwarg_list_chunks = [ function_kwargs_list[i: i+ self.max_array_size] for i in range(0, len(function_kwargs_list), self.max_array_size)]

        # Submit jobs
        master_job_list = []
        master_result_list: List[Any] = []

        for n, (arg_list_chunk, kwarg_list_chunk) in enumerate(zip(arg_list_chunks, kwarg_list_chunks), start=1):

            self.logger.info(json.dumps({
                "event_type": "submitting_job_chunk",
                "chunk_size": len(arg_list_chunk),
                "chunk_index": n,
            }))

            job_list = []

            with self.executor.batch():
                for idx, (args, kw_args) in enumerate(zip(arg_list_chunk, kwarg_list_chunk)):
                    job = self.executor.submit(function, *args, **kw_args)
                    job_list.append(job)

            if not self.async_mode:
                # Poll until all jobs have reached a terminal SLURM state
                job_complete = [is_job_done(job) for job in job_list]
                check_count = 0
                wait_deadline = time.monotonic() + self.max_wait_seconds

                while not all(job_complete) and time.monotonic() < wait_deadline:
                    time.sleep(self.check_interval)
                    job_complete = [is_job_done(job) for job in job_list]
                    check_count += 1

                    completed_count = sum(job_complete)
                    self.logger.info(json.dumps({
                        "event_type": "job_status_check",
                        "check_count": check_count,
                        "completed": completed_count,
                        "total": len(job_list),
                        "pending": len(job_list) - completed_count
                    }))

                if not all(job_complete):
                    self.logger.error(json.dumps({
                        "event_type": "slurm_array_wait_timeout",
                        "max_wait_seconds": self.max_wait_seconds,
                        "unfinished": len(job_list) - sum(job_complete),
                        "total": len(job_list)
                    }))

                # Log every job that did not succeed, not just state FAILED:
                # TIMEOUT, OUT_OF_MEMORY, NODE_FAIL and CANCELLED matter too.
                failed_jobs = [job for job in job_list if job.state != "COMPLETED"]
                if failed_jobs:
                    self.logger.warning(json.dumps({
                        "event_type": "failed_jobs_detected",
                        "num_failed": len(failed_jobs)
                    }))

                    for job in failed_jobs:
                        stderr = job.stderr() or ""
                        self.logger.error(json.dumps({
                            "event_type": "slurm_job_failed",
                            "job_id": job.job_id,
                            "state": job.state,
                            "stderr_path": str(job.paths.stderr),
                            "stderr_tail": stderr[-STDERR_TAIL_CHARS:]
                        }))

                # Results live in pickles that _cleanup_files removes
                if collect_results:
                    master_result_list.extend(self._gather_results(job_list))

                if self.delete_logs:
                    self._cleanup_files(job_list)

                self.logger.info(json.dumps({
                    "event_type": "job_chunk_completed",
                    "completed": len([j for j in job_list if j.state == "COMPLETED"]),
                    "failed": len(failed_jobs),
                    "total": len(job_list)
                }))

            master_job_list.extend(job_list)

        self.logger.info(json.dumps({
                "event_type": "slurm_array_completed",
                "total_jobs": len(master_job_list),
                "total_chunks": n
            }))

        if collect_results:
            return master_job_list, master_result_list

        return master_job_list
