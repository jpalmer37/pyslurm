import os
import json 
import logging
import submitit
import time
from glob import glob 
from typing import Optional, Callable, List, Dict, Tuple, Any


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
                - array_parallelism: Number of parallel array jobs (default: 4)
                - check_interval: Interval in seconds to check job status (default: 30)
                - delete_logs: Whether to delete logs for completed jobs (default: True)
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

        # Validate max_array_size to ensure it is a positive integer
        raw_max_array_size = config.get("max_array_size", 1000)
        try:
            max_array_size = int(raw_max_array_size)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid max_array_size={raw_max_array_size!r}: must be a positive integer."
            ) from exc
        if max_array_size <= 0:
            raise ValueError(
                f"Invalid max_array_size={max_array_size!r}: must be a positive integer greater than 0."
            )
        self.max_array_size = max_array_size
        
        # Log initialization
        self.logger.info(json.dumps({
            "event_type": "slurm_executor_init",
            "log_dir": log_dir,
            "check_interval": self.check_interval,
            "delete_logs": self.delete_logs
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

    def run_slurm_array(self, 
                        function: Callable, 
                        function_arg_list: List[tuple], 
                        function_kwargs_list: Optional[List[Dict[str, Any]]] = None) -> List[Any]:
        """
        Submit and execute a SLURM job array.
        
        Args:
            function: The callable function to execute on each job
            function_arg_list: List of argument tuples, one per job
            function_kwargs_list: Optional list of keyword argument dicts, one per job
            
        Returns:
            List of job objects. Users can inspect job.state to determine success/failure.
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
        arg_list_chunks = [ function_arg_list[i: i+ self.max_array_size] for i in range(0, len(function_arg_list), self.max_array_size)]
        kwarg_list_chunks = [ function_kwargs_list[i: i+ self.max_array_size] for i in range(0, len(function_kwargs_list), self.max_array_size)]

        # Submit jobs
        master_job_list = []

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
        
            # Continue checking for all jobs to complete
            job_complete = [job.done() for job in job_list]
            check_count = 0

            while not all(job_complete):
                time.sleep(self.check_interval)
                job_complete = [job.done() for job in job_list]
                check_count += 1
                
                completed_count = sum(job_complete)
                self.logger.info(json.dumps({
                    "event_type": "job_status_check",
                    "check_count": check_count,
                    "completed": completed_count,
                    "total": len(job_list),
                    "pending": len(job_list) - completed_count
                }))
            
            self.logger.info(json.dumps({
                "event_type": "all_jobs_completed",
                "num_jobs": len(job_list),
                "total_checks": check_count
            }))
            
            # Check for any jobs that did not complete in time
            if not all(job_complete):
                self.logger.error(json.dumps({"event_type": "slurm_jobs_timeout"}))
            
            # Log any failed jobs
            failed_jobs = [job for job in job_list if job.state == "FAILED"]
            if failed_jobs:
                self.logger.warning(json.dumps({
                    "event_type": "failed_jobs_detected",
                    "num_failed": len(failed_jobs)
                }))
                
                for job in failed_jobs:
                    self.logger.error(json.dumps({
                        "event_type": "slurm_job_failed", 
                        "job_id": job.job_id, 
                        "exception": str(job.exception()), 
                        "stderr": str(job.stderr())
                    }))
            
            # Optionally delete logs for completed jobs
            if self.delete_logs:
                completed_jobs = [job for job in job_list if job.state == "COMPLETED"]
                deleted_files = 0
                for job in completed_jobs:
                    for file_path in glob(os.path.join(self.executor.folder, f"{job.job_id}*")):
                        os.remove(file_path)
                        deleted_files += 1
                
                if deleted_files > 0:
                    self.logger.info(json.dumps({
                        "event_type": "logs_deleted",
                        "num_files": deleted_files,
                        "num_completed_jobs": len(completed_jobs)
                    }))

            self.logger.info(json.dumps({
                "event_type": "job_chunk_completed",
                "completed": len([j for j in job_list if j.state == "COMPLETED"]),
                "failed": len(failed_jobs),
                "total": len(job_list)
            }))

            master_job_list.extend(job_list)
        
        return master_job_list
