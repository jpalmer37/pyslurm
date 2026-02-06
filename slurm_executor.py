import os
import json 
import logging
import submitit
import time
from glob import glob 

class SlurmExecutor():
    def __init__(self, config: dict = None):
        if config is None:
            config = {}
        self.executor = submitit.AutoExecutor(folder=config.get("log_dir", "slurm_logs"))
        self.executor.update_parameters(
            slurm_job_name=config.get("job_name", "auto-batch-runner"),
            slurm_partition=config.get("partition", "prod"),
            slurm_time=config.get("time", "01:00:00"),
            slurm_mem=config.get("mem", "4G"),
            slurm_cpus_per_task=config.get("cpus_per_task", 1),
            slurm_array_parallelism=config.get("array_parallelism", 4)
        )

        self.check_interval = config.get("check_interval", 30)
        self.delete_logs = config.get("delete_logs", True)
    
    def get_executor_parameters(self) -> dict:
        return self.executor.parameters

    def run_slurm_array(self, 
                        function: callable, 
                        function_arg_list: list, 
                        function_kwargs_list: list = None) -> None:

        # Prepare kwargs list if not provided
        if function_kwargs_list is None:
            function_kwargs_list = [{}] * len(function_arg_list)

        # Submit jobs
        job_list = []
        with self.executor.batch():
            for args, kw_args in zip(function_arg_list, function_kwargs_list):
                job = self.executor.submit(function, *args, **kw_args)
                job_list.append(job)
        
        # continue checking for all jobs to complete until timeout
        job_complete = [job.done() for job in job_list]
        while not all(job_complete):
            time.sleep(self.check_interval)
            job_complete = [job.done() for job in job_list]
        
        # Check for any jobs that did not complete in time
        if not all(job_complete):
            logging.error(json.dumps({"event_type": "slurm_jobs_timeout"}))
            return job_list, False
        
        # Log any failed jobs
        failed_jobs = [job for job in job_list if job.state == "FAILED"]
        for job in failed_jobs:
            logging.error(json.dumps({"event_type": "slurm_job_failed", "job_id": job.job_id, "exception": str(job.exception()), "stderr": str(job.stderr())}))
        
        # Optionally delete logs for completed jobs
        if self.delete_logs:
            completed_jobs = [job for job in job_list if job.state == "COMPLETED"]
            for job in completed_jobs:
                for file_path in glob(os.path.join(self.executor.folder, f"{job.job_id}*")):
                    os.remove(file_path)

        return job_list, True
