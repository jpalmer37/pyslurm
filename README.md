# pyslurm

A Python wrapper for submitting and managing SLURM job arrays using Meta's [submitit](https://github.com/facebookincubator/submitit) library.

## Features

- Simple, Pythonic interface for SLURM job submission
- Automatic job monitoring and status tracking
- Comprehensive logging with customizable log paths
- Support for job arrays with configurable parallelism
- Automatic cleanup of completed job logs (optional)
- Type hints for better IDE support

## Installation

### Using pip

```bash
pip install .
```

Or install in development mode:

```bash
pip install -e .
```

### Using conda

Create the conda environment from the provided `environment.yaml`:

```bash
conda env create -f environment.yaml
conda activate pyslurm
pip install .
```

## Dependencies

- Python >= 3.8
- setuptools = 80 (pinned for compatibility)
- submitit = 1.5.2 (pinned for compatibility)

## Usage

### Basic Example

```python
from pyslurm import SlurmExecutor

# Define a function to run on SLURM
def my_task(x, y):
    return x + y

# Configure the executor
config = {
    "log_dir": "slurm_logs",
    "job_name": "my-job",
    "partition": "prod",
    "time": "01:00:00",
    "mem": "4G",
    "cpus_per_task": 1,
    "array_parallelism": 4,
    "check_interval": 30,
    "delete_logs": True
}

# Create executor
executor = SlurmExecutor(config=config)

# Prepare job arguments
function_arg_list = [
    (1, 2),
    (3, 4),
    (5, 6)
]

# Submit and run jobs (waits for all to complete)
job_list = executor.run_slurm_array(
    function=my_task,
    function_arg_list=function_arg_list
)

# Check job status and get results
# Note: All jobs will be in a terminal state (COMPLETED or FAILED)
completed_jobs = [job for job in job_list if job.state == "COMPLETED"]
failed_jobs = [job for job in job_list if job.state == "FAILED"]

if len(completed_jobs) == len(job_list):
    print("All jobs completed successfully!")
    # Get results
    results = [job.result() for job in job_list]
    print(f"Results: {results}")
else:
    print(f"{len(completed_jobs)}/{len(job_list)} jobs completed, {len(failed_jobs)} failed")
```

### Custom Logging

```python
import logging
from pyslurm import SlurmExecutor

# Option 1: Use custom log path in config
config = {
    "custom_log_path": "/path/to/my/logfile.log",
    "log_dir": "slurm_logs"
}
executor = SlurmExecutor(config=config)

# Option 2: Provide your own logger
logger = logging.getLogger("my_app")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("/path/to/my/logfile.log")
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

executor = SlurmExecutor(config=config, logger=logger)
```

### Configuration Options

The `config` dictionary supports the following options:

- `log_dir` (str): Directory for SLURM job logs (default: "slurm_logs")
- `job_name` (str): Name for SLURM jobs (default: "auto-batch-runner")
- `partition` (str): SLURM partition to use (default: "prod")
- `time` (str): Time limit for jobs (default: "01:00:00")
- `mem` (str): Memory per job (default: "4G")
- `cpus_per_task` (int): CPUs per task (default: 1)
- `array_parallelism` (int): Number of parallel array jobs (default: 4)
- `check_interval` (int): Interval in seconds to check job status (default: 30)
- `delete_logs` (bool): Whether to delete logs for completed jobs (default: True)
- `custom_log_path` (str): Path for custom application logs (optional)

### Advanced Example with Keyword Arguments

```python
from pyslurm import SlurmExecutor

def process_data(input_file, output_file, mode="default", verbose=False):
    # Your processing logic here
    pass

executor = SlurmExecutor()

# Prepare arguments
function_arg_list = [
    ("input1.txt", "output1.txt"),
    ("input2.txt", "output2.txt"),
    ("input3.txt", "output3.txt")
]

# Prepare keyword arguments
function_kwargs_list = [
    {"mode": "fast", "verbose": True},
    {"mode": "accurate", "verbose": False},
    {"mode": "default", "verbose": True}
]

# Submit jobs
job_list = executor.run_slurm_array(
    function=process_data,
    function_arg_list=function_arg_list,
    function_kwargs_list=function_kwargs_list
)
```

## Logging

The package includes comprehensive logging with structured JSON log messages. Log events include:

- `slurm_executor_init`: Executor initialization
- `slurm_executor_configured`: Configuration parameters
- `slurm_array_start`: Job array submission started
- `job_submitted`: Individual job submitted
- `jobs_submitted`: All jobs submitted with IDs
- `job_status_check`: Periodic status checks
- `all_jobs_completed`: All jobs finished
- `failed_jobs_detected`: Failed jobs detected
- `slurm_job_failed`: Individual job failure with details
- `logs_deleted`: Log cleanup completed
- `slurm_array_complete`: Final summary

## Development

### Setting up development environment

```bash
# Clone the repository
git clone https://github.com/jpalmer37/pyslurm.git
cd pyslurm

# Create conda environment
conda env create -f environment.yaml
conda activate pyslurm

# Install in development mode
pip install -e ".[dev]"
```

### Running tests

```bash
pytest
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

This package is built on top of Meta's excellent [submitit](https://github.com/facebookincubator/submitit) library.

