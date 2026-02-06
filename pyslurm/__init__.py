"""
pyslurm - A Python wrapper for submitting SLURM job arrays using submitit

This package provides a simple interface for submitting and managing SLURM jobs
from Python using Meta's submitit library.
"""

__version__ = "0.1.0"
__author__ = "jpalmer37"
__description__ = "Module leveraging Meta's submitit library for submitting Python functions to SLURM"

from .slurm_executor import SlurmExecutor

__all__ = ["SlurmExecutor", "__version__"]
