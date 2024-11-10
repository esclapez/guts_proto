"""A set of utilities to get platform information."""
import psutil
import os
import sys

# Get the SLURM job ID
SLURM_JOB_ID = os.environ.get(
    "SLURM_JOB_ID"
)

def is_mac_os() -> str:
    system = sys.platform.lower()
    return system.startswith("dar")

def is_windows_os() -> str:
    system = sys.platform.lower()
    return system.startswith("win")

def is_linux_os() -> str:
    system = sys.platform.lower()
    return system.startswith("lin")

def get_cpu_count():
    return psutil.cpu_count(logical=False)
