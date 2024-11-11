"""A set of SLURM utilities for GUTS."""
import os
import shutil
import subprocess

def is_slurm_avail() -> bool:
    """Assess if slurm is available on system."""
    slurm_avail = False

    # Test environment variable
    if "SLURM_VERSION" in os.environ:
        slurm_avail = True

    # Test for a slurm command
    try:
        result = subprocess.run(["sinfo", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            slurm_avail = True
        else:
            if slurm_avail:
                print(f"SLURM sinfo is not available even though SLURM_VERSION is defined")
            slurm_avail = False
    except FileNotFoundError:
        if slurm_avail:
            print(f"SLURM sinfo is not available even though SLURM_VERSION is defined")
        slurm_avail = False

    return slurm_avail
    
def make_job_script_wgroup(res_config) -> dict[str : str]:
    """Assemble a workergroup job script from config."""
    
