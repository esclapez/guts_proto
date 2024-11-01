"""A front end for GUTS utils."""
import argparse
import os
import time
import toml
from typing import Any, Optional
from guts_utils.guts_worker import guts_workergroup
from guts_utils.guts_queue import guts_queue

def parse_cl_args(a_args: Optional[list[str]] = None) -> argparse.Namespace :
    """Parse provided list or default CL argv.

    Args:
        a_args: optional list of options
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="GUTS input .toml file", default="input.toml")
    if a_args is not False:
        args = parser.parse_args(a_args)
    else:
        args = parser.parse_args()
    return args

class guts_scheduler:
    def __init__(self,
                 a_args: Optional[list[str]] = None) -> None:
        """Initialize scheduler internals.
        
        Args:
            a_args: optional list of options
        """
        # Read input parameters
        input_file = vars(parse_cl_args(a_args=a_args))["input"]
        if (not os.path.exists(input_file)):
            raise GUTSError(
                "Could not find the {} GUST input file !".format(input_file)
            )
        with open(input_file, 'r') as f:
            self._params = toml.load(f)
            
        # Scheduler metadata
        self._name = self._params.get("case",{}).get("name","guts_run")
        self._queue_file = self._params.get("case",{}).get("queue_file",f"{self._name}_queue.db")
        self._queue = guts_queue(self._queue_file)

        # Worker groups & compute resources
        self._backend : str = self._params.get("resource",{}).get("backend", "local")
        self._wgroups : list[guts_workergroup] = []
    
    def run(self) -> None:
        """Run the scheduler."""
        # Initialize the worker groups
        self._nwgroups = self._params.get("resource",{}).get("ngroup",1)
        resource_config = {}

    def name(self) -> str:
        """Return the case name."""
        return self._name

    def cleanup(self) -> None:
        """Clean scheduler internals."""
        self._queue.delete()
