"""A front end for GUTS utils."""
import argparse
import os
import time
import toml
from typing import Any, Optional
from guts_utils.guts_workergroup import guts_workergroup
from guts_utils.guts_queue import guts_queue
from guts_utils.guts_task import guts_task
from guts_utils.guts_resource_manager import resource_manager

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
        self._resource_manager = resource_manager(self._params)
        self._wgroups : list[guts_workergroup] = []
    
    def start(self) -> None:
        """Start the scheduler."""
        # Initialize the worker groups
        self._nwgroups = self._resource_manager.get_nwgroups()
        res_config = self._params.get("resource",{}).get("config",{})
        for i in range(self._nwgroups):
            self._wgroups.append(guts_workergroup(i,
                                                  self._params,
                                                  res_config,
                                                  queue=self._queue))

        # Start the worker groups
        for wgroup in self._wgroups:
            wgroup.acquire_resources(self._resource_manager)

    def check(self) -> None:
        """Check the scheduler queue and workergroups status."""
        pass

    def restore(self) -> None:
        """Restore the workergroups if needed."""
        pass

    def get_queue(self) -> guts_queue:
        """Return the scheduler queue."""
        return self._queue

    def add_task(self, task : guts_task) -> None:
        """Add a new task to the queue."""
        self._queue.add_task(task)

    def name(self) -> str:
        """Return the case name."""
        return self._name

    def cleanup(self) -> None:
        """Clean scheduler internals."""
        self._queue.delete()
