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
    parser.add_argument("-wg", "--wgroup", help="workergroup number")
    if a_args is not False:
        args = parser.parse_args(a_args)
    else:
        args = parser.parse_args()
    return args

class guts_scheduler:
    """A scheduler class for GUTS

    The GUTS scheduler is the top-level, user-facing end of GUTS compute
    system, which includes a disk-based queue, a resources manager
    and a set of worker groups, each of which containing multiple
    workers with a set of resources.

    The scheduler is designed to not be persistent, but rather
    respawned at will by the user or the individual worker group
    themselves.

    Attributes
    ----------
    _params : dict[Any]
        The configuration parameters dictionary
    _name : str
        The scheduler name
    _queue_file : str
        The name of the guts_queue file associated with the scheduler
    _queue : guts_queue
        A guts_queue object with which the scheduler interacts
    _resource_manager : resource_manager
        A resource manager to dispatch resources to worker groups
    _wgroups : list[guts_workergroup]
        A list of workergroup
    _nwgroups : int
        The number of worker groups the scheduler manage
    """
    def __init__(self,
                 a_args: Optional[list[str]] = None) -> None:
        """Initialize scheduler internals.
        
        Args:
            a_args: optional list of options
        """
        # Read input parameters
        input_file = vars(parse_cl_args(a_args=a_args))["input"]
        if (not os.path.exists(input_file)):
            raise ValueError(
                "Could not find the {} GUTS input file !".format(input_file)
            )
        with open(input_file, 'r') as f:
            self._params = toml.load(f)

        # Add the input file name to the dict for future reference
        self._params["input_toml"] = input_file
            
        # Scheduler metadata
        self._name = self._params.get("case",{}).get("name","guts_run")
        self._queue_file = self._params.get("case",{}).get("queue_file",f"{self._name}_queue.db")
        self._queue = guts_queue(self._queue_file)

        # Worker groups & compute resources
        self._resource_manager = resource_manager(self._params)
        self._wgroups : list[guts_workergroup] = []

        # Minimalist internal initiation
        self._nwgroups = self._resource_manager.get_nwgroups()
    
        # TODO: enable map wgroups to different resource configs
        res_config = self._params.get("resource",{}).get("config",{})
        for i in range(self._nwgroups):
            self._wgroups.append(guts_workergroup(i,
                                                  self._params,
                                                  res_config,
                                                  queue=self._queue))

        # Parse workergroup number if provided
        wgroup_id_str = vars(parse_cl_args(a_args=a_args))["wgroup"]
        self._wgroup_id_spawn = None
        if wgroup_id_str:
            self._wgroup_id_spawn = int(wgroup_id_str)
        
    
    def start(self) -> None:
        """Start the scheduler.
        
        This a non-blocking call, where each workergroup is initiated
        and launched seperately depending on the resource backend type.
        """
        # Initiate the worker groups by requesting resource from the manager
        for wgroup in self._wgroups:
            wgroup.request_resources(self._resource_manager)

    def run_wgroup(self) -> None:
        """Run a given workergroup.

        Arguments
        ---------
        wgroup_id : int
            A workergroup index
        """
        assert self._wgroup_id_spawn < self._nwgroups
        target_wgroup = self._wgroups[self._wgroup_id_spawn]
        target_wgroup.launch(self._resource_manager)

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
