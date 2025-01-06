"""A resource set/manager classes for GUTS."""
from __future__ import annotations
from abc import ABCMeta, abstractmethod
from multiprocessing import Process
from guts_utils.guts_error import GUTSError
from guts_utils.guts_sys_utils import get_cpu_count
from guts_utils.guts_slurm_utils import SlurmCluster, make_job_script_wgroup, time_to_s, submit_slurm_job, cancel_slurm_job
from typing import Any, Optional
import json
import toml
import os
import sys
import subprocess
import time

class resource_set_baseclass(metaclass=ABCMeta):
    """A resource set base class for GUTS.

    A resource set base class, defining the API
    expected by the resource_manager class.

    The base class is responsible for the public initialization,
    while concrete implementation relies on `_init_rs` to set
    specific attributes.

    Attributes
    ----------
    _wgroup_id : int
        The id of the workergroup this resource set is attached to.
    _nworkers : int
        The number of workers in the resource set
    __runtime : float
        The lifespan of the resource set
    _workers : list[Process]
        The actual worker's Processes
    _worker_pids : list[int]
        Convenience list of worker's PID
    """
    def __init__(self,
                 wgroup_id : int,
                 res_config : Optional[dict[Any]] = None,
                 json_str : Optional[str] = None) -> None:
        """Initialize the resource set.

        It can be initialized from a dictionary or from a 
        previously serialized version of the set.
        
        Arguments
        ---------
        int
            The id of the group with which this set is associated
        Optional[dict[Any]]
            A dictionary describing the resource set
        Optional[str]
            A json string describing the resource set
        """
        assert(res_config or json_str)
        self._wgroup_id = wgroup_id
        self._nworkers : int = 0

        # Concrete class need to override self._runtime
        self._runtime : int | None = None

        if res_config:
            self._nworkers = res_config.get("nworkers", 1)
        if json_str:
            json_dict = json.loads(json_str) 
            self._nworkers = json_dict.get("nworkers", 1)
        self._workers = []
        self._worker_pids : list[int] = [] 
        self._init_rs(res_config = res_config, json_str = json_str)

    @abstractmethod
    def _init_rs(self,
                 res_config : Optional[dict[Any]] = None,
                 json_str : Optional[str] = None) -> None:
        """Asbtract method initializing the resource set.
        
        Arguments
        ---------
        Optional[dict[Any]]
            A dictionary describing the resource set, passed from `__init__`
        Optional[str]
            A json string describing the resource set, passed from `__init__`
        """
        pass

    @abstractmethod
    def request(self, config : dict[Any]) -> None:
        """Request the resources of the set.
        
        A non-blocking call responsible of using subprocess to request
        the resources of the set from the system.

        Arguments
        ---------
        config : dict[Any]
            The GUTS top-level configuration parameters dictionary.
        """
        pass

    @abstractmethod
    def serialize(self) -> str:
        """Serialize the resource set.
        
        Returns
        -------
        str
            A json_string describing the acquire resources, to be stored in the queue
        """
        pass

    @abstractmethod
    def from_json(self):
        """Deserialize the resource set.
        
        Returns
        -------
        class
            An instance of the particular derived class object
        """
        pass

    @abstractmethod
    def release(self) -> None:
        """Release the resources of the set.
        
        Raises
        ------
        RuntimeError
            If something went wrong while releasing the resource
        """
        pass

    def get_nworkers(self) -> int:
        """Get the (theoretical) number of workers."""
        return self._nworkers

    def get_nworkers_active(self) -> int:
        """Get the active number of workers."""
        return len(self._workers)

    def workers_initiated(self) -> bool:
        """Test if all workers are initiated."""
        return len(self._workers) >= self._nworkers

    def append_worker_process(self, process : Process) -> None:
        """Add a worker process to the list."""
        self._workers.append(process)
        self._workers[-1].start()
        self._worker_pids.append(self._workers[-1].pid)

    def worker_runtime(self) -> int:
        """Return the worker runtime."""
        assert self._runtime
        return self._runtime

class resource_manager:
    """A resource manager class for GUTS.

    The resource manager interface between the workergroups
    and the compute resources of the system.

    Attributes
    ----------
    _nwgroups : int
        The number of worker groups the manager has to handle
    _backend : int
        The type of compute resource available in the backend (local, slurm, ...)
    _configs : dict[Any]
        The GUTS full parmeters dictionary, augmented if necessary
    """
    def __init__(self,
                 configs : dict[Any]) -> None:
        """Initialize the resource manager.

        Arguments
        ---------
        configs : dict[Any]
            The GUTS full parmeters dictionary

        Raises
        ------
        ValueError
            If a configuration parameters has a wrong value
        RuntimeError
            If if was not possible to access the resource backend
        """
        self._nwgroups : int = configs.get("resource",{}).get("nwgroups", 1)
        self._backend : str = configs.get("resource",{}).get("backend", "local")
        self._configs : dict[Any] = configs

        if self._backend == "local":
            # Keep track of local cpu resources available
            self._max_cpus : int = get_cpu_count()
            self._used_cpus : int = 0

        elif self._backend == "slurm":
            # Gather information on the system using SlurmCluster
            try:
                self._cluster = SlurmCluster()
            except Exception as e:
                print("Error while instanciating a SlurmCluster")
                raise
            

    def get_resources(self,
                      res_configs : dict[Any],
                      wgroup_id : int) -> resource_set_baseclass:
        """Get the resources from the manager.

        Arguments
        ---------
        res_configs : dict[Any]
            A dictionary describing the resource set
        wgroup_id : int
            The id of the workergroup for which the resources are acquired

        Returns
        -------
        resource_set_baseclass
            A resource set of the specified concrete type

        Raises
        ------
        ValueError
            If the resource specification has wrong parameters
        RuntimeError
            If something went wrong while trying to acquire the resources
        """
        if self._backend == "local":
            try:
                res = resource_set_local(wgroup_id, res_config = res_configs)
            except:
                print("Resource manager caught a wrong parameter with wgroup {wgroup_id} resource set")
                raise
            self._used_cpus += res.get_nworkers()
            if self._used_cpus > self._max_cpus:
                raise RuntimeError(f"Maximum number of CPUs reached: {self._max_cpus}")
            try:
                res.request(self._configs)
            except RuntimeError:
                print(f"Resource manager failed to request local resources for wgroup {wgroup_id}")
                raise
            return res
        elif self._backend == "slurm":
            try:
                updated_configs = self._cluster.process_res_config(res_configs)
            except:
                print("Resource manager caught a wrong parameter with wgroup {wgroup_id} resource set")
                raise
            res = resource_set_slurm(wgroup_id, res_config = updated_configs)
            try:
                res.request(self._configs)
            except RuntimeError:
                print("Resource manager failed to request slurm resources for wgroup {wgroup_id}")
                raise
            return res
        else:
            raise ValueError(f"Unknown backend '{self._backend}'")

    def instanciate_resource(self, res_json : str) -> resource_set_baseclass:
        """Instanciate a resource set from a json string.
        
        Arguments
        ---------
        res_json : str
            The json string produced by serialization of a resource set

        Raises
        ------
        ValueError
            When mixing backends or providing erroneous wgroup ids
        """
        backend = json.loads(res_json)["type"]
        if backend != self._backend:
            raise ValueError("Resource manager mix-up: instanciating resource from json with different backend")

        wgroup_id = json.loads(res_json)["wgroup_id"]
        if self._backend == "local":
            res = resource_set_local(wgroup_id, json_str = res_json)
            return res

        elif self._backend == "slurm":
            res = resource_set_slurm(wgroup_id, json_str = res_json)
            return res
        else:
            raise ValueError(f"Unknown backend '{self._backend}'")

    def get_nwgroups(self) -> int:
        """Get the number of workergroups."""
        return self._nwgroups

class resource_set_local(resource_set_baseclass):
    """A local resource set class for GUTS.

    Manage the resource available locally in the session.
    This is the appropriate backend when working on a personal
    computer.

    Attributes
    ----------
    _runtime : int
        A runtime limit in seconds
    _deamonize : bool
        Release the main process, effectively deamonizing the workers ?
    """
    def _init_rs(self,
                 res_config : Optional[dict[Any]] = None,
                 json_str : Optional[str] = None) -> None:
        """Initialize the local resource set attributes.
    
        Arguments
        ---------
        res_config : Optional[dict[Any]]
            A dictionary describing the resource set
        json_str : Optional[str]
            A json string describing the resource set
        """
        if res_config:
            self._runtime = res_config.get("runtime", 100)
            self._daemonize : bool = res_config.get("daemonize", False)
        if json_str:
            json_dict = json.loads(json_str) 
            self._runtime = json_dict.get("runtime", 100)
            self._daemonize : bool = json_dict.get("daemonize", False)

    def request(self, config : dict[Any]) -> None:
        """Request the resources of the set.
        
        Use subprocess to launch the run_workergroup CLI
        with proper arguments.

        Arguments
        ---------
        config : dict[Any]
            The GUTS top-level configuration parameters dictionary.

        Raises
        ------
        RuntimeError
            If it fails to create the child process
        """
        # Check resource set was provided a definition
        assert(self._runtime > 0)

        # Process the configuration dict
        toml_file = f"input_WG{self._wgroup_id:05d}.toml"
        wgroup_config = dict(config)
        wgroup_config[f"WG{self._wgroup_id:05d}"] = {"NullField" : 0}
        with open(toml_file, "w") as f:
            toml.dump(wgroup_config, f)

        # Request command
        wgroup_cmd = ["run_workergroup"]
        wgroup_cmd.append("-i")
        wgroup_cmd.append(toml_file)
        wgroup_cmd.append("-wg")
        wgroup_cmd.append(f"{self._wgroup_id}")
        print(wgroup_cmd)

        # stdout/stderr, handles passed to the child process
        stdout_name = f"stdout_{self._wgroup_id:05d}.txt"
        stderr_name = f"stderr_{self._wgroup_id:05d}.txt"
        stdout_f = open(stdout_name, "wb")
        stderr_f = open(stderr_name, "wb")
        try:
            req_process = subprocess.Popen(wgroup_cmd,
                                           stdout = stdout_f,
                                           stderr = stderr_f,
                                           start_new_session=True)
        except Exception as e:
            print(f"Unable to request resource using subprocess.call({wgroup_cmd})")
            raise

    def serialize(self) -> str:
        """Serialize the local resource set.
        
        Returns
        -------
        str
            A json sting version of the resource set
        """
        return json.dumps({
            "type": "local",
            "wgroup_id": self._wgroup_id,
            "nworkers": self._nworkers,
            "runtime": self._runtime,
            "daemonize": self._daemonize,
        })

    def from_json(resource_set_json : str) -> resource_set_local:
        """Deserialize the local resource set."""
        res_dict = json.loads(resource_set_json)
        return resource_set_local(res_dict["wgroup_id"], json_str = resource_set_json)

    def release(self) -> None:
        """Release the resources."""
        pass

class resource_set_slurm(resource_set_baseclass):
    """A slurm resource set class for GUTS.

    Manage the resource available on an HPC cluster with the Slurm
    job scheduler.

    Attributes
    ----------
    _slurm_job_id : int | None
        The Slurm job ID, obtained after the job is submitted to the scheduler
    _slurm_job_script : list[str]
        The Slurm batch script, assembled from resource set description
    """
    def _init_rs(self,
                 res_config : Optional[dict[Any]] = None,
                 json_str : Optional[str] = None) -> None:
        """Initialize the slurm resource set.

        Arguments
        ---------
        res_config : Optional[dict[Any]]
            A dictionary describing the resource set
        json_str : Optional[str]
            A json string describing the resource set
        """
        if res_config:
            self._slurm_job_id = None
            self._slurm_submit_time = None
            self._slurm_job_script = self._build_job_script(self._wgroup_id, res_config)
            runtime = res_config.get("runtime", None)
            self._runtime = time_to_s(runtime)
        if json_str:
            json_dict = json.loads(json_str)
            self._runtime = time_to_s(str(json_dict.get("runtime")))
            self._slurm_job_id = json_dict.get("job_id")
            self._slurm_job_script = json_dict.get("job_script")

    def request(self, config : dict[Any]) -> None:
        """Request the resources of the set.

        Arguments
        ---------
        config : dict[Any]
            The GUTS top-level configuration parameters dictionary.

        Raises
        ------
        RuntimeError
            If it fails to submit the batch script
        """
        # Check resource set was provided a definition
        assert(self._runtime > 0)

        # Process the configuration dict
        toml_file = f"input_WG{self._wgroup_id:05d}.toml"
        wgroup_config = dict(config)
        wgroup_config[f"WG{self._wgroup_id:05d}"] = {"NullField" : 0}
        with open(toml_file, "w") as f:
            toml.dump(wgroup_config, f)

        try:
            job_id = submit_slurm_job(self._wgroup_id,
                                      self._slurm_job_script)
            assert job_id is not None
            self._slurm_job_id = job_id
        except Exception as e:
            print(e)
            raise RuntimeError("Unable to submit batch script to slurm for wgroup {self._wgroup_id}")
        

    def serialize(self) -> str:
        """Serialize the slurm resource set."""
        return json.dumps({
            "type": "slurm",
            "wgroup_id": self._wgroup_id,
            "nworkers": self._nworkers,
            "runtime": self._runtime,
            "job_id": self._slurm_job_id,
            "job_script": self._slurm_job_script,
        })

    def from_json(resource_set_json) -> resource_set_slurm:
        """Deserialize the slurm resource set."""
        res_dict = json.loads(resource_set_json)
        return resource_set_slurm(res_dict["wgroup_id"], json_str = resource_set_json)

    def _build_job_script(self,
                          wgroup_id : int,
                          res_config : dict[Any]) -> list[str]:
        """Build the job script from config.
        
        Arguments
        ---------
        wgroup_id : int
            The id of the worker group
        res_config : dict[Any]
            A dictionary containing the entries needed for assembling a Slurm job script

        Returns
        -------
        list[str]
            The content of the job script as a list of strings
        """
        return make_job_script_wgroup(wgroup_id, res_config)

    def release(self) -> None:
        """Release the slurm resource."""
        if self._slurm_job_id:
            try:
                cancel_slurm_job(self._slurm_job_id)
            except Exception as e:
                print(f"Error while rReleasing resource for wgroup {self._wgroup_id}")
                raise
