"""A set of SLURM utilities for GUTS."""
import os
import shlex
import shutil
import subprocess
from typing import Any
from guts_utils.guts_sys_utils import get_username

_slurm_dir_prefix : str = "#SBATCH"
_slurm_header : list[str] = ["#!/bin/bash"]
_slurm_group_cmd : str  = "run_workergroup"

def is_slurm_avail() -> bool:
    """Assess if slurm is available on system.

    Detect if Slurm is available in the compute environment.
    First checking for environment variable, then checking
    the `sinfo` command.
    
    Returns
    -------
    bool
        A boolean True if Slurm is detected, False otherwise
    """
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

def time_to_s(slurm_time : str) -> int:
    """Convert a Slurm formatted time to seconds.

    Arguments
    ---------
    str
        A Slurm formatted time d-hh:mm:ss

    Returns
    -------
    int
        Time converted to seconds

    Raises
    ------
    ValueError
        If the input string is not properly formatted
    """
    # There is a chance time is "infinite"
    # -> Make that 10 days
    if slurm_time.strip() == "infinite":
        return 10 * 60 * 3600 * 24

    time = 0
    ndays = "0" 
    if slurm_time.find("-") == -1:
        hms = slurm_time.split(":")
    else:
        ndays, intra_day = slurm_time.split("-")
        hms = intra_day.split(":")

    # Checks
    if len(hms) > 3:
        raise ValueError(f"Format error: {hms} does not match hh:mm:ss")
    for e in hms:
        if len(e) > 2 or not e.isdigit():
            raise ValueError(f"Format error: {hms} does not match hh:mm:ss")
    if not ndays.isdigit():
        raise ValueError(f"Format error: {nday} is not a number in d-hh:mm:ss")

    # Aggretates hours, minutes and seconds
    if len(hms) == 1:
        time = time + int(hms[0])
    elif len(hms) == 2:
        time = time + int(hms[1])
        time = time + 60 * int(hms[0])
    elif len(hms) == 3:
        time = time + int(hms[2])
        time = time + 60 * int(hms[1])
        time = time + 3600 * int(hms[0])

    time = time + int(ndays) * 60 * 3600 * 24
    return time


class SlurmCluster:
    """  
    A class to describe the Slurm Cluster compute resources.

    Attributes
    ----------
    sinfo_executable : str
        Name or path to the sinfo executable, by default "sinfo".
    sacct_executable : str
        Name or path to the sacct executable, by default "sacct".
    """

    sinfo_executable = "sinfo"
    sacct_executable = "sacct"

    def __init__(self) -> None:
        """SlurmCluster initialize function.

        Attributes
        ----------
        _all_nodes : list[str]
            The list of nodes available
        _all_partitions : dict[str:int]
            The list of partitions available with number of nodes
        _default_partition : str | None
            Name of the default partition
        _all_cpu_per_nodetypes : dict[str:int]
            Number of CPUs available on each type of nodes
        _all_gpu_per_nodetypes : dict[str:int]
            Number of GPUs available on each type of nodes

        Raises
        ------
        RuntimeError
            If Slurm is not available on the system during initalization
        """
        if not is_slurm_avail():
            raise RuntimeError("Trying to initialize SlurmCluster without SLURM.")
        self._all_nodes = self._list_all_nodes()
        self._all_part_nnodes, self._all_part_maxtimes, \
            self._default_partition = self._list_all_partitions()
        self._all_cpu_per_nodetypes = self._list_all_partition_cpus()
        self._all_gpu_per_nodetypes = self._list_all_partition_gpus()

    def _list_all_partitions(self) -> tuple[dict[str:int],dict[str:str],str]:
        """Query Slurm to get the list of partitions.
            
        Returns
        -------
        tuple
            dict[str:int]: a dict total number of nodes for each partition
            dict[str:int]: a dict of max runtime for each partition
            str: the default partition
        """
        nnodes_dict = {}
        mtimes_dict = {}
        sinfo_cmd = f"{self.sinfo_executable} --noheader --format='%P %F %l'"
        sinfo_out = subprocess.check_output(shlex.split(sinfo_cmd), text=True)
        partitions_ncount_list = sinfo_out.split("\n")[:-1]
        # Find the default if defined, total number of nodes per partition
        default_partition = None
        for p in range(len(partitions_ncount_list)):
            partition, ncount_s, mtimes_s = partitions_ncount_list[p].split(" ")
            if partition[-1] == "*":
                partition = partition[:-1]
                default_partition = partition
            ncount = ncount_s.split("/")[3]
            nnodes_dict[partition] = int(ncount)
            mtimes_dict[partition] = time_to_s(mtimes_s.strip())
        return nnodes_dict, mtimes_dict, default_partition

    def _list_all_nodes(self) -> list[str]:
        """Query Slurm to get the list of nodes.

        Returns
        -------
        list[str]
            the list of nodes on the system   
        """
        sinfo_cmd = f"{self.sinfo_executable} --noheader --format='%n'"
        sinfo_out = subprocess.check_output(shlex.split(sinfo_cmd), text=True)
        nodes_list = sinfo_out.split("\n")[:-1]
        return nodes_list

    def _list_all_partition_cpus(self) -> dict[str:int]:
        """Query Slurm to get number of CPUs for each partition.
    
        Returns:
        dict[str:int]
            A dictionary with the number of CPU for each partition name
        """
        cpus_dict = {}
        sinfo_cmd = f"{self.sinfo_executable} --noheader --format='%P %c'"
        sinfo_out = subprocess.check_output(shlex.split(sinfo_cmd), text=True)
        ncpu_list = sinfo_out.split("\n")[:-1]    
        for part_cpu in ncpu_list:
            part, cpu = part_cpu.split(" ")
            if part[-1] == "*":
                part = part[:-1]
            cpus_dict[part] = int(cpu)
        return cpus_dict

    def _list_all_partition_gpus(self) -> dict[str:int]:
        """Query Slurm to get number of GPUs for each partition.
    
        Returns:
        dict[str:int]
            A dictionary with the number of GPU for each partition name
        """
        gpus_dict = {}
        sinfo_cmd = f"{self.sinfo_executable} --noheader --format='%P %G'"
        sinfo_out = subprocess.check_output(shlex.split(sinfo_cmd), text=True)
        gres_list = sinfo_out.split("\n")[:-1]    
        for part_gres in gres_list:
            ngpus = 0
            part, gres = part_gres.split(" ")
            if part[-1] == "*":
                part = part[:-1]
            # Count the total number of GPUs, maybe multiple type
            # of GPUs on a single node
            if "gpu:" in gres:
                gres_l = gres.split(",")
                for g in gres_l:
                    if "gpu:" in g:
                        ngpus += int(g.split(":")[2][0])
            gpus_dict[part] = ngpus
        return gpus_dict
        
    def get_node_count(self) -> int:
        """Get the number of nodes on the system.
        
        Returns
        -------
        int
            number of nodes on the system
        """
        return len(self._all_nodes)

    def get_partition_count(self) -> int:
        """Get the number of partitions on the system.
        
        Returns
        -------
        int
            number of partitions on the system
        """
        return len(self._all_part_nnodes)

    def process_res_config(self, res_config : dict[Any]) -> dict[Any]:
        """Process the input config dictionary.

        Check the sanity of the keys contained in the dictionary
        and append identified missing keys with defaults.

        Arguments
        ---------
        dict[Any]
            A dictionary listing resource configuration

        Returns
        -------
        dict[Any]
            An updated dictionary listing resource configuration

        Raises
        ------
        ValueError
            If a configuration key as a wrong value (partition, node, ...)
        """
        # Check the sanity of the provided parameters
        self._check_res_config(res_config)
        # Append default parameters
        updated_config = self._update_res_config(res_config)
        return updated_config

    def _check_res_config(self, res_config : dict[Any]): 
        """Check the sanity of the user-provided parameters.
        
        Arguments
        ---------
        dict[Any]
            A dictionary listing resource configuration

        Raises
        ------
        ValueError
            If a configuration key as a wrong value (partition, node, ...)
        """
        # Check partition name if provided
        partition = res_config.get("partition", None)
        if partition:
            if partition not in self._all_part_nnodes.keys():
                raise ValueError(f"Partition {partition} unknown!")

        if not partition and not self._default_partition:
            raise ValueError("Partition not provided and system has no default!")

        # Assume the default partition hereafter
        if not partition:
            partition = self._default_partition

        # Check time format and limit
        time = res_config.get("runtime", None)
        if not time:
            raise ValueError("No runtime specified ! Use d-hh:mm:ss Slurm format")
        time_s = time_to_s(time)
        if time_s > self._all_part_maxtimes[partition]:
            raise ValueError(f"Requested runtime {time} exceeds partition limit!")

        # Slurm has a complex rationale when it comes to 
        # nodes and cpus, just catch obvious errors here
        nnodes = res_config.get("nodes", None)
        if nnodes:
            if nnodes > self._all_part_nnodes[partition]:
                raise ValueError(f"Requested number of nodes {nnodes} exceeds partition max count!")

        ngpus = res_config.get("gpus-per-node", None)
        if ngpus:
            if ngpus > self._all_gpu_per_nodetypes[partition]:
                raise ValueError(f"Requested number of GPUs {ngpus} per node exceeds the node limit!")


    def _update_res_config(self, res_config : dict[Any]) -> dict[Any]: 
        """Update resource configuration parameters with defaults.
        
        Arguments
        ---------
        dict[Any]
            A dictionary listing resource configuration

        Returns
        -------
        dict[Any]
            An updated dictionary listing resource configuration
        """
        updated_config = res_config
        partition = res_config.get("partition", None)
        if not partition:
            updated_config["partition"] = self._default_partition

        return updated_config

def make_job_script_wgroup(wgroup_id : int,
                           res_config: dict[Any]) -> list[str]:
    """Assemble a workergroup job script from resource config.

    Arguments
    ---------
    int
        The workergroup index number
    dict[Any]
        The resource configuration specification

    Returns
    -------
    list[str]
        A full Slurm batch script as a list of strings, one per line
    """
    # Initialize with header
    job_script = list(_slurm_header)
    job_script.append(f"{_slurm_dir_prefix} --job-name=GUTS_WG{wgroup_id:05d}")

    # Append mandatory directives
    runtime = res_config.get("runtime")
    partition = res_config.get("partition")
    nnodes = res_config.get("nodes")
    job_script.append(f"{_slurm_dir_prefix} --time={runtime}")
    job_script.append(f"{_slurm_dir_prefix} --partition={partition}")
    job_script.append(f"{_slurm_dir_prefix} --nodes={nnodes}")

    # Account can be default associated to user in SLURM
    account = res_config.get("account", None)
    if account is not None:
        job_script.append(f"{_slurm_dir_prefix} --account={account}")

    # Add any user-defined extra directives
    extra_dirs = res_config.get("extra_directives", {})
    for key, value in extra_dirs.items():
        if value:
            job_script.append(f"{_slurm_dir_prefix} {key}={value}")
        else:
            job_script.append(f"{_slurm_dir_prefix} {key}")

    # Add pre-command lines
    pre_cmd_list = res_config.get("pre_cmd_list", None)
    if pre_cmd_list is not None:
        for line in pre_cmd_list:
            job_script.append(line)

    toml_file = f"input_WG{wgroup_id:05d}.toml"
    full_slurm_group_cmd = f"{_slurm_group_cmd} -i {toml_file} -wg {wgroup_id}"
    job_script.append(full_slurm_group_cmd)

    post_cmd_list = res_config.get("post_cmd_list", None)
    if post_cmd_list is not None:
        for line in post_cmd_list:
            job_script.append(line)

    return job_script

def submit_slurm_job(wgroup_id : int,
                     job_script : list[str]) -> int | None:
    """Submit a job to the Slurm queue.

    Arguments
    ---------
    job_script : list[str]
        The job batch script as a list of strings

    Returns
    -------
    int
        The submitted SLURM_JOB_ID
    """
    sbatch = "sbatch"

    # Dump script to temporary file
    tmp_batch = f".WG{wgroup_id:05d}.batch"
    with open(tmp_batch, 'w') as f:
        for line in job_script:
            f.write(f"{line}\n")

    sbatch_cmd = [sbatch]
    sbatch_cmd.append(tmp_batch)
    try:
        result = subprocess.run(sbatch_cmd, stdout=subprocess.PIPE) 
        success_msg = 'Submitted batch job'
        stdout = result.stdout.decode('utf-8')
        assert success_msg in stdout, result.stderr
        job_id = int(stdout.split(' ')[3])
        return job_id
    except subprocess.CalledProcessError as e:
        print(e)
        raise RuntimeError("Unable to submit job to Slurm queue.""")
    return None

def get_inqueue_slurm_jobs() -> list[dict[Any]]:
    """Get the list of jobs currently in queue."""
    squeue = "squeue"
    user = get_username()

    job_list = []

    squeue_cmd = [squeue]
    squeue_cmd.append("-u")
    squeue_cmd.append(user)
    squeue_cmd.append("--format='%i %P %j %t %M %D'")
    try:
        result = subprocess.run(squeue_cmd, stdout=subprocess.PIPE) 
        header_msg = 'JOBID PARTITION'
        stdout = result.stdout.decode('utf-8')
        assert header_msg in stdout, result.stderr
        job_raw_list = stdout.split("\n")[1:-1]
        for job in job_raw_list:
            job_id, part, jname, status, rtime, nnode = job[1:-1].split(" ") 
            job_list.append({"id" : int(job_id),
                             "partition" : part,
                             "name" : jname,
                             "status" : status,
                             "nnode" : int(nnode),
                             "runtime" : rtime})
        return job_list
    except subprocess.CalledProcessError as e:
        print(e)
        raise RuntimeError("Unable to query jobs in the Slurm queue.""")

    return job_list

def get_past_slurm_jobs() -> list[dict[Any]]:
    """Get the list of jobs recently submitted."""
    sacct = "sacct"
    user = get_username()

    job_list = []

    sacct_cmd = [sacct]
    sacct_cmd.append("-u")
    sacct_cmd.append(user)
    sacct_cmd.append("-X")
    sacct_cmd.append("-o")
    sacct_cmd.append("jobid,partition,jobname,state,time,nnodes")
    try:
        result = subprocess.run(sacct_cmd, stdout=subprocess.PIPE) 
        header_msg = 'JobID'
        stdout = result.stdout.decode('utf-8')
        assert header_msg in stdout, result.stderr
        job_raw_list = stdout.split("\n")[2:-1]
        for job in job_raw_list:
            ' '.join(job.split())
            print(job)
            job_id, part, jname, state, wtime, nnode = job.split(" ") 
            job_list.append({"id" : int(job_id),
                             "partition" : part,
                             "name" : jname,
                             "state" : state,
                             "nnode" : int(nnode),
                             "walltime" : wtime})
        return job_list
    except subprocess.CalledProcessError as e:
        print(e)
        raise RuntimeError("Unable to query jobs in the Slurm history using sacct.""")

    return job_list

def cancel_slurm_job(job_id : int) -> None:
    """Cancel a job.

    Arguments
    ---------
    job_id : int
        The job id
    """
    scancel = "scancel"

    # Get the list of jobs in queue
    inqueue_jobs = get_inqueue_slurm_jobs()
    is_running = False   
    for jobs in inqueue_jobs:
        if jobs.get("id") == job_id:
            is_running = True
            break

    # Check jobs history
    if not is_running:
        past_jobs = get_past_slurm_jobs()    

    sbatch_cmd = [sbatch]
    sbatch_cmd.append(tmp_batch)
    try:
        result = subprocess.run(sbatch_cmd, stdout=subprocess.PIPE) 
        success_msg = 'Submitted batch job'
        stdout = result.stdout.decode('utf-8')
        assert success_msg in stdout, result.stderr
        job_id = int(stdout.split(' ')[3])
        return job_id
    except subprocess.CalledProcessError as e:
        print(e)
        raise RuntimeError("Unable to submit job to Slurm queue.""")
    return None
