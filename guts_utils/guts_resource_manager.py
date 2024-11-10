"""A resource set/manager classes for GUTS."""
from abc import ABCMeta, abstractmethod
from multiprocessing import Process
from guts_utils.guts_queue import guts_queue
from guts_utils.guts_error import GUTSError
from guts_utils.guts_sys_utils import get_cpu_count
from typing import Any
import json
import os
import sys
import time

class resource_set_baseclass(metaclass=ABCMeta):
    """A resource set base class for GUTS."""
    def __init__(self,
                 res_config : dict[Any],
                 wgroup_id : int):
        """Initialize the resource set."""
        self._wgroup_id = wgroup_id
        self._init_rs(res_config)

    @abstractmethod
    def _init_rs(self, res_config : dict[Any]):
        """Asbtract method initializing the resource set."""
        pass

    @abstractmethod
    def acquire_resources(self, queue : guts_queue):
        """Acquire the resources of the set."""
        pass

    @abstractmethod
    def serialize(self):
        """Serialize the resource set."""
        pass

class resource_manager:
    """A resource manager class for GUTS."""
    def __init__(self,
                 configs : dict[Any]):
        """Initialize the resource manager."""
        self._nwgroups : int = configs.get("resource",{}).get("nwgroups", 1)
        self._backend : str = configs.get("resource",{}).get("backend", "local")
        self._max_cpus : int = get_cpu_count()
        self._used_cpus : int = 0

    def get_resources(self,
                      res_configs : dict[Any],
                      queue : guts_queue,
                      wgroup_id : int) -> resource_set_baseclass:
        """Get the resources from the manager."""
        if self._backend == "local":
            res = resource_set_local(res_configs, wgroup_id)
            self._used_cpus += res.get_nworkers()
            if self._used_cpus > self._max_cpus:
                raise GUTSError(f"Maximum number of CPUs reached: {self._max_cpus}")
            res.acquire_resources(queue)
            return res
        elif self._backend == "slurm":
            res = resource_set_slurm(res_config, wgroup_id)
            res.acquire_resources(queue)
            return res
        else:
            raise ValueError(f"Unknown backend '{self._backend}'")

    def get_nwgroups(self):
        """Get the number of worker groups."""
        return self._nwgroups

class resource_set_local(resource_set_baseclass):
    """A local resource set class for GUTS."""
    def _init_rs(self, res_config : dict[Any]):
        """Initialize the local resource set."""
        self._nworkers : int = res_config.get("nworkers", 1)
        self._workers : list[Process] = []
        self._workers_pids : list[int] = []
        self._runtime : int = res_config.get("runtime", 100)
        self._deamonize : bool = res_config.get("deamonize", False)

    def acquire_resources(self, queue : guts_queue):
        """Acquire the resources of the set."""
        # Double fork to detach the main process
        if self._deamonize:
            pid = os.fork()
            if pid > 0:
                # Exit the main program immediately after fork.
                os._exit(0)
                return

            os.setsid()

            pid = os.fork()
            if pid > 0:
                os._exit(0)

        for i in range(self._nworkers):
            # Each worker runs the `worker_function` in a separate process
            self._workers.append(Process(target=worker_function, args=(queue,
                                                                       self._wgroup_id,
                                                                       i,
                                                                       100,
                                                                       self._runtime)))
            self._workers[-1].start()
            self._workers_pids.append(self._workers[-1].pid)

        if not self._deamonize:
            for worker in self._workers:
                worker.join()

    def serialize(self):        
        """Serialize the local resource set."""
        return json.dumps({
            "nworkers": self._nworkers,
            "workers": self._workers_pids
        })

    def get_nworkers(self):
        """Get the number of workers."""
        return self._nworkers
        

#class resource_set_slurm(resource_set_baseclass):
#    """A slurm resource set class for GUTS."""
#    def _init_rs(self):
#        """Initialize the slurm resource set."""
#        pass


# Worker function that will run in a separate process
def worker_function(queue : guts_queue,
                    wgroup_id : int,
                    worker_id : int,
                    max_tasks : int,
                    runtime : int) -> None:
    # Start the timer to trigger runtime termination
    time_start = time.time()

    # Set a tuple uniquely defining the worker
    wid = (wgroup_id, worker_id)

    # Worker might be queue-less for testing purposes
    if queue:
        # Register worker in queue
        queue.register_worker(wid)

    # TODO: add a hook function to initialize the worker data/ojects
    while True:
        # Check for function runtime termination
        if time.time() - time_start > runtime:
            print(f"Worker {worker_id} is stopping as the function runtime has exceeded {runtime} seconds.")
            if queue:
                queue.unregister_worker(wid)
            break

        # Worker might be queue-less for testing purposes
        if queue:
            # Check the completed task count
            if queue.get_completed_tasks() >= max_tasks:
                print(f"Worker {worker_id} is stopping as {max_tasks} tasks have been completed.")
                queue.unregister_worker(wid)
                break

            # Check for events in the queue
            event_data = queue.fetch_event()
            if event_data:
                event_id, event_count, event = event_data
                process_event(event, wid, queue)

            # Check for tasks in the queue
            task_data = queue.fetch_task()
            if task_data:
                task_id, task_uuid, task = task_data
                queue.update_worker_status(wid, f'in_progress-{task_id}')
                print(f"Worker {worker_id} picked up task {task_id}")

                # Execute the task using the registered functions
                task.execute()

                # Mark task as done
                queue.mark_task_done(task_uuid)
                print(f"Worker {worker_id} completed task {task_id}")

                # Atomically increment the completed task counter
                completed_tasks = queue.increment_completed_tasks()
                queue.update_worker_status(wid, 'waiting')
                print(f"Total completed tasks: {completed_tasks}")
            else:
                # If no tasks, wait for a while before trying again
                print(f"Worker {worker_id} is waiting for tasks...")
                time.sleep(0.5)

        else:
            # If no queue, wait for a while before trying again
            time.sleep(0.5)

def process_event(event : dict[Any],
                  wid : tuple[int,int],
                  queue : guts_queue) -> None:
    """ Process the event action."""
    wid_str = f"{wid[0]}-{wid[1]}"
    if not (event.target == wid_str or event.target == 'all'):
        return
    print("Processing event")
    if event.action in event_actions_dict:
        event_actions_dict[event.action](wid, queue)

def stop_worker(wid : tuple[int,int], queue : guts_queue) -> None:
    """ Action to stop a worker """
    print("Stopping worker")
    queue.unregister_worker(wid)
    sys.exit(0)

# Dictionary of event actions
event_actions_dict = {
        "worker-kill": stop_worker,
        }
