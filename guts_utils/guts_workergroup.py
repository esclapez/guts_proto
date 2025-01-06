"""A worker group class for GUTS."""
from multiprocessing import Process
import time
import sys
from typing import Any, Optional
from guts_utils.guts_error import GUTSError
from guts_utils.guts_queue import guts_queue
from guts_utils.guts_resource_manager import resource_manager

class guts_workergroup:
    """A workergroup class gathering some workers
    with a given set of resources.

    This is a volatile class, providing granularity in
    the allocation of the compute resources to workers
    within the GUTS scheduler.

    Attributes
    ----------
    _config : dict[Any]
        A dictionary of configuration parameters
    _wgroup_id : int
        The group id number
    _resource_config : dict[Any]
        A dictionary describing the compute resources associated to the group
    _resources_set : 
        A resource set object gathering resources functionalities
    _queue : guts_queue
        The GUTS queue this group interacts with
    """
    def __init__(self,
                 wgroup_id : int,
                 config : dict[Any],
                 resource_config : dict[Any],
                 queue : Optional[guts_queue] = None) -> None:
        """Initialize the worker group."""

        # Metadata
        self._config = config
        self._wgroup_id = wgroup_id

        # Resource configuration
        self._resource_config = resource_config
        self._resources_set = None

        # Set queue if present and register wgroup
        self._queue = None
        if queue:
            self._queue = queue
            self._queue.register_worker_group(wgroup_id)

    def id(self) -> int:
        """Get the id of the worker group."""
        return self._wgroup_id

    def get_queue(self) -> guts_queue | None:
        """Return the queue attached to the workergroup."""
        return self._queue

    def attach_queue(self, queue : guts_queue) -> None:
        """Attach a queue to the worker group.
        
        Arguments
        ---------
        guts_queue
            A queue with which the wgroup will interact
        
        Raises
        ------
        RuntimeError
            If the workergroup already has a queue attached
        """
        if not self._queue:
            self._queue = queue
            self._queue.register_worker_group(self._wgroup_id)
        else:
            raise RuntimeError("Cannot overwrite queue in worker group")

    def request_resources(self, manager : resource_manager) -> None:
        """Request resources for the worker group.

        This function request resources from the manager, effectively
        relinquishing the control of the program to a subprocess in
        order to harmonize workflow accross various resource backend.
        
        Arguments
        ---------
        resource_manager
            A resource manager from which to get the resources
        """
        # Check that the worker group is associated with a queue
        assert(self._queue)

        # Get the resources from the manager
        self._resource_set = manager.get_resources(self._resource_config,
                                                   self._wgroup_id)

        # Update the queue with worker group request resources info
        self._queue.update_worker_group_resources(self._wgroup_id,
                                                  self._resource_set.serialize())

    def launch(self, manager : resource_manager) -> None:
        """Launch the workers in the group.

        This is the entry-point of the workergroup after a request_resources.
        Resources are looked up from the queue, checked in the current
        environment and worker are fired up.
        
        Arguments
        ---------
        manager : resource_manager
            A resource manager to handle resource set and checks
        """
        # Check that the worker group is associated with a queue
        assert(self._queue)

        # Query the queue for the resources set
        res_json = self._queue.get_worker_group_resource(self._wgroup_id)
        if not res_json:
            raise RuntimeError(f"Unable to spawn resource set for wgroup {self._wgroup_id} from queue !")
        self._resource_set = manager.instanciate_resource(res_json)

        ## Double fork to detach the main process
        #if self._deamonize:
        #    pid = os.fork()
        #    if pid > 0:
        #        # Exit the main program immediately after fork.
        #        os._exit(0)
        #        return

        #    os.setsid()

        #    pid = os.fork()
        #    if pid > 0:
        #        os._exit(0)

        # Take a 5% margin on worker runtime
        w_runtime = self._resource_set.worker_runtime()
        wid = 0
        while not self._resource_set.workers_initiated():
            # Each worker runs the `worker_function` in a separate process
            self._resource_set.append_worker_process(Process(target=worker_function,
                                                             args=(self._queue,
                                                                   self._wgroup_id,
                                                                   wid,
                                                                   100,
                                                                   w_runtime)))
            wid = wid + 1
        
        #if not self._deamonize:
        #    for worker in self._workers:
        #        worker.join()
        
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

    print(f"Worker {wid} is working")

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
                time.sleep(0.1)

        else:
            # If no queue, wait for a while before trying again
            time.sleep(0.1)

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
