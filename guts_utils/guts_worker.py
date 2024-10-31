"""A worker group class for GUTS."""
from typing import Any
import time
import os
import sys
from multiprocessing import Process
from guts_utils.guts_queue import guts_queue
from guts_utils.guts_task import task_functions_reg

class guts_workergroup:
    def __init__(self,
                 workerg_id : int,
                 config : dict[Any],
                 resource_config : dict[Any]) -> None:
        self._config = config
        self._workerg_id = workerg_id
        self._resource_config = resource_config
        self._resources_set = None
        self._nworkers = resource_config.get('nworkers', 1)
        self._runtime = resource_config.get('runtime', 10)
        self._deamonize = resource_config.get('deamonize', True)
        self._queue : guts_queue = None
        self._workers = []

    def id(self) -> int:
        """Get the id of the worker group."""
        return self._workerg_id

    def set_queue(self, queue : guts_queue) -> None:
        """Set the queue for the worker group."""
        self._queue = queue

    def acquire_resources(self):
        """ Acquire resources for the worker group. """
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
            self._workers.append(Process(target=worker_function, args=(self._queue,
                                                                       self._workerg_id,
                                                                       i,
                                                                       100,
                                                                       self._runtime)))
            self._workers[-1].start()

        if not self._deamonize:
            for worker in self._workers:
                worker.join()

# Worker function that will run in a separate process
def worker_function(queue : guts_queue,
                    wkgroup_id : int,
                    worker_id : int,
                    max_tasks : int,
                    runtime : int) -> None:
    # Start the timer to trigger runtime termination
    time_start = time.time()

    # Set a tuple uniquely defining the worker
    wid = (wkgroup_id, worker_id)

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
