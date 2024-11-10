"""A worker group class for GUTS."""
from typing import Any, Optional
from guts_utils.guts_error import GUTSError
from guts_utils.guts_queue import guts_queue
from guts_utils.guts_resource_manager import resource_manager

class guts_workergroup:
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

    def attach_queue(self, queue : guts_queue) -> None:
        """Attach a queue to the worker group."""
        if not self._queue:
            self._queue = queue
            self._queue.register_worker_group(self._wgroup_id)
        else:
            raise GUTSError("Cannot overwrite queue in worker group")

    def acquire_resources(self, manager : resource_manager) -> None:
        """Acquire resources for the worker group."""
        # Check that the worker group is associated with a queue
        assert(self._queue)

        # Get the resources from the manager
        self._resource_set = manager.get_resources(self._resource_config,
                                                   self._queue,
                                                   self._wgroup_id)

        # Update the queue with worker group resources info
        self._queue.update_worker_group_resources(self._wgroup_id,
                                                  self._resource_set.serialize())
