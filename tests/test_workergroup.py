"""Tests for the gutsutils.guts_worker class."""
import pytest
import os
import time

from guts_utils.guts_workergroup import guts_workergroup
from guts_utils.guts_queue import guts_queue
from guts_utils.guts_task import guts_task
from guts_utils.guts_event import guts_event
from guts_utils.guts_resource_manager import resource_manager

def test_init():
    """Test creating a workergroup."""
    config = {}
    res_config = {}
    wgroup = guts_workergroup(0, config, res_config)
    assert(wgroup.id() == 0)

def test_init_withqueue():
    """Test creating a workergroup with a queue."""
    queue = guts_queue()
    config = {}
    res_config = {}
    wgroup = guts_workergroup(0, config, res_config, queue = queue)

def test_attach_queue():
    """Test creating a workergroup then attach a queue."""
    config = {}
    res_config = {}
    wgroup = guts_workergroup(0, config, res_config)
    queue = guts_queue()
    wgroup.attach_queue(queue)

def test_reattach_queue():
    """Test creating a workergroup with a queue then attach a queue."""
    queue_1 = guts_queue()
    config = {}
    res_config = {}
    wgroup = guts_workergroup(0, config, res_config, queue = queue_1)
    queue_2 = guts_queue()
    with pytest.raises(Exception):
        wgroup.attach_queue(queue_2)

def test_acquire_resources_without_queue():
    """Test creating a workergroup, acquiring resources without a queue."""
    config = {}
    res_config = {}
    wgroup = guts_workergroup(0, config, res_config)
    manager = resource_manager(config)
    with pytest.raises(Exception):
        wgroup.acquire_resources(manager)

def test_acquire_resources_with_queue():
    """Test creating a workergroup, acquiring resources and a queue."""
    queue = guts_queue()
    for _ in range(10):
        queue.add_task(guts_task("function_test", {"nap_duration": 0.1}))
    config = {}
    res_config = {"nworkers": 2, "deamonize": False, "runtime": 2}
    wgroup = guts_workergroup(0, config, res_config)
    manager = resource_manager(config)
    wgroup.attach_queue(queue)
    wgroup.acquire_resources(manager)
    queue.delete(timeout=10)
