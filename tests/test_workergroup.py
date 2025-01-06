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
    wgroup = guts_workergroup(12, config, res_config, queue = queue)
    assert(wgroup.id() == 12)
    assert queue.get_worker_groups_count() == 1
    queue.delete(timeout=2)

def test_attach_queue():
    """Test creating a workergroup then attach a queue."""
    config = {}
    res_config = {}
    wgroup = guts_workergroup(42, config, res_config)
    queue = guts_queue()
    wgroup.attach_queue(queue)
    assert(wgroup.id() == 42)
    assert queue.get_worker_groups_count() == 1
    queue.delete(timeout=2)

def test_reattach_queue():
    """Test creating a workergroup with a queue then attach a queue."""
    queue_1 = guts_queue()
    config = {}
    res_config = {}
    wgroup = guts_workergroup(0, config, res_config, queue = queue_1)
    queue_2 = guts_queue() # In practice, same as queue_1
    with pytest.raises(Exception):
        wgroup.attach_queue(queue_2)
    queue_1.delete(timeout=2)

def test_request_resources_without_queue():
    """Test requiring resources without a queue."""
    config = {}
    res_config = {}
    wgroup = guts_workergroup(0, config, res_config)
    manager = resource_manager(config)
    with pytest.raises(Exception):
        wgroup.request_resources(manager)

def test_launch_without_queue():
    """Test launching without a queue."""
    config = {}
    res_config = {}
    wgroup = guts_workergroup(0, config, res_config)
    manager = resource_manager(config)
    with pytest.raises(Exception):
        wgroup.launch(manager)

def test_request_resources_with_queue_erroneous_res():
    """Test requiring resources with a queue, erroneous resources"""
    # Setup wgroup
    config = {}
    res_config = {"nworkers": 10000}
    wgroup = guts_workergroup(0, config, res_config)
    # Attach queue
    queue = guts_queue("myqueue_there.db")
    wgroup.attach_queue(queue)
    manager = resource_manager(config)
    with pytest.raises(Exception):
        wgroup.request_resources(manager)
    queue.delete(timeout=2)

def test_request_resources_with_queue():
    """Test creating a workergroup, requiring resources and a queue."""
    # Setup wgroup
    config = {"case" : {"queue_file" : "myqueue.db"}}
    res_config = {"nworkers": 2, "deamonize": False, "runtime": 3}
    wgroup = guts_workergroup(0, config, res_config)
    # Attach queue
    queue = guts_queue("myqueue.db")
    wgroup.attach_queue(queue)
    # Define resource manager and request resources for the group
    manager = resource_manager(config)
    wgroup.request_resources(manager)
    # Check queue for 
    assert queue.get_worker_groups_count() == 1
    assert queue.get_worker_group_resource is not None
    time.sleep(1)
    queue.delete(timeout=5)

def test_request_resources_with_queue_and_tasks():
    """Test creating a workergroup, with resources, queue and tasks."""
    # Setup wgroup
    config = {"case" : {"queue_file" : "myqueue.db"}}
    res_config = {"nworkers": 2, "deamonize": False, "runtime": 3}
    wgroup = guts_workergroup(0, config, res_config)
    # Attach queue
    queue = guts_queue("myqueue.db")
    for _ in range(10):
        queue.add_task(guts_task("function_test", {"nap_duration": 0.1}))
    wgroup.attach_queue(queue)
    # Define resource manager and request resources for the group
    manager = resource_manager(config)
    wgroup.request_resources(manager)
    # Give times for tasks to run
    time.sleep(2)
    # Check queue for number of tasks done
    assert queue.get_completed_tasks() == 10
    queue.delete(timeout=5)
