"""Tests for the gutsutils.guts_worker class."""
import pytest
import os
import time

from guts_utils.guts_worker import guts_workergroup
from guts_utils.guts_queue import guts_queue
from guts_utils.guts_task import guts_task
from guts_utils.guts_event import guts_event

def test_init():
    """Test creating a workergroup."""
    config = {}
    resource_config = {"nworkers": 1}
    worker = guts_workergroup(0, config, resource_config)
    assert(worker.id() == 0)

def test_acquire_resources():
    """Test creating a workergroup and acquiring resources."""
    config = {}
    resource_config = {"nworkers": 1, "deamonize": False, "runtime": 2}
    worker = guts_workergroup(0, config, resource_config)
    worker.acquire_resources()

def test_acquire_resources_with_queue():
    """Test creating a workergroup, acquiring resources and a queue."""
    queue = guts_queue()
    for _ in range(10):
        queue.add_task(guts_task("function_test", {"nap_duration": 0.1}))
    config = {}
    resource_config = {"nworkers": 2, "deamonize": False, "runtime": 6}
    worker = guts_workergroup(0, config, resource_config)
    worker.set_queue(queue)
    worker.acquire_resources()
    queue.delete_queue(timeout=15)
