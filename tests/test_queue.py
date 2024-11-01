"""Tests for the gutsutils.guts_queue class."""
import pytest
import os
import uuid

from guts_utils.guts_queue import guts_queue
from guts_utils.guts_task import guts_task
from guts_utils.guts_event import guts_event

def test_initDB():
    """Test creating a database."""
    queue = guts_queue()
    assert queue.db_name == 'guts_queue.db'
    os.remove("./guts_queue.db")

def test_initNamedDB():
    """Test creating a database with a custom name."""
    queue = guts_queue("myguts_queue.db")
    assert queue.db_name == 'myguts_queue.db'
    os.remove("./myguts_queue.db")

def test_delete_queue():
    """Test deleting the queue."""
    queue = guts_queue()
    queue.delete(timeout=2)

def test_add_task():
    """Test adding a task to the queue."""
    queue = guts_queue()
    task = guts_task("dummy", {})
    queue.add_task(task)
    assert queue.get_remaining_tasks_count() == 1
    os.remove("./guts_queue.db")

def test_add_task_with_deps():
    """Test adding a task with dependencies to the queue."""
    queue = guts_queue()
    task = guts_task("dummy", {})
    task_uuid = queue.add_task(task)
    queue.add_task(task, task_uuid)
    assert queue.get_remaining_tasks_count() == 2
    os.remove("./guts_queue.db")

def test_add_task_with_unknown_deps():
    """Test adding a task with unknown dependencies to the queue."""
    queue = guts_queue()
    task = guts_task("dummy", {})
    random_uuid = uuid.uuid4()
    with pytest.raises(Exception):
        queue.add_task(task, task_uuid)
    os.remove("./guts_queue.db")

def test_mark_task_done():
    """Test marking a task as done."""
    queue = guts_queue()
    task_id = queue.add_task(guts_task("dummy", {}))
    queue.mark_task_done(task_id)
    assert queue.get_tasks_count() == 1
    assert queue.get_remaining_tasks_count() == 0
    os.remove("./guts_queue.db")

def test_mark_task_inprogress():
    """Test marking a task as inprogress."""
    queue = guts_queue()
    task_id = queue.add_task(guts_task("dummy", {}))
    _ = queue.fetch_task()
    assert queue.get_running_tasks_count() == 1
    os.remove("./guts_queue.db")

def test_add_event():
    """Test adding an event to the queue."""
    queue = guts_queue()
    event = guts_event(1, action = "dummy", target = "dummy")
    queue.add_event(event)
    assert queue.get_events_count() == 1
    os.remove("./guts_queue.db")

def test_fetch_event():
    """Test fetching an event in the queue."""
    queue = guts_queue()
    event = guts_event(1, action = "dummy", target = "dummy")
    queue.add_event(event)
    _ = queue.fetch_event()
    assert queue.get_events_count() == 1
    os.remove("./guts_queue.db")

def test_fetch_event_counter():
    """Test fetching multiple times an event in the queue."""
    queue = guts_queue()
    event = guts_event(1, action = "dummy", target = "dummy")
    queue.add_event(event)
    _ = queue.fetch_event()
    _ = queue.fetch_event()
    _, count, _ = queue.fetch_event()
    assert count == 3
    os.remove("./guts_queue.db")

def test_register_worker():
    """Test registering a worker."""
    queue = guts_queue()
    queue.register_worker((0,0))
    queue.register_worker((0,1))
    assert queue.get_workers_count() == 2
    os.remove("./guts_queue.db")

def test_unregister_worker():
    """Test unregistering a worker."""
    queue = guts_queue()
    queue.register_worker((0,0))
    queue.unregister_worker((0,0))
    assert queue.get_workers_count() == 0
    os.remove("./guts_queue.db")

def test_update_worker_status():
    """Test registering a worker."""
    queue = guts_queue()
    queue.register_worker((0,0))
    queue.update_worker_status((0,0), "working")
    assert queue.get_active_workers_count() == 1
    os.remove("./guts_queue.db")

def test_register_worker_group():
    """Test registering a worker group."""
    queue = guts_queue()
    queue.register_worker_group(1)
    assert queue.check_worker_group(1) == "waiting"
    os.remove("./guts_queue.db")

def test_unregister_worker_group():
    """Test unregistering a worker group."""
    queue = guts_queue()
    queue.register_worker_group(0)
    queue.unregister_worker_group(0)
    assert queue.get_worker_groups_count() == 0
    os.remove("./guts_queue.db")

def test_update_worker_group_status():
    """Test registering a worker."""
    queue = guts_queue()
    queue.register_worker_group(0)
    queue.update_worker_group_status(0, "working")
    assert queue.check_worker_group(0) == "working"
    os.remove("./guts_queue.db")
