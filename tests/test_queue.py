"""Tests for the gutsutils.guts_queue class."""
import pytest
import os

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

def test_add_task():
    """Test adding a task to the queue."""
    queue = guts_queue()
    task = guts_task("dummy", {})
    queue.add_task(task)
    assert queue.get_tasks_count() == 1
    os.remove("./guts_queue.db")

def test_add_event():
    """Test adding a task to the queue."""
    queue = guts_queue()
    event = guts_event(1, "DoNothing")
    queue.add_event(event)
    assert queue.get_events_count() == 1
    os.remove("./guts_queue.db")
