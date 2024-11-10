"""Tests for the gut_scheduler class."""
import pytest
import os
import toml
from guts_utils.guts_scheduler import guts_scheduler

def test_init_scheduler():
    """Test creating a scheduler."""
    with open("input.toml", 'w') as f:
        toml.dump({"case": {"name": "test"}}, f)
    guts_sched = guts_scheduler(a_args=[])
    assert (guts_sched.name() == "test")
    assert os.path.exists("test_queue.db") is True
    guts_sched.cleanup()

def test_start_scheduler():
    """Test starting a scheduler."""
    with open("input.toml", 'w') as f:
        toml.dump({"case": {"name": "test"},
                   "resource": {"nwgroups": 2,
                                "config": {"nworkers": 1, "runtime": 1}}}, f)
    guts_sched = guts_scheduler(a_args=[])
    guts_sched.start()
    assert guts_sched.get_queue().get_worker_groups_count() == 2
    guts_sched.cleanup()

def test_oversubscribe_scheduler():
    """Test oversubscribing a scheduler."""
    with open("input.toml", 'w') as f:
        toml.dump({"case": {"name": "test"},
                   "resource": {"nwgroups": 4,
                                "config": {"nworkers": 3, "runtime": 1}}}, f)
    guts_sched = guts_scheduler(a_args=[])
    with pytest.raises(Exception):
        guts_sched.start()
