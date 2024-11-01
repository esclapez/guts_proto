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
