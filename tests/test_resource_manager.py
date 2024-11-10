"""Tests for the gutsutils.guts_resource_manager class."""
import pytest
import os
import time
import psutil

from guts_utils.guts_resource_manager import resource_manager

def test_init():
    """Test creating a resource manager."""
    config = {"nwgroups": 1}
    manager = resource_manager(config)
    assert(manager.get_nwgroups() == 1)

def test_get_resources():
    """Test getting the resources from the manager."""
    config = {"nwgroups": 1}
    manager = resource_manager(config)
    res_config = {"nworkers": 2, "runtime": 1}
    resources = manager.get_resources(res_config, None, 0)
    assert(resources.get_nworkers() == 2)

def test_oversubscribe_resources():
    """Test getting too many resources from the manager."""
    config = {"nwgroups": 1}
    manager = resource_manager(config)
    res_config = {"nworkers": psutil.cpu_count() + 1}
    with pytest.raises(Exception):
        resources = manager.get_resources(res_config, None, 0)
