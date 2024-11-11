"""Tests for the gutsutils.guts_resource_manager class."""
import pytest
import os
import time
import psutil

from guts_utils.guts_resource_manager import resource_manager, resource_set_slurm

def test_init_manager():
    """Test creating a resource manager."""
    config = {"nwgroups": 1}
    manager = resource_manager(config)
    assert(manager.get_nwgroups() == 1)

def test_init_manager_unknown_backend():
    """Test creating a resource manager with unknown backend"""
    config = {"nwgroups": 1, "resource" : {"backend" : "WillFail"}}
    manager = resource_manager(config)
    with pytest.raises(Exception):
        _ = manager.get_resources({}, None, 0)

def test_get_resources():
    """Test getting the resources from the manager."""
    config = {"nwgroups": 1}
    manager = resource_manager(config)
    res_config = {"nworkers": 2, "runtime": 1}
    resources = manager.get_resources(res_config, None, 0)
    assert(resources.get_nworkers() == 2)

def test_oversubscribe_resources():
    """Test getting too many resources from the manager."""
    config = {"resource" : {"nwgroups": 1}}
    manager = resource_manager(config)
    res_config = {"nworkers": psutil.cpu_count() + 1}
    with pytest.raises(Exception):
        resources = manager.get_resources(res_config, None, 0)

def test_define_slurm_resource_set(slurm_available):
    res_config = {"nworkers": 1}
    res = resource_set_slurm(res_config, 0)

def test_get_resources_slurm(slurm_available):
    config = {"resource" : {"nwgroups": 1, "backend": "slurm"}}
    manager = resource_manager(config)
    res_config = {"nworkers": 2, "runtime": 10}
    resources = manager.get_resources(res_config, None, 0)
