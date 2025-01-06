"""Tests for the gutsutils.guts_resource_manager class."""
import pytest
import os
import time
import psutil
import pathlib

from guts_utils.guts_resource_manager import resource_manager, resource_set_slurm, resource_set_local

def test_init_manager():
    """Test creating a resource manager."""
    config = {"resource" : {"nwgroups": 2}}
    manager = resource_manager(config)
    assert(manager.get_nwgroups() == 2)

def test_init_manager_unknown_backend():
    """Test creating a resource manager with unknown backend"""
    config = {"resource" : {"nwgroups": 1, "backend" : "WillFail"}}
    manager = resource_manager(config)
    with pytest.raises(Exception):
        _ = manager.get_resources({}, 0)

#def test_get_resources(slurm_not_available):
#    """Test getting the resources from the manager."""
#    config = {"case" : {"name" : "RSTests"},
#              "resource" : {"nwgroups": 1}}
#    manager = resource_manager(config)
#    res_config = {"nworkers": 2, "runtime": 1}
#    resources = manager.get_resources(res_config, 0)
#    assert(resources.get_nworkers() == 2)

def test_oversubscribe_resources():
    """Test getting too many resources from the manager."""
    config = {"resource" : {"nwgroups": 1}}
    manager = resource_manager(config)
    res_config = {"nworkers": psutil.cpu_count() + 1}
    with pytest.raises(Exception):
        resources = manager.get_resources(res_config, 0)

def test_define_local_resource_set(slurm_not_available):
    """Test initializing a local resource set."""
    res_config = {"nworkers": 2, "runtime": "10"}
    res = resource_set_local(15, res_config = res_config)
    assert res.get_nworkers() == 2

def test_serialize_local_resource_set(slurm_not_available):
    """Test initializing a local resource set."""
    res_config = {"nworkers": 2, "runtime": "10"}
    res = resource_set_local(10000, res_config = res_config)
    serialized_res = res.serialize()
    res_restored = resource_set_local(10000, json_str = serialized_res)
    assert res_restored.get_nworkers() == 2

def test_define_slurm_resource_set(slurm_available):
    """Test initializing a slurm resource set."""
    res_config = {"nworkers": 7, "nodes": 1, "runtime": "30"}
    res = resource_set_slurm(12, res_config = res_config)
    assert res.get_nworkers() == 7

def test_serialize_slurm_resource_set(slurm_available):
    """Test serializing a slurm resource set."""
    res_config = {"nworkers": 12, "nodes": 1, "runtime": "1:30"}
    res = resource_set_slurm(12, res_config = res_config)
    serialized_res = res.serialize()
    res_restored = resource_set_slurm(12, json_str = serialized_res)
    assert res_restored.get_nworkers() == 12
    
def test_get_resources_slurm(slurm_available):
    """Test getting a slurm resource set through the manager."""
    config = {"resource" : {"nwgroups": 1, "backend": "slurm"}}
    manager = resource_manager(config)
    res_config = {"nodes": 1, "runtime": "00:10",
                  "extra_directives": {"--exclusive" : None}}
                                       #"--hold": None}}
    resources = manager.get_resources(res_config, 0)
    #resources.release()
