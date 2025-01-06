"""Tests for the guts_utils.gut_slurm_utils functions."""
import pytest
from guts_utils.guts_slurm_utils import (
    is_slurm_avail,
    make_job_script_wgroup,
    SlurmCluster,
    time_to_s,
    submit_slurm_job,
    cancel_slurm_job
)

@pytest.mark.skipif(slurm_available=True, reason="SLURM is available.")
def test_donot_have_slurm():
    """Test the slurm avail function, when no slurm"""
    has_slurm = is_slurm_avail()
    assert has_slurm == False

def test_has_slurm(slurm_available):
    """Test the slurm avail function."""
    has_slurm = is_slurm_avail()
    assert has_slurm == True

def test_time_util(slurm_available):
    """Test time format convertion util."""
    time_s = time_to_s("00:00:30")
    assert time_s == 30
    time_s = time_to_s("00:05:12")
    assert time_s == 312
    time_s = time_to_s("05:12")
    assert time_s == 312
    time_s = time_to_s("10:05:56")
    assert time_s == 36356
    with pytest.raises(Exception):
        _ = time_to_s("W-30:10")
    with pytest.raises(Exception):
        _ = time_to_s("1-30:10:30:1")
    with pytest.raises(Exception):
        _ = time_to_s("300:10:30")

def test_init_SlurmCluster(slurm_available):
    """Test initializing a SlurmCluster."""
    slurm_cluster = SlurmCluster()
    assert slurm_cluster.get_node_count() > 0

def test_failcheck_res_config(slurm_available):
    """Test for resource config errors."""
    slurm_cluster = SlurmCluster()
    res_config = {"partition": "UnlikelyName"}          # Wrong partition 
    with pytest.raises(Exception):
        slurm_cluster.process_res_config(res_config)
    res_config = {"nodes": 1}                           # Missing time
    with pytest.raises(Exception):
        slurm_cluster.process_res_config(res_config)
    res_config = {"nodes": 1, "runtime": "01:100:00"}      # Wrong time format
    with pytest.raises(Exception):
        slurm_cluster.process_res_config(res_config)
    res_config = {"nodes": 1000000, "runtime": "01:00:00"} # Too many nodes
    with pytest.raises(Exception):
        slurm_cluster.process_res_config(res_config)

def test_success_check_res_config(slurm_available):
    """Test for resource config succeful processing."""
    slurm_cluster = SlurmCluster()
    res_config = {"nodes": 1, "runtime": "01:00:00"}
    up_config = slurm_cluster.process_res_config(res_config)
    assert res_config["nodes"] == up_config["nodes"]

def test_build_script(slurm_available):
    """Test assembling a slurm script from resource config."""
    slurm_cluster = SlurmCluster()
    res_config = {"nodes": 1, "runtime": "00:05:00"}
    up_config = slurm_cluster.process_res_config(res_config)
    job_script = make_job_script_wgroup(0, up_config)
    assert job_script is not None

def test_submit_job(slurm_available):
    """Test submitting a Slurm job to the queue."""
    slurm_cluster = SlurmCluster()
    res_config = {"nodes": 1, "runtime": "00:01:00",
                  "extra_directives": {"--exclusive" : None,
                                       "--hold": None}}
    up_config = slurm_cluster.process_res_config(res_config)
    job_script = make_job_script_wgroup(0, up_config)
    job_id = submit_slurm_job(0, job_script)
    assert job_id is not None
