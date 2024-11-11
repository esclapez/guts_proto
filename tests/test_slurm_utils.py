"""Tests for the guts_utils.gut_slurm_utils functions."""
import pytest
from guts_utils.guts_slurm_utils import is_slurm_avail

def test_has_slurm():
    """Test the slurm avail function."""
    has_slurm = is_slurm_avail()
