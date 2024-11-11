import pytest
from guts_utils.guts_slurm_utils import is_slurm_avail

@pytest.fixture(scope="session")
def slurm_available():
    """Fixture to check SLURM availability."""
    if not is_slurm_avail():
        pytest.skip("SLURM is not available on this system. Skipping SLURM-dependent tests.")
