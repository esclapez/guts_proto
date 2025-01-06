"""A set of utilities to get platform information."""
import psutil
import os
import sys
import getpass

def is_mac_os() -> str:
    """Indicates MacOS platform."""
    system = sys.platform.lower()
    return system.startswith("dar")

def is_windows_os() -> str:
    """Indicates Windows platform."""
    system = sys.platform.lower()
    return system.startswith("win")

def is_linux_os() -> str:
    """Indicates Linux platform."""
    system = sys.platform.lower()
    return system.startswith("lin")

def get_cpu_count() -> int:
    """Get the number of CPU on the system."""
    return psutil.cpu_count(logical=False)

def get_username() -> str:
    """Get the hostname."""
    return getpass.getuser()
