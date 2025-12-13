"""Pytest configuration for pg_probackup tests."""


def pytest_pycollect_makeitem(collector, name, obj):
    """Skip ProbackupTest base class to avoid collection warning."""
    if name == "ProbackupTest":
        return []
