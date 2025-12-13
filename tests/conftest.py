"""Pytest configuration for pg_probackup tests."""

import os

import pytest


def pytest_pycollect_makeitem(collector, name, obj):
    """Skip ProbackupTest base class to avoid collection warning."""
    if name == "ProbackupTest":
        return []


def pytest_collection_modifyitems(config, items):
    """Skip ptrack tests if PG_PROBACKUP_PTRACK is not ON."""
    if os.environ.get('PG_PROBACKUP_PTRACK') == 'ON':
        return

    skip_ptrack = pytest.mark.skip(reason="ptrack extension not available (PG_PROBACKUP_PTRACK != ON)")
    for item in items:
        if 'ptrack' in item.nodeid.lower():
            item.add_marker(skip_ptrack)
