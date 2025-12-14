"""Pytest configuration for pg_probackup tests."""

import os
import sys

import pytest


# Cache for PG version to avoid repeated calls
_pg_version_cache = None


def get_pg_version():
    """Get PostgreSQL version from pg_config.

    Returns version as integer: e.g., 140500 for PG 14.5, 110000 for PG 11.0
    """
    global _pg_version_cache
    if _pg_version_cache is not None:
        return _pg_version_cache

    try:
        import testgres

        version_str = testgres.get_pg_config()["VERSION"].split(" ")[1]
        # Convert version string like "14.5" or "14beta1" to numeric
        # e.g., "14.5" -> 140500, "11.0" -> 110000
        parts = version_str.split(".")
        major = int("".join(c for c in parts[0] if c.isdigit()))
        minor = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        _pg_version_cache = major * 10000 + minor * 100
    except Exception:
        _pg_version_cache = 0
    return _pg_version_cache


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "ptrack: mark test as requiring ptrack extension")
    config.addinivalue_line("markers", "pg_version(min_version): mark test as requiring minimum PG version")


def pytest_pycollect_makeitem(collector, name, obj):
    """Skip ProbackupTest base class to avoid collection warning."""
    if name == "ProbackupTest":
        return []


def pytest_collection_modifyitems(config, items):
    """Add skip markers based on environment and test requirements."""
    # Get environment settings
    ptrack_enabled = os.environ.get("PG_PROBACKUP_PTRACK") == "ON"
    ssh_remote = os.environ.get("PGPROBACKUP_SSH_REMOTE")
    old_binary = os.environ.get("PGPROBACKUPBIN_OLD")
    gdb_enabled = os.environ.get("PGPROBACKUP_GDB") == "ON"
    archive_compression = os.environ.get("ARCHIVE_COMPRESSION") == "ON"
    pg_version = get_pg_version()
    is_windows = sys.platform == "win32"

    # Define skip markers
    skip_ptrack = pytest.mark.skip(reason="ptrack not available (PG_PROBACKUP_PTRACK != ON)")
    skip_remote = pytest.mark.skip(reason="SSH remote not configured (PGPROBACKUP_SSH_REMOTE not set)")
    skip_old_binary = pytest.mark.skip(reason="Old binary not available (PGPROBACKUPBIN_OLD not set)")
    skip_gdb = pytest.mark.skip(reason="GDB not enabled (PGPROBACKUP_GDB != ON)")
    skip_archive_compression = pytest.mark.skip(reason="Archive compression not enabled (ARCHIVE_COMPRESSION != ON)")
    skip_windows = pytest.mark.skip(reason="Test not supported on Windows")
    skip_posix = pytest.mark.skip(reason="Test requires POSIX (not Windows)")

    for item in items:
        nodeid_lower = item.nodeid.lower()

        # Skip ptrack tests if ptrack not enabled
        # Check both nodeid (for ptrack_test.py) and explicit @pytest.mark.ptrack marker
        if not ptrack_enabled:
            if "ptrack" in nodeid_lower:
                item.add_marker(skip_ptrack)
            elif item.get_closest_marker("ptrack"):
                item.add_marker(skip_ptrack)

        # Skip tests requiring specific PG version
        pg_version_marker = item.get_closest_marker("pg_version")
        if pg_version_marker and pg_version:
            min_version = pg_version_marker.args[0] if pg_version_marker.args else 0
            if pg_version < min_version:
                major = min_version // 10000
                item.add_marker(pytest.mark.skip(reason=f"Requires PostgreSQL >= {major}"))

        # Skip compatibility tests if old binary not available
        if not old_binary and "compatibility_test" in nodeid_lower:
            item.add_marker(skip_old_binary)

        # Skip remote tests if SSH not configured
        if not ssh_remote and "remote_test" in nodeid_lower:
            item.add_marker(skip_remote)

        # Skip GDB-dependent tests
        if not gdb_enabled:
            # Tests that require GDB are marked with 'gdb' in name or use @needs_gdb decorator
            if "gdb" in nodeid_lower or hasattr(item, "needs_gdb"):
                item.add_marker(skip_gdb)

        # Skip archive compression tests
        if not archive_compression and "archive_compression" in nodeid_lower:
            item.add_marker(skip_archive_compression)

        # Platform-specific skips
        if is_windows:
            # Skip POSIX-only tests on Windows
            if "posix" in nodeid_lower:
                item.add_marker(skip_posix)
        else:
            # Skip Windows-only tests on POSIX
            if "windows" in nodeid_lower and "skip" in nodeid_lower:
                item.add_marker(skip_windows)
