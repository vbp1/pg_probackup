# pg_probackup Tests

## Requirements

- Python 3.8+
- PostgreSQL installation with development headers
- [uv](https://docs.astral.sh/uv/) package manager (recommended) or pip

## Quick Start

```bash
# Install uv (if not installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Set PostgreSQL config path
export PG_CONFIG=/path/to/pg_config

# Run all tests
uv run pytest -v -n4

# Run specific test file
uv run pytest -v tests/backup_test.py

# Run specific test
uv run pytest -v tests/backup_test.py::BackupTest::test_full_backup
```

## Alternative: Using pip

```bash
pip install testgres pytest pytest-xdist allure-pytest deprecation pexpect
export PG_CONFIG=/path/to/pg_config
python -m pytest -v -n4
```

## Platform Notes

- Tests work on Linux and partially on Windows
- Windows: For tablespace tests, explicitly grant current user full access to tmp_dirs

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PG_CONFIG` | Path to pg_config (required) |
| `PGPROBACKUPBIN` | Custom pg_probackup binary path |
| `PG_PROBACKUP_TEST_BASIC` | Run basic tests only |
| `PG_PROBACKUP_PTRACK` | Enable ptrack tests |
| `PG_PROBACKUP_LONG` | Enable time-consuming tests |
| `PG_PROBACKUP_PARANOIA` | Strict physical validation |
| `PGPROBACKUP_GDB` | Enable GDB debugging tests |
| `ARCHIVE_COMPRESSION` | Check archive compression |
| `PGPROBACKUPBIN_OLD` | Path to previous version binary (compatibility tests) |
| `PGPROBACKUP_SSH_REMOTE` | Enable remote backup tests via SSH |

## Advanced Configuration

### Physical correctness validation
```bash
# Apply patch to disable HINT BITS:
# https://gist.github.com/gsmol/2bb34fd3ba31984369a72cc1c27a36b6
export PG_PROBACKUP_PARANOIA=ON
```

### GDB debugging tests
Requires pg_probackup compiled without optimizations:
```bash
CFLAGS="-O0" ./configure --prefix=/path/to/prefix \
    --enable-debug --enable-cassert --enable-depend \
    --enable-tap-tests --enable-nls

sudo echo 0 > /proc/sys/kernel/yama/ptrace_scope
export PGPROBACKUP_GDB=ON
```

## Code Quality

```bash
# Run linter
uv run ruff check tests/

# Auto-fix issues
uv run ruff check tests/ --fix
```

# Troubleshooting FAQ

## Python tests failure

### 1. Could not open extension "..."
```
testgres.exceptions.QueryException ERROR:  could not open extension control file "<postgres_build_dir>/share/extension/amcheck.control": No such file or directory
```

#### Solution:

You have no `<postgres_src_root>/contrib/...` extension installed, please do

```bash
cd <postgres_src_root>
make install-world
```
