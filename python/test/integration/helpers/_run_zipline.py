"""Subprocess wrapper for zipline CLI — used by submit_run_subprocess().

Runs zipline as an isolated process so Click's CliRunner thread-safety
issues are avoided. Uses os._exit() to force-exit without waiting for
daemon threads (e.g. GCP SDK connection pools) to join.
"""
import os
import sys

os.environ.pop("PYTHONPATH", None)

from ai.chronon.repo.zipline import zipline  # noqa: E402

try:
    zipline()
except SystemExit as e:
    os._exit(e.code if isinstance(e.code, int) else 1)
