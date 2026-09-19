#!/usr/bin/env python3
"""Compatibility entrypoint; all validation lives in scripts/check-docs.py."""
import runpy
from pathlib import Path
runpy.run_path(str(Path(__file__).resolve().parents[2] / 'scripts/check-docs.py'), run_name='__main__')
