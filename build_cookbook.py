#!/usr/bin/env python3
"""Backward-compatible wrapper for cookbook generation.

Use:
  python scripts/build_cookbook.py

This wrapper delegates to the canonical implementation to avoid duplication.
"""

from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    target = Path(__file__).parent / "scripts" / "build_cookbook.py"
    runpy.run_path(str(target), run_name="__main__")
