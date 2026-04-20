# sitecustomize.py
# Python executes this file automatically at interpreter startup,
# before any other imports.
#
# Fix: Python 3.14 on Windows has a bug where platform.machine() hangs
# due to a WMI query failure. pandas 3.x calls platform.machine() at
# import time to check for WASM environment, causing the interpreter
# to freeze indefinitely.
#
# This patch replaces platform.machine() with a direct registry read
# that never hangs. It runs before pandas is imported.

import platform as _platform
import sys as _sys

if _sys.platform == "win32":
    def _safe_machine() -> str:
        """
        Returns the machine architecture without WMI.
        Falls back gracefully on any error.
        """
        # Try environment variable first (always available, no WMI)
        import os
        arch = os.environ.get("PROCESSOR_ARCHITECTURE", "")
        if arch:
            return "AMD64" if "64" in arch else arch

        # Try struct.calcsize — 8 bytes = 64-bit
        import struct
        return "AMD64" if struct.calcsize("P") == 8 else "x86"

    _platform.machine = _safe_machine
