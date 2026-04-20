# PR #33 — Fix: Python 3.14 + pandas hang on Windows (sitecustomize.py)

**Branch:** `dev`
**Date:** 2026-04-20

## Root cause

Python 3.14 on Windows has a bug where `platform.machine()` hangs
indefinitely due to a WMI query failure:

```
pandas/compat/_constants.py line 19:
    WASM = platform.machine() in ["wasm32", "wasm64"]
           ↑ calls platform.uname() → win32_ver() → _wmi_query()
             WMI query hangs → pandas import never completes
             → indicator engine never starts
```

Confirmed: `py -c "import pandas"` hangs on Python 3.14.

## Solution: `sitecustomize.py`

Python automatically executes `sitecustomize.py` at interpreter startup,
**before any other imports**. This makes it the ideal place to patch
`platform.machine` before pandas is imported.

```python
# sitecustomize.py (in project root)
import platform, sys

if sys.platform == "win32":
    def _safe_machine() -> str:
        import os, struct
        arch = os.environ.get("PROCESSOR_ARCHITECTURE", "")
        if arch:
            return "AMD64" if "64" in arch else arch
        return "AMD64" if struct.calcsize("P") == 8 else "x86"

    platform.machine = _safe_machine
```

No changes needed to any script — Python loads `sitecustomize.py`
automatically when the interpreter starts, from the directory where
the script is located (project root).

## Why sitecustomize.py?

- Executes before ANY import (including pandas)
- No code changes needed in each script
- Only applies on Windows (guarded by `sys.platform == "win32"`)
- Uses environment variables + struct — never calls WMI
- Zero performance impact on Linux/Mac
