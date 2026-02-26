# ============================================================
# FILE: scripts/clean_runtime.py
# ROLE: Safe cleanup of runtime artifacts (logs/OUT/__pycache__)
# ============================================================

from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REMOVE_GLOBS = [
    "logs/*.log",
    "logs/*.txt",
    "OUT/*.json",
    "OUT/*.txt",
    "OUT/*.tmp",
]

KEEP_NAMES = {".gitkeep"}


def remove_file(path: Path) -> bool:
    try:
        if path.name in KEEP_NAMES:
            return False
        if path.is_file() or path.is_symlink():
            path.unlink(missing_ok=True)
            return True
    except Exception:
        return False
    return False


def main() -> None:
    removed_files = 0
    removed_dirs = 0

    for pat in REMOVE_GLOBS:
        for p in ROOT.glob(pat):
            if remove_file(p):
                removed_files += 1
                print(f"[DEL] {p.relative_to(ROOT).as_posix()}")

    for p in ROOT.rglob("__pycache__"):
        try:
            shutil.rmtree(p)
            removed_dirs += 1
            print(f"[DEL DIR] {p.relative_to(ROOT).as_posix()}")
        except Exception:
            pass

    print(f"Done. removed_files={removed_files}, removed_dirs={removed_dirs}")


if __name__ == "__main__":
    main()
