#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DEST = Path.home() / ".codex" / "skills"
PROJECT_SKILLS = [
    ("dc-code-reviewer", PROJECT_ROOT / ".agents" / "skills" / "code-reviewer"),
    ("dc-write-tests", PROJECT_ROOT / ".agents" / "skills" / "write-tests"),
    ("dc-simplify", PROJECT_ROOT / ".agents" / "skills" / "simplify"),
]


def copy_skill(source_dir: Path, dest_dir: Path) -> None:
    if not source_dir.exists():
        raise SystemExit(f"[ERROR] Missing source skill: {source_dir}")

    temp_dir = dest_dir.with_name(f".{dest_dir.name}.tmp")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    shutil.copytree(source_dir, temp_dir)
    if dest_dir.exists():
        shutil.rmtree(dest_dir)
    temp_dir.replace(dest_dir)


def main() -> None:
    dest_root = DEFAULT_DEST
    dest_root.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Syncing project skills to {dest_root}")
    for skill_name, source_dir in PROJECT_SKILLS:
        dest_dir = dest_root / skill_name
        copy_skill(source_dir, dest_dir)
        print(f"[OK] {skill_name} -> {dest_dir}")


if __name__ == "__main__":
    main()
