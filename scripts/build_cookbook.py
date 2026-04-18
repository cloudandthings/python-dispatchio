#!/usr/bin/env python3
"""
Generate COOKBOOK.md from examples/ directories.

Each subdirectory of examples/ that contains a run.py is included as one
cookbook entry. If example.toml is present, it is used as metadata; otherwise
the script falls back to inferred defaults so the cookbook stays self-updating.

Entries are ordered by the optional `order` field (integer, lower = earlier),
then alphabetically by directory name as a tiebreaker.

Usage:
    python scripts/build_cookbook.py
    python scripts/build_cookbook.py --output docs/cookbook.md
"""

from __future__ import annotations

import argparse
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent.parent
EXAMPLES_DIR = ROOT / "examples"
DEFAULT_OUT = ROOT / "COOKBOOK.md"

_FENCE_LANG = {
    ".py": "python",
    ".toml": "toml",
    ".sh": "bash",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".json": "json",
}

_HEADER = """\
# Dispatchio Cookbook

A collection of runnable examples, each in its own directory under
[`examples/`](examples/). To run any example:

```
python examples/<name>/run.py
```

> **Production note:** `run.py` uses `simulate()` to drive multiple ticks
> locally. In production, replace it with a single `orchestrator.tick()`
> call triggered by your scheduler (EventBridge, cron, Kubernetes CronJob, …).

---

"""


def _title_from_dir(name: str) -> str:
    return name.replace("_", " ").replace("-", " ").title()


def _default_show_files(example_dir: Path) -> list[str]:
    preferred = ["jobs.py", "my_work.py", "run.py", "dispatchio.toml"]
    show_files = [name for name in preferred if (example_dir / name).exists()]

    if show_files:
        return show_files

    # Fallback for unusual examples: show all top-level Python files except __init__.
    py_files = sorted(
        p.name
        for p in example_dir.glob("*.py")
        if p.name != "__init__.py" and p.is_file()
    )
    return py_files


def _load_meta(example_dir: Path) -> dict[str, Any]:
    meta_path = example_dir / "example.toml"
    if meta_path.exists():
        with meta_path.open("rb") as fh:
            meta: dict[str, Any] = tomllib.load(fh)
    else:
        meta = {}

    title = str(meta.get("title", _title_from_dir(example_dir.name)))
    summary = str(
        meta.get(
            "summary",
            "Auto-discovered example (add example.toml for richer cookbook metadata).",
        )
    )
    tags = [str(tag) for tag in meta.get("tags", [])]
    show_files = [
        str(name) for name in meta.get("show_files", _default_show_files(example_dir))
    ]
    order = int(meta.get("order", 999))

    return {
        "title": title,
        "summary": summary,
        "tags": tags,
        "show_files": show_files,
        "order": order,
    }


def _code_block(path: Path) -> str:
    lang = _FENCE_LANG.get(path.suffix, "")
    source = path.read_text()
    return f"**`{path.name}`**\n\n```{lang}\n{source.rstrip()}\n```\n"


def _render(example_dir: Path, meta: dict) -> str:
    parts: list[str] = []

    parts.append(f"## {meta['title']}\n")

    if summary := meta.get("summary", "").strip():
        parts.append(f"> {summary}\n")

    if tags := meta.get("tags", []):
        parts.append("**Tags:** " + " · ".join(f"`{t}`" for t in tags) + "\n")

    parts.append(f"**Run:** `python examples/{example_dir.name}/run.py`\n")

    for filename in meta.get("show_files", []):
        file_path = example_dir / filename
        if file_path.exists():
            parts.append(_code_block(file_path))
        else:
            parts.append(f"> ⚠️  `{filename}` listed in show_files but not found\n")

    return "\n".join(parts)


def build(output: Path = DEFAULT_OUT) -> None:
    entries: list[tuple[int, str, Path, dict]] = []

    for example_dir in sorted(EXAMPLES_DIR.iterdir()):
        if not example_dir.is_dir() or not (example_dir / "run.py").exists():
            continue
        meta = _load_meta(example_dir)
        entries.append((meta["order"], example_dir.name, example_dir, meta))

    entries.sort(key=lambda e: (e[0], e[1]))

    sections = [_render(d, m) for _, _, d, m in entries]
    output.write_text(_HEADER + "\n---\n\n".join(sections))

    print(f"Wrote {output.relative_to(ROOT)} ({len(entries)} example(s))")
    for _, name, _, meta in entries:
        print(f"  [{meta.get('order', '–')}] {name}: {meta['title']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUT,
        metavar="PATH",
        help=f"destination file (default: {DEFAULT_OUT.name})",
    )
    build(parser.parse_args().output)
