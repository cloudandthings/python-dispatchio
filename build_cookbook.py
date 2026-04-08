#!/usr/bin/env python3
"""
Generate COOKBOOK.md from per-example metadata.

Each subdirectory of examples/ that contains an example.toml is
included as one cookbook entry. Entries are ordered by the optional
`order` field (integer, lower = earlier), then alphabetically by
directory name as a tiebreaker.

Usage:
    python scripts/build_cookbook.py
    python scripts/build_cookbook.py --output docs/cookbook.md
"""

from __future__ import annotations

import argparse
import tomllib
from pathlib import Path

ROOT         = Path(__file__).parent.parent
EXAMPLES_DIR = ROOT / "examples"
DEFAULT_OUT  = ROOT / "COOKBOOK.md"

_FENCE_LANG = {
    ".py":   "python",
    ".toml": "toml",
    ".sh":   "bash",
    ".yaml": "yaml",
    ".yml":  "yaml",
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

    for toml_path in EXAMPLES_DIR.glob("*/example.toml"):
        with toml_path.open("rb") as fh:
            meta = tomllib.load(fh)
        entries.append((meta.get("order", 999), toml_path.parent.name, toml_path.parent, meta))

    entries.sort(key=lambda e: (e[0], e[1]))

    sections = [_render(d, m) for _, _, d, m in entries]
    output.write_text(_HEADER + "\n---\n\n".join(sections))

    print(f"Wrote {output.relative_to(ROOT)} ({len(entries)} example(s))")
    for _, name, _, meta in entries:
        print(f"  [{meta.get('order', '–')}] {name}: {meta['title']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT, metavar="PATH",
                        help=f"destination file (default: {DEFAULT_OUT.name})")
    build(parser.parse_args().output)
