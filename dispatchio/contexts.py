"""
Context management — named pointers to Dispatchio config files.

A "context" is a name → config file path mapping stored at
~/.dispatchio/contexts.json. This lets users switch between multiple
orchestrators (each with its own config) without retyping paths.

    dispatchio context add daily-etl /srv/pipelines/daily/dispatchio.toml
    dispatchio context use daily-etl
    dispatchio ticks          # inspects daily-etl's tick log

This mirrors the kubectl context / AWS CLI profile pattern. Users with a
single orchestrator never need to touch this — it only matters when managing
two or more orchestrators from one terminal.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


_CONTEXTS_FILE = Path.home() / ".dispatchio" / "contexts.json"


@dataclass
class ContextEntry:
    name: str
    config_path: str
    description: str = ""


class ContextStore:
    """Read/write context registry backed by ~/.dispatchio/contexts.json."""

    def __init__(self, path: Path = _CONTEXTS_FILE) -> None:
        self.path = path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        if not self.path.exists():
            return {"contexts": {}, "current": None}
        with open(self.path) as f:
            return json.load(f)

    def _save(self, data: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(data, f, indent=2)
            f.write("\n")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, entry: ContextEntry) -> None:
        """Register or update a named context."""
        data = self._load()
        data["contexts"][entry.name] = {
            "config_path": entry.config_path,
            "description": entry.description,
        }
        self._save(data)

    def remove(self, name: str) -> None:
        """Remove a context by name. No-op if it doesn't exist."""
        data = self._load()
        data["contexts"].pop(name, None)
        if data.get("current") == name:
            data["current"] = None
        self._save(data)

    def use(self, name: str) -> None:
        """Set the active (default) context."""
        data = self._load()
        if name not in data["contexts"]:
            raise KeyError(f"Unknown context: {name!r}")
        data["current"] = name
        self._save(data)

    def current_name(self) -> str | None:
        """Return the name of the currently active context, or None."""
        return self._load().get("current")

    def get(self, name: str) -> ContextEntry | None:
        """Look up a context by name."""
        data = self._load()
        raw = data["contexts"].get(name)
        if raw is None:
            return None
        return ContextEntry(name=name, **raw)

    def resolve(self, name: str | None) -> ContextEntry | None:
        """Resolve a context by name, falling back to the current context."""
        data = self._load()
        effective = name or data.get("current")
        if effective is None:
            return None
        raw = data["contexts"].get(effective)
        if raw is None:
            return None
        return ContextEntry(name=effective, **raw)

    def list(self) -> list[ContextEntry]:
        """Return all registered contexts."""
        data = self._load()
        return [
            ContextEntry(name=name, **info) for name, info in data["contexts"].items()
        ]
