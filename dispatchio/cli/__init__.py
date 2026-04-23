"""Dispatchio CLI package."""

import sys


def main() -> None:
    try:
        from dispatchio.cli.app import app
    except ImportError as e:
        missing = str(e).split("'")[1] if "'" in str(e) else str(e)
        print(
            f"Error: missing CLI dependency '{missing}'.\n"
            "Install CLI extras with:\n\n"
            "    pip install 'dispatchio[cli]'\n",
            file=sys.stderr,
        )
        sys.exit(1)
    app()


__all__ = ["main"]
