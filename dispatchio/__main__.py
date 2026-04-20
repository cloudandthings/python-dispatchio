if __name__ == "__main__":
    try:
        from dispatchio.cli.main import app
        app()
    except ImportError as e:
        raise ImportError(
            "The Dispatchio CLI requires the 'cli' extra. "
            "Install it with: pip install dispatchio[cli]"
        ) from e
