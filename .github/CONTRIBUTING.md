# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
email, or any other method with the owners of this repository before making a change.

Please note we have a code of conduct, please follow it in all your interactions with the project.

## Development environment

We recommend [VSCode](https://code.visualstudio.com/) and [mise-en-place](https://mise.jdx.dev/).

Once `mise` is installed and activated, it will create and activate a Python virtual environment
and install all required development tools defined in `mise.toml`.

Install Python dependencies:

```sh
uv sync --all-extras
```

### Reducing clutter

To improve focus while developing, you may want to configure VSCode to hide all files beginning
with `.` from the Explorer view. Add `"**/.*"` to the `files.exclude` setting.

## Code quality

This project uses [ruff](https://github.com/astral-sh/ruff) for linting and formatting, managed
via pre-commit. Run all hooks before committing:

```sh
pre-commit run -a
```

## Pull Request Process

1. Update the code, examples, and/or documentation where appropriate.
1. Follow [conventional commits](https://www.conventionalcommits.org/) for your commit messages.
1. Run pre-commit hooks locally: `pre-commit run -a`
1. Run tests locally: `mise test`
1. Create a pull request.
1. Once all CI checks pass, notify a reviewer.

Once all outstanding comments and checklist items have been addressed, your contribution will be
merged. Merged PRs will be included in the next release.

## Testing

```sh
mise test
# or directly:
pytest tests/
```

## Releases

Releases are automated via [release-please](https://github.com/googleapis/release-please) based
on [conventional commits](https://www.conventionalcommits.org/). A release will bump the version
in `pyproject.toml`, generate a changelog entry, and publish the package to PyPI.
