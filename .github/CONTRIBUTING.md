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

Releases are fully automated using [release-please](https://github.com/googleapis/release-please)
and [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC — no API token needed).

### How it works

1. **Commit to `main`** using [conventional commits](https://www.conventionalcommits.org/)
   (e.g. `feat:`, `fix:`, `chore:`). release-please opens or updates a **Release PR** that:
   - bumps the version in `pyproject.toml` according to semver
   - generates a `CHANGELOG.md` entry

2. **Merge the Release PR** when you are ready to ship. release-please automatically:
   - creates a Git tag (e.g. `v1.2.0`)
   - publishes a GitHub release with the changelog

3. **The GitHub release triggers `publish.yml`**, which:
   - builds the package with `uv build`
   - publishes to [TestPyPI](https://test.pypi.org/project/dispatchio/)
   - on success, publishes to [PyPI](https://pypi.org/project/dispatchio/)

### Commit types and version bumps

| Commit prefix | Version bump |
|---|---|
| `fix:` | patch (`1.0.x`) |
| `feat:` | minor (`1.x.0`) |
| `feat!:` or `BREAKING CHANGE:` footer | major (`x.0.0`) |
| `chore:`, `docs:`, `refactor:`, etc. | none |

### Prerequisites (one-time setup)

Trusted Publishers must be configured on both PyPI and TestPyPI for this repository before the
publish step will succeed. See the
[PyPI documentation](https://docs.pypi.org/trusted-publishers/adding-a-trusted-publisher/) for
instructions. The workflow name is `publish.yml` and the environment names are `pypi` and
`testpypi`.
