# GitHub Copilot Instructions

## Repository Overview

This is a **Terraform AWS Module Template** repository that serves as a starting point for creating new Terraform AWS modules. It provides a standardized structure, development environment, testing framework, and CI/CD workflows.

**Key Purpose**: Template for creating well-structured, tested, and documented Terraform modules for AWS infrastructure.

## Code Structure

```text
.
├── main.tf                 # Main module resources
├── variables.tf            # Module input variables
├── outputs.tf              # Module outputs
├── locals.tf               # Computed local values
├── terraform.tf            # Terraform and provider version constraints
├── examples/
│   └── basic/             # Example usage of the module
├── tests/
│   ├── basic.tftest.hcl   # Unit tests (mock providers, no AWS needed)
│   └── integration/
│       └── basic.tftest.hcl  # Integration tests (real AWS resources)
├── .github/
│   ├── workflows/         # CI/CD workflows
│   └── CONTRIBUTING.md    # Contribution guidelines
└── .pre-commit-config.yaml # Pre-commit hooks configuration
```

## Development Environment

### Tool Management

- **mise-en-place** (`mise.toml`): Manages all development tools and their versions — all pinned to specific versions for reproducibility.
  - OpenTofu (primary dev tool, `tofu` binary)
  - Terraform (secondary, for CI matrix validation)
  - Python 3.13, terraform-docs, tflint, pre-commit, etc.
  - Run `./dev-bootstrap.sh` to set up tools and pre-commit hooks.

### Environment Setup Options

1. **GitHub Codespaces**: Ready-to-use cloud environment
2. **Dev Containers**: Local containerized environment using Docker
3. **Local with mise**: Install mise and run `./dev-bootstrap.sh`
4. **Manual**: Install tools from `mise.toml` manually (not recommended)

## Terraform and OpenTofu Compatibility

This template targets both **Terraform** (≥ 1.5.0) and **OpenTofu** (≥ 1.6.0).

- Module `required_version`: `>= 1.5.0, < 2.0.0`
- Never use `~> 1.x.y` patch constraints in modules — `~> 1.5.7` resolves to `>= 1.5.7, < 1.6.0`, which excludes OpenTofu.
- The primary dev binary is `tofu`; CI also validates with `terraform`.
- Avoid features exclusive to one tool. Safe shared baseline: TF/OpenTofu 1.6–1.8.
  - Do not use: Terraform 1.10+ exclusive features (ephemeral resources, write-only attributes)
  - Do not use: OpenTofu-exclusive features not yet in Terraform

## Terraform Conventions

### Module Structure

- **Required variables**: Must have `naming_prefix` (string) for resource naming
- **Optional variables**: Common `tags` (map) for resource tagging
- **Locals**: Computed values in `locals.tf`
- **Resources**: Follow AWS best practices with consistent naming
- **Documentation**: Auto-generated using terraform-docs (do not edit manually)

### Naming Patterns

- Use snake_case for variables, resources, and locals
- Prefix resources with module-specific identifier
- Use `local.naming_prefix` for resource naming consistency

### Version Constraints

- Terraform/OpenTofu: `>= 1.5.0, < 2.0.0`
- AWS provider: `>= 5, < 7`
- Pin provider versions appropriately

## Testing

### Framework

Tests use the native `terraform test` / `tofu test` framework (`.tftest.hcl` files), available in both tools from version 1.6 onwards. No Python environment required.

### Unit tests (no AWS credentials)

```sh
tofu test
# or: terraform test
```

Unit tests use `mock_provider` blocks so they can run anywhere without credentials.

### Integration tests (real AWS resources)

```sh
AWS_PROFILE=<your_profile> tofu test -test-directory=tests/integration
```

### What to Test

- Unit tests: all significant input variable combinations; mock providers cover plan-time logic.
- Integration tests: behaviour that needs real AWS (IAM evaluation, data source lookups, etc.).
- Cover bug fixes with a regression test.
- Ensure module behaves as expected before rolling out changes.

## Code Quality & Linting

### Pre-commit Hooks

Run before every commit: `pre-commit run -a`

**Enabled checks:**

- **Terraform**: fmt, validate, tflint (with AWS ruleset), terraform-docs
- **Security**: checkov (Terraform framework), detect-aws-credentials, detect-private-key
- **General**: trailing whitespace, EOF fixer, merge conflicts
- **Python**: ruff, black
- **Shell**: shellcheck, shfmt
- **Spelling**: codespell

### TFLint Configuration

- Configuration in `.tflint.hcl`
- Includes the `tflint-ruleset-aws` plugin for AWS-specific checks
- Enforces naming conventions, documentation, module structure
- Must pass before merging

### Documentation Generation

- **terraform-docs** auto-generates README sections
- Configuration in `.tfdocs-config.yml`
- Generated sections between `<!-- BEGIN_TF_DOCS -->` and `<!-- END_TF_DOCS -->`
- Run via pre-commit hook: `python examples/terraform-docs.py`

## Git & CI/CD

### Branch Protection

- Cannot commit directly to `main`, `master`, or `develop`
- Use feature branches for changes

### Semantic Commits

Follow conventional commit format:

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `test:` - Test additions/changes
- `refactor:` - Code refactoring
- `ci:` - CI/CD changes
- `chore:` - Maintenance tasks (excluded from changelog)

### CI Workflows

1. **pre-commit-and-tests.yml**: Runs pre-commit, unit tests (ubuntu-latest, no AWS), and integration tests (CodeBuild + AWS)
2. **terraform-min-max.yml**: Validates with min/max Terraform versions and the pinned OpenTofu version
3. **pr-title.yml**: Validates PR titles follow conventions
4. **trivy-scan.yml**: Security scanning
5. **release.yml**: Automated releases using release-please

### Dependency Updates

Managed by **Renovate** (`renovate.json`), which covers:

- GitHub Actions (with SHA digest pinning)
- Terraform providers
- Pre-commit hook versions
- `mise.toml` tool versions

### Pull Request Process

1. Update code, examples, and documentation
2. Run `pre-commit run -a` locally
3. Run `tofu test` locally
4. Ensure CI checks pass
5. Use semantic commit messages
6. Request review when ready

## Common Patterns

### Adding New Resources

1. Define resource in `main.tf`
2. Add computed values to `locals.tf`
3. Add variables to `variables.tf` with descriptions and types
4. Add outputs to `outputs.tf` if needed
5. Update example in `examples/basic/`
6. Run `pre-commit run -a` to update docs
7. Add/update unit tests in `tests/basic.tftest.hcl`

### Working with Examples

- Keep examples minimal and focused
- Use `random_integer` for unique naming
- Example must be `tofu validate`-able
- Documentation auto-generated from code

## Best Practices

### Code Style

- Follow existing patterns in the repository
- Use descriptive variable and resource names
- Add inline comments only for complex logic
- Let terraform-docs handle documentation

### Security

- Never commit AWS credentials
- Use IAM roles with OIDC for authentication in CI
- Keep sensitive data in AWS Secrets Manager/Parameter Store
- Security scanning via checkov (pre-commit) and trivy (CI)

### Module Design

- Keep modules focused and single-purpose
- Use sensible defaults for optional variables
- Make required variables truly required
- Provide clear, auto-generated documentation

## Maintenance

### Template Updates

- Periodically review [terraform-aws-template](https://github.com/cloudandthings/terraform-aws-template)
- Sync improvements back to this template
- Dependency updates are automated via Renovate

### Version Management

- Use semantic versioning
- Automated via release-please based on commit messages
- CHANGELOG.md auto-generated

## Common Commands

```sh
# Setup environment
./dev-bootstrap.sh

# Format and validate
tofu fmt -recursive
tofu validate

# Run all pre-commit hooks
pre-commit run -a

# Run unit tests (no AWS needed)
tofu test

# Run integration tests (requires AWS credentials)
AWS_PROFILE=<profile> tofu test -test-directory=tests/integration

# Generate documentation
python examples/terraform-docs.py

# Check TFLint
tflint --config=.tflint.hcl
```

## Important Notes

- This repository was created from [terraform-aws-template](https://github.com/cloudandthings/terraform-aws-template)
- The `null_resource.delete_me` in `main.tf` is a placeholder and should be deleted when implementing actual functionality
- Documentation between `BEGIN_TF_DOCS` and `END_TF_DOCS` is auto-generated — don't edit manually
- Tests run in isolated environments with unique resource naming to prevent conflicts
- All tool versions in `mise.toml` are pinned — update deliberately and verify compatibility
