# Contributing to MCP Database Toolbox

Thank you for your interest in contributing to the MCP Database Toolbox! This document provides guidelines and instructions for contributing to the project.

## 📋 Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Features](#suggesting-features)
  - [Adding Database Support](#adding-database-support)
  - [Improving Documentation](#improving-documentation)
  - [Writing Tests](#writing-tests)
- [Development Workflow](#development-workflow)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Release Process](#release-process)
- [Community](#community)

---

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [info@cognition.ai](mailto:info@cognition.ai).

---

## Getting Started

### Prerequisites

Before you begin, ensure you have the following installed:

- **Docker** (20.10 or later) - [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Buildx** - Usually comes with Docker Desktop
- **Git** - [Install Git](https://git-scm.com/downloads)
- **A code editor** - We recommend [VS Code](https://code.visualstudio.com/)

Optional but recommended:

- **Google Cloud CLI** (for GCP database testing) - [Install gcloud](https://cloud.google.com/sdk/docs/install)
- **AWS CLI** (for AWS database testing) - [Install AWS CLI](https://aws.amazon.com/cli/)

### Understanding the Project Structure

```
database-toolbox/
├── .github/              # GitHub Actions workflows and templates
│   └── workflows/        # CI/CD pipeline definitions
├── images/               # Docker image configurations
│   ├── alloydb-postgres-toolbox/
│   ├── bigquery-toolbox/
│   ├── cloud-sql-mysql-toolbox/
│   └── ...              # One directory per database
├── tests/               # Test suites
├── CODE_OF_CONDUCT.md
├── CONTRIBUTING.md      # This file
├── LICENSE
└── README.md
```

Each database has its own subdirectory in `images/` with:

- `Dockerfile` - Image build instructions
- `README.md` - Database-specific documentation
- Configuration files (if needed)

---

## Development Setup

### 1. Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/database-toolbox.git
cd database-toolbox

# Add upstream remote
git remote add upstream https://github.com/cognition-ai/database-toolbox.git
```

### 2. Create a Branch

```bash
# Update your main branch
git checkout main
git pull upstream main

# Create a feature branch
git checkout -b feature/your-feature-name
# or
git checkout -b fix/bug-description
```

### 3. Test Your Environment

```bash
# Pull the base toolbox image
docker pull us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest

# Test with a simple database (PostgreSQL)
docker run --rm -i \
  -e POSTGRES_HOST=localhost \
  -e POSTGRES_DATABASE=testdb \
  -e POSTGRES_USER=test \
  -e POSTGRES_PASSWORD=test \
  us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest \
  --prebuilt postgres \
  --stdio
```

---

## How to Contribute

### Reporting Bugs

Before submitting a bug report:

1. **Check existing issues** - Your bug might already be reported
2. **Test with the latest version** - The bug might already be fixed
3. **Isolate the problem** - Create a minimal reproduction case

When submitting a bug report, include:

- **Clear title** - Descriptive and specific
- **Environment details** - OS, Docker version, database version
- **Steps to reproduce** - Numbered list of exact steps
- **Expected behavior** - What should happen
- **Actual behavior** - What actually happens
- **Logs and errors** - Full error messages and relevant logs
- **Screenshots** - If applicable

**Use the bug report template**: [Create Bug Report](https://github.com/cognition-ai/database-toolbox/issues/new?template=bug_report.md)

### Suggesting Features

Before suggesting a feature:

1. **Check existing feature requests** - It might already be proposed
2. **Consider the scope** - Does it fit the project's goals?
3. **Think about implementation** - Is it technically feasible?

When suggesting a feature, include:

- **Clear title** - What the feature does
- **Problem statement** - What problem does it solve?
- **Proposed solution** - How should it work?
- **Alternatives considered** - Other ways to solve the problem
- **Use cases** - Real-world examples
- **Additional context** - Mockups, diagrams, or examples

**Use the feature request template**: [Request Feature](https://github.com/cognition-ai/database-toolbox/issues/new?template=feature_request.md)

### Adding Database Support

Adding a new database is one of the most valuable contributions! Follow these steps:

#### 1. Research Phase

- Check if the database is supported by [Google Database Toolbox](https://googleapis.github.io/genai-toolbox/)
- Determine if prebuilt support exists or custom YAML is needed
- Identify required environment variables and connection parameters
- Review the database's authentication methods

#### 2. Create Database Directory

```bash
# Create directory structure
mkdir -p images/YOUR-DATABASE-toolbox
cd images/YOUR-DATABASE-toolbox

# Create Dockerfile
touch Dockerfile

# Create README
touch README.md
```

#### 3. Write the Dockerfile

For **prebuilt** databases:

```dockerfile
FROM us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest

# Set metadata
LABEL maintainer="your-email@example.com"
LABEL description="MCP Server for YOUR-DATABASE"

# Set default command
CMD ["--prebuilt", "your-database", "--stdio"]
```

For **custom configuration** databases:

```dockerfile
FROM us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest

# Set metadata
LABEL maintainer="your-email@example.com"
LABEL description="MCP Server for YOUR-DATABASE"

# Copy custom tools configuration
COPY tools.yaml /config/tools.yaml

# Set default command
CMD ["--tools-file", "/config/tools.yaml", "--stdio"]
```

#### 4. Create Custom Configuration (if needed)

Create `tools.yaml` with your database-specific tools:

```yaml
tools:
  - name: list_tables
    description: Lists all tables in the database
    parameters:
      type: object
      properties:
        schema:
          type: string
          description: Schema name (optional)
      required: []
    steps:
      - type: sql
        query: |
          -- Your database-specific query here
          SELECT table_name FROM information_schema.tables
          WHERE table_schema = COALESCE(:schema, 'public');
        params:
          - name: schema
            type: string
            description: Schema name

  - name: execute_sql
    description: Execute arbitrary SQL queries
    parameters:
      type: object
      properties:
        sql:
          type: string
          description: The SQL query to execute
      required:
        - sql
    steps:
      - type: sql
        query: "{{sql}}"
```

#### 5. Document Your Addition

Create a comprehensive `README.md` in your database directory:

```markdown
# YOUR-DATABASE MCP Toolbox

Brief description of the database.

## Prerequisites

- List any specific requirements
- Required accounts or services
- Minimum versions

## Environment Variables

| Variable   | Required | Description          | Default | Example         |
| ---------- | -------- | -------------------- | ------- | --------------- |
| `VAR_NAME` | Yes      | Variable description | -       | `example-value` |

## Quick Start

\`\`\`bash

# Docker command example

docker run --rm -i \
 -e VAR_NAME=value \
 us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest \
 --prebuilt your-database \
 --stdio
\`\`\`

## MCP Client Configuration

\`\`\`json
{
"your-database": {
"command": "docker",
"args": [...],
"env": {...}
}
}
\`\`\`

## Troubleshooting

Common issues and solutions.
```

#### 6. Update Main README

Add your database to the appropriate section in the main `README.md`:

1. Add to the supported databases table
2. Create a detailed configuration section
3. Include Docker command examples
4. Include MCP client configuration
5. Document all environment variables

#### 7. Write Tests

Create tests in the `tests/` directory:

```bash
# Create test file
touch tests/test_your_database.sh
chmod +x tests/test_your_database.sh
```

Example test script:

```bash
#!/bin/bash
set -e

echo "Testing YOUR-DATABASE MCP Toolbox..."

# Set up test environment
export YOUR_DATABASE_HOST="test-host"
export YOUR_DATABASE_USER="test-user"

# Run the container
docker run --rm -i \
  -e YOUR_DATABASE_HOST \
  -e YOUR_DATABASE_USER \
  us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest \
  --prebuilt your-database \
  --stdio &

# Wait and test
sleep 5

echo "✅ Test passed!"
```

#### 8. Build and Test Locally

```bash
# Build the image
docker build -t your-database-toolbox:test ./images/YOUR-DATABASE-toolbox/

# Test the image
docker run --rm -i \
  -e REQUIRED_ENV_VAR=value \
  your-database-toolbox:test
```

#### 9. Submit Pull Request

- Commit your changes with clear messages
- Push to your fork
- Open a pull request with a detailed description
- Reference any related issues

### Improving Documentation

Documentation improvements are always welcome! You can:

- **Fix typos and grammar** - Even small fixes matter
- **Clarify confusing sections** - If something confused you, improve it
- **Add examples** - More examples help users understand
- **Update outdated information** - Keep docs current
- **Improve formatting** - Make docs easier to read
- **Add diagrams** - Visual aids help understanding

Documentation files to improve:

- `README.md` - Main documentation
- `CONTRIBUTING.md` - This file
- Database-specific READMEs in `images/*/README.md`
- Code comments

### Writing Tests

Good tests ensure reliability. You can contribute by:

1. **Adding new test cases** - Cover more scenarios
2. **Improving existing tests** - Make them more robust
3. **Testing edge cases** - What happens when things go wrong?
4. **Integration tests** - Test real database connections
5. **Performance tests** - Ensure images are efficient

Test guidelines:

- Tests should be reproducible
- Use clear, descriptive test names
- Include both positive and negative test cases
- Clean up resources after tests
- Document any prerequisites

---

## Development Workflow

### 1. Keep Your Fork Updated

```bash
# Fetch upstream changes
git fetch upstream

# Update your main branch
git checkout main
git merge upstream/main

# Rebase your feature branch (if needed)
git checkout feature/your-feature
git rebase main
```

### 2. Make Changes

- Write clear, concise code
- Follow the coding standards (see below)
- Add comments for complex logic
- Update documentation as needed

### 3. Test Your Changes

```bash
# Build the image
docker build -t test-image:latest ./images/your-database-toolbox/

# Run manual tests
./tests/test_your_database.sh

# Test with real database (if available)
# ... your test commands ...
```

### 4. Commit Your Changes

Write clear commit messages following the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```bash
# Format: <type>(<scope>): <subject>

git commit -m "feat(postgres): add connection pooling support"
git commit -m "fix(mysql): resolve authentication timeout issue"
git commit -m "docs(readme): update installation instructions"
git commit -m "test(bigquery): add integration tests"
```

**Types:**

- `feat` - New feature
- `fix` - Bug fix
- `docs` - Documentation changes
- `test` - Adding or updating tests
- `refactor` - Code refactoring
- `style` - Code style changes (formatting)
- `chore` - Maintenance tasks
- `ci` - CI/CD changes

---

## Pull Request Process

### Before Submitting

- [ ] Code follows the project's coding standards
- [ ] All tests pass locally
- [ ] Documentation is updated
- [ ] Commit messages are clear and follow conventions
- [ ] Branch is up-to-date with main
- [ ] No merge conflicts

### Submitting the PR

1. **Push your branch** to your fork

   ```bash
   git push origin feature/your-feature-name
   ```

2. **Open a Pull Request** on GitHub

3. **Fill out the PR template** with:

   - Clear description of changes
   - Related issue numbers (e.g., "Fixes #123")
   - Type of change (bug fix, feature, docs, etc.)
   - Testing performed
   - Screenshots (if applicable)

4. **Wait for review** - Maintainers will review your PR

### During Review

- **Respond to feedback** - Address reviewer comments
- **Make requested changes** - Update your PR as needed
- **Keep discussion professional** - Be respectful and constructive
- **Be patient** - Reviews take time

### PR Requirements

Your PR must:

- ✅ Pass all CI/CD checks
- ✅ Have at least one approving review
- ✅ Have no unresolved conversations
- ✅ Be up-to-date with the main branch
- ✅ Follow the project's coding standards
- ✅ Include appropriate tests

### After Merge

- Delete your feature branch (both locally and on GitHub)
- Update your fork's main branch
- Celebrate! 🎉

---

## Coding Standards

### Dockerfile Best Practices

- Use official base images
- Minimize layers
- Use specific tags, not `latest` (except for the toolbox base)
- Add meaningful labels
- Don't run as root (if possible)
- Clean up in the same layer
- Use `.dockerignore` to exclude unnecessary files

Example:

```dockerfile
FROM us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest

LABEL maintainer="team@example.com" \
      version="1.0" \
      description="MCP Server for Database X"

# Copy only what's needed
COPY tools.yaml /config/tools.yaml

# Use specific commands
CMD ["--tools-file", "/config/tools.yaml", "--stdio"]
```

### YAML Configuration

- Use 2 spaces for indentation
- Include comments for complex configurations
- Validate YAML syntax before committing
- Use descriptive names for tools and parameters
- Include detailed descriptions

### Shell Scripts

- Use `#!/bin/bash` shebang
- Use `set -e` to exit on errors
- Quote variables: `"$VAR"` not `$VAR`
- Use `shellcheck` for validation
- Add comments for complex logic

### Documentation

- Use clear, simple language
- Include examples
- Keep line length under 120 characters
- Use proper Markdown formatting
- Check spelling and grammar
- Test all code examples

---

## Testing Guidelines

### Local Testing

Test your changes locally before submitting:

```bash
# 1. Build the image
docker build -t test:local ./images/your-database-toolbox/

# 2. Test basic functionality
docker run --rm -i test:local

# 3. Test with environment variables
docker run --rm -i \
  -e DB_HOST=localhost \
  test:local

# 4. Test edge cases
# ... your specific tests ...
```

### Integration Testing

If you have access to the actual database:

```bash
# Set up real credentials
export DB_HOST="real-host"
export DB_USER="real-user"
export DB_PASSWORD="real-password"

# Run integration test
./tests/integration_test.sh
```

### CI/CD Testing

The CI/CD pipeline automatically:

- Builds all images
- Runs tests
- Checks for vulnerabilities
- Generates SBOM
- Creates provenance attestation

Check the status in the GitHub Actions tab.

---

## Release Process

> **Note**: Only maintainers can perform releases.

### Versioning

We follow [Semantic Versioning](https://semver.org/):

- `MAJOR.MINOR.PATCH` (e.g., `1.2.3`)
- **MAJOR** - Breaking changes
- **MINOR** - New features (backwards compatible)
- **PATCH** - Bug fixes

### Release Steps

1. Update version numbers
2. Update CHANGELOG.md
3. Create and push tag: `git tag -a v1.2.3 -m "Release v1.2.3"`
4. Push tag: `git push origin v1.2.3`
5. GitHub Actions builds and publishes images
6. Create GitHub Release with notes

---

## Community

### Getting Help

- 💬 **GitHub Discussions** - Ask questions, share ideas
- 🐛 **GitHub Issues** - Report bugs, request features
- 📧 **Email** - [info@cognition.ai](mailto:info@cognition.ai)

### Communication Guidelines

- **Be respectful** - Treat everyone with respect
- **Be patient** - Maintainers are volunteers
- **Be constructive** - Provide actionable feedback
- **Be helpful** - Help others when you can
- **Follow the Code of Conduct** - Always

### Recognition

We value all contributions! Contributors are:

- Listed in the README
- Mentioned in release notes
- Given credit in commit history

---

## Questions?

If you have questions not covered in this guide:

1. Check the [README](README.md)
2. Search [existing issues](https://github.com/cognition-ai/database-toolbox/issues)
3. Ask in [GitHub Discussions](https://github.com/cognition-ai/database-toolbox/discussions)
4. Email us at [info@cognition.ai](mailto:info@cognition.ai)

---

## Thank You! 🙏

Your contributions make this project better. Whether you're fixing a typo, adding a feature, or helping other users, every contribution matters.

**Happy Contributing!**

---

<div align="center">

**[⬆ back to top](#contributing-to-mcp-database-toolbox)**

Made with ❤️ by Cognition.AI

</div>
