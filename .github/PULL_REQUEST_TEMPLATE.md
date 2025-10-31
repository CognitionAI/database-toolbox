<!--
Thank you for contributing to the MCP Database Toolbox! 

Please fill out this template to help us review your changes efficiently.
Delete any sections that don't apply to your PR.
-->

## 📝 Change Type

<!-- Check all that apply -->

- [ ] 🆕 New database toolbox image
- [ ] 🔧 Bug fix (non-breaking change that fixes an issue)
- [ ] ✨ Feature enhancement (new capability to existing toolbox)
- [ ] 📚 Documentation update
- [ ] 🔄 CI/CD or infrastructure change
- [ ] 🧪 Tests or examples
- [ ] ⚠️ Breaking change (requires version bump)

## 🎯 Description

<!-- Provide a clear and concise description of what this PR does -->


## 🗄️ Database(s) Affected

<!-- List the database toolboxes affected by this change -->

- [ ] AlloyDB for PostgreSQL
- [ ] BigQuery
- [ ] Bigtable
- [ ] Cloud SQL (MySQL)
- [ ] Cloud SQL (PostgreSQL)
- [ ] Cloud SQL (SQL Server)
- [ ] Couchbase
- [ ] Dataplex
- [ ] Dgraph
- [ ] Firestore
- [ ] Looker
- [ ] MongoDB
- [ ] MySQL
- [ ] Neo4j
- [ ] PostgreSQL
- [ ] Redis
- [ ] Redshift
- [ ] Spanner
- [ ] SQL Server
- [ ] SQLite
- [ ] TiDB
- [ ] Valkey
- [ ] Other: _______

## ✅ Testing Checklist

<!-- Check all completed items -->

- [ ] Built Docker image(s) locally
- [ ] Tested image with `--stdio` flag (MCP mode)
- [ ] Verified environment variables work correctly
- [ ] Tested connection to actual database (if applicable)
- [ ] Ran existing test suite
- [ ] Added new tests for new functionality
- [ ] Updated examples in README
- [ ] Tested on both `amd64` and `arm64` architectures (if possible)

## 🔐 Security Considerations

<!-- Address any security implications -->

- [ ] No hardcoded credentials or secrets
- [ ] Follows environment-based credential management
- [ ] No sensitive data in logs or output
- [ ] Dependencies updated to non-vulnerable versions
- [ ] N/A - No security implications

## 📦 Docker Image Changes

<!-- If this PR modifies Docker images, provide details -->

### Base Image
<!-- Did you change the base image or layers? -->


### New Dependencies
<!-- List any new packages, libraries, or tools added -->


## 🔗 Related Issues

<!-- Link related issues using keywords: closes, fixes, resolves -->

- Closes #
- Related to #

## ⚠️ Breaking Changes

<!-- If this is a breaking change, describe the impact and migration path -->


## 📋 Reviewer Notes

<!-- Anything specific you want reviewers to focus on? -->


## ✍️ Author Checklist

<!-- Final checklist before requesting review -->

- [ ] Code follows project style guidelines
- [ ] Self-reviewed the code changes
- [ ] Commented complex or non-obvious code
- [ ] Updated relevant documentation
- [ ] No warnings or errors in CI pipeline
- [ ] Commit messages are clear and descriptive