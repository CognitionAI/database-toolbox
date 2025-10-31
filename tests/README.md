# MCP Database Server Tests

Simple test scripts to verify MCP database servers work correctly.

## Quick Start

```bash
# Test PostgreSQL
cd postgres/
./test.sh

# Test MySQL
cd mysql/
./test.sh

# Test Spanner (emulator)
cd spanner/
./test.sh
```

## What It Tests

Each test:
1. Starts a database container using docker-compose
2. Runs the MCP server using the **exact docker command from the main README**
3. Sends a simple `list_tables` request to verify the server works
4. Cleans up all containers

## Structure

```
tests/
├── postgres/
│   ├── docker-compose.yml  # PostgreSQL container only
│   ├── test.sh            # Test runner script
│   └── test_mcp.py        # Python script to test MCP protocol
├── mysql/
│   ├── docker-compose.yml  # MySQL container only
│   ├── test.sh            # Test runner script
│   └── test_mcp.py        # Python script to test MCP protocol
└── spanner/
    ├── docker-compose.yml  # Spanner emulator container
    ├── test.sh            # Test runner script
    └── test_mcp.py        # Python script to test MCP protocol
```

## Requirements

- Docker and docker-compose
- Python 3.x (for test scripts)
- Network access to `us-central1-docker.pkg.dev`

## Adding New Database Tests

1. Create a new directory: `tests/[database]/`
2. Add `docker-compose.yml` with just the database container
3. Copy and adapt `test.sh` and `test_mcp.py` from postgres/mysql
4. Update the docker command to match the one from the main README

## Known Issues

- **SQL Server on ARM64**: SQL Server cannot run on ARM64 systems, even with x86 emulation. Both approaches fail:
  - Azure SQL Edge (ARM-native): Has TLS certificate issues and missing tools
  - SQL Server 2022/2025 (x86 emulated): Crashes on startup due to SQLPAL compatibility issues
  - Additionally, the MCP server expects `MSSQL_*` environment variables (not `SQLSERVER_*` as documented)

- **Google Cloud Services**: Most `--prebuilt` options are for Google Cloud services that require:
  - Valid Google Cloud credentials (service account JSON)
  - Actual cloud resources (projects, instances, databases)
  - Cannot be tested with local emulators except for Spanner
  
- **Firestore Emulator**: The `--prebuilt firestore` option requires Google Cloud credentials even when using the emulator, making local testing impossible

## Complete List of --prebuilt Databases

### Successfully Tested ✅
- **postgres** - Works with local PostgreSQL Docker container
- **mysql** - Works with local MySQL Docker container
- **spanner** - Works with Google's official Spanner emulator

### Failed on ARM64 ❌
- **mssql** (SQL Server) - No ARM64 support, emulation fails

### Require Real Cloud Resources 🚫
- **alloydb** - Google Cloud AlloyDB (no local emulator)
- **bigquery** - BigQuery emulator exists but only for x86_64, MCP requires real credentials
- **cloud-sql-mysql** - Requires actual Google Cloud SQL instance
- **cloud-sql-postgres** - Requires actual Google Cloud SQL instance  
- **cloud-sql-mssql** - Requires actual Google Cloud SQL instance
- **dataplex** - Google Cloud data management service
- **firestore** - Emulator exists but MCP server requires real credentials
- **looker** - Business intelligence platform, not a database