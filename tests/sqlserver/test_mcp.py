#!/usr/bin/env python3
"""
Simple MCP test for SQL Server
Tests stdio mode with prebuilt target, using hardcoded local docker-compose values.
"""

import json
import subprocess
import sys
import os


def test_mcp_sqlserver() -> bool:
    print("Testing SQL Server MCP server (stdio)...")

    # Hardcoded safe defaults for local docker-compose SQL Server
    sqlserver_host = "host.docker.internal"
    sqlserver_db = "master"
    sqlserver_user = "sa"
    sqlserver_password = "YourStrong!Passw0rd"
    sqlserver_port = "1433"
    sqlserver_trust_cert = "true"

    missing = [name for name, val in [
        ("SQLSERVER_HOST", sqlserver_host),
        ("SQLSERVER_DATABASE", sqlserver_db),
        ("SQLSERVER_USER", sqlserver_user),
        ("SQLSERVER_PASSWORD", sqlserver_password),
    ] if not val]
    if missing:
        print(f"✗ Missing required variables in .env: {', '.join(missing)}")
        return False

    DOCKER_IMAGE = "us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest"

    cmd = [
        "docker", "run", "--rm", "-i",
        # Prebuilt mssql expects MSSQL_* env vars
        "-e", "MSSQL_HOST",
        "-e", "MSSQL_DATABASE",
        "-e", "MSSQL_USER",
        "-e", "MSSQL_PASSWORD",
        "-e", "MSSQL_PORT",
        # Some builds may also accept trust cert flag
        "-e", "MSSQL_TRUST_CERT",
        DOCKER_IMAGE,
        "--prebuilt", "mssql",
        "--stdio",
    ]

    child_env = {
        **os.environ,
        # Provide both, but prebuilt will read MSSQL_*
        "SQLSERVER_HOST": sqlserver_host,
        "SQLSERVER_DATABASE": sqlserver_db,
        "SQLSERVER_USER": sqlserver_user,
        "SQLSERVER_PASSWORD": sqlserver_password,
        "SQLSERVER_PORT": sqlserver_port,
        "SQLSERVER_TRUST_CERT": sqlserver_trust_cert,
        "MSSQL_HOST": sqlserver_host,
        "MSSQL_DATABASE": sqlserver_db,
        "MSSQL_USER": sqlserver_user,
        "MSSQL_PASSWORD": sqlserver_password,
        "MSSQL_PORT": sqlserver_port,
        "MSSQL_TRUST_CERT": sqlserver_trust_cert,
    }

    process = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=child_env,
    )

    try:
        # Send initialize request
        initialize_request = {
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "protocolVersion": "1.0.0",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "1.0.0"},
            },
            "id": 1,
        }
        assert process.stdin is not None
        process.stdin.write(json.dumps(initialize_request) + "\n")
        process.stdin.flush()

        # Read initialize response (skip any non-JSON log lines)
        assert process.stdout is not None
        while True:
            response_line = process.stdout.readline()
            if not response_line:
                raise RuntimeError("No response from server during initialize")
            response_line_stripped = response_line.strip()
            try:
                response = json.loads(response_line_stripped)
                break
            except json.JSONDecodeError:
                # Not JSON; continue reading
                continue
        print(
            f"✓ Initialize response: "
            f"{response.get('result', {}).get('serverInfo', {}).get('name', 'Unknown')}"
        )

        # List available tools
        list_tools_request = {
            "jsonrpc": "2.0",
            "method": "tools/list",
            "params": {},
            "id": 2,
        }
        process.stdin.write(json.dumps(list_tools_request) + "\n")
        process.stdin.flush()

        response_line = process.stdout.readline()
        if response_line:
            response = json.loads(response_line)
            if "result" in response:
                tools = response.get("result", {}).get("tools", [])
                print("✓ Available tools: " + ", ".join([t.get("name", "unknown") for t in tools]))

        # Try a simple execute_sql if available
        execute_sql_request = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": "execute_sql", "arguments": {"sql": "SELECT 1 as one;"}},
            "id": 3,
        }
        process.stdin.write(json.dumps(execute_sql_request) + "\n")
        process.stdin.flush()

        response_line = process.stdout.readline()
        if response_line:
            response = json.loads(response_line)
            if "result" in response:
                print("✓ execute_sql call successful")
                return True
            else:
                print(f"✗ execute_sql call failed: {response.get('error', 'Unknown error')}")
                return False

        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        try:
            if process and process.stderr:
                err = process.stderr.read()
                if err:
                    print(f"✗ Stderr: {err}")
        except Exception:
            pass
        return False
    finally:
        try:
            process.terminate()
            process.wait(timeout=5)
        except Exception:
            pass


if __name__ == "__main__":
    success = test_mcp_sqlserver()
    sys.exit(0 if success else 1)


