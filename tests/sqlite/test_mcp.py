#!/usr/bin/env python3
"""
Simple MCP test for SQLite
Uses a custom tools file and a mounted SQLite db file as described in README.
"""

import json
import os
import subprocess
import sys
from pathlib import Path


def load_env_file(env_path: Path) -> dict:
    values: dict[str, str] = {}
    if not env_path.exists():
        raise FileNotFoundError(f".env file not found at {env_path}")
    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        values[key] = value
    return values


def test_mcp_sqlite() -> bool:
    print("Testing SQLite MCP server (stdio)...")

    script_dir = Path(__file__).resolve().parent
    env_path = script_dir / ".env"
    env_file = load_env_file(env_path)

    # Required configuration
    sqlite_file = env_file.get("SQLITE_FILE", "").strip()
    sqlite_tools_file = env_file.get("SQLITE_TOOLS_FILE", "").strip()

    missing = [name for name, val in [
        ("SQLITE_FILE", sqlite_file),
        ("SQLITE_TOOLS_FILE", sqlite_tools_file),
    ] if not val]
    if missing:
        print(f"✗ Missing required variables in .env: {', '.join(missing)}")
        return False

    DOCKER_IMAGE = "us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest"

    # Build docker run command
    cmd = [
        "docker", "run", "--rm", "-i",
        "-v", f"{sqlite_file}:/data/mydb.sqlite",
        "-e", "SQLITE_FILE=/data/mydb.sqlite",
        "-v", f"{sqlite_tools_file}:/config/sqlite.yaml",
        DOCKER_IMAGE,
        "--tools-file", "/config/sqlite.yaml",
        "--stdio",
    ]

    process = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ,
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

        # Read initialize response
        assert process.stdout is not None
        response_line = process.stdout.readline()
        if not response_line:
            stderr_output = process.stderr.read() if process.stderr else ""
            print("✗ No response from server during initialize")
            if stderr_output:
                print(f"✗ Stderr: {stderr_output}")
            return False
        try:
            response = json.loads(response_line)
            print(
                f"✓ Initialize response: "
                f"{response.get('result', {}).get('serverInfo', {}).get('name', 'Unknown')}"
            )
        except json.JSONDecodeError:
            stderr_output = process.stderr.read() if process.stderr else ""
            print(f"✗ Failed to parse initialize response. Raw: {response_line}")
            if stderr_output:
                print(f"✗ Stderr: {stderr_output}")
            return False

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
        if not response_line:
            print("✗ No response to tools/list")
            return False

        response = json.loads(response_line)
        if "result" not in response:
            print(f"✗ Failed to list tools: {response.get('error', 'Unknown error')}")
            return False

        tools = response.get("result", {}).get("tools", [])
        tool_names = [tool.get("name", "unknown") for tool in tools]
        print("✓ Available tools: " + ", ".join(tool_names))

        # Execute a simple query to validate tool calls
        if "execute_sql" in tool_names:
            execute_sql_request = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "execute_sql",
                    "arguments": {"sql": "SELECT 1 AS one"},
                },
                "id": 3,
            }
            process.stdin.write(json.dumps(execute_sql_request) + "\n")
            process.stdin.flush()

            exec_line = process.stdout.readline()
            if exec_line:
                try:
                    exec_resp = json.loads(exec_line)
                    if "result" in exec_resp:
                        print("✓ execute_sql call successful")
                    else:
                        print(f"✗ execute_sql call failed: {exec_resp.get('error', 'Unknown error')}")
                        return False
                except json.JSONDecodeError:
                    print(f"✗ execute_sql returned non-JSON: {exec_line[:200]}")
                    return False

        return len(tools) > 0

    except Exception as e:
        print(f"✗ Error: {e}")
        stderr_output = process.stderr.read() if process.stderr else ""
        if stderr_output:
            print(f"✗ Stderr: {stderr_output}")
        return False
    finally:
        try:
            process.terminate()
            process.wait(timeout=5)
        except Exception:
            pass


if __name__ == "__main__":
    success = test_mcp_sqlite()
    sys.exit(0 if success else 1)


