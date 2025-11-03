#!/usr/bin/env python3
"""
Simple MCP test for Amazon Redshift
Tests the custom Redshift Docker image built from images/redshift/Dockerfile
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


def build_redshift_image() -> str:
    """Build the custom Redshift Docker image and return the image tag"""
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent
    redshift_image_dir = repo_root / "images" / "redshift"
    
    image_tag = "redshift-toolbox:test"
    
    print(f"Building Redshift Docker image from {redshift_image_dir}...")
    build_cmd = [
        "docker", "build",
        "-t", image_tag,
        str(redshift_image_dir),
    ]
    
    result = subprocess.run(build_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"✗ Failed to build Docker image: {result.stderr}")
        sys.exit(1)
    
    print(f"✓ Built Docker image: {image_tag}")
    return image_tag


def test_mcp_redshift() -> bool:
    print("Testing Redshift MCP server (stdio)...")

    script_dir = Path(__file__).resolve().parent
    env_path = script_dir / ".env"
    env_file = load_env_file(env_path)

    # Allow POSTGRES_* aliases for local testing
    redshift_host = (env_file.get("REDSHIFT_HOST") or env_file.get("POSTGRES_HOST") or "").strip()
    redshift_database = (env_file.get("REDSHIFT_DATABASE") or env_file.get("POSTGRES_DATABASE") or "").strip()
    redshift_user = (env_file.get("REDSHIFT_USER") or env_file.get("POSTGRES_USER") or "").strip()
    redshift_password = (env_file.get("REDSHIFT_PASSWORD") or env_file.get("POSTGRES_PASSWORD") or "").strip()
    redshift_port = (env_file.get("REDSHIFT_PORT") or env_file.get("POSTGRES_PORT") or "5439").strip() or "5439"

    missing = [name for name, val in [
        ("REDSHIFT_HOST", redshift_host),
        ("REDSHIFT_DATABASE", redshift_database),
        ("REDSHIFT_USER", redshift_user),
        ("REDSHIFT_PASSWORD", redshift_password),
    ] if not val]
    if missing:
        print(f"✗ Missing required variables in .env: {', '.join(missing)}")
        return False

    # Build the custom Redshift Docker image
    docker_image = build_redshift_image()

    # Build docker run command
    # The custom Redshift image has the tools file baked in and uses a custom entrypoint
    # that automatically passes --tools-file /config/redshift.yaml, so we just need --stdio
    cmd = [
        "docker", "run", "--rm", "-i",
        "-e", "POSTGRES_HOST",
        "-e", "POSTGRES_DATABASE",
        "-e", "POSTGRES_USER",
        "-e", "POSTGRES_PASSWORD",
        "-e", "POSTGRES_PORT",
        docker_image,
        "--stdio",
    ]

    child_env = {
        **os.environ,
        "POSTGRES_HOST": redshift_host,
        "POSTGRES_DATABASE": redshift_database,
        "POSTGRES_USER": redshift_user,
        "POSTGRES_PASSWORD": redshift_password,
        "POSTGRES_PORT": redshift_port,
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

        # Verify both required tools are present
        if "list_tables" not in tool_names:
            print("✗ Required tool 'list_tables' not found")
            return False
        if "execute_sql" not in tool_names:
            print("✗ Required tool 'execute_sql' not found")
            return False

        # Test list_tables tool
        if "list_tables" in tool_names:
            list_tables_request = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "list_tables",
                    "arguments": {"table_names": ""},
                },
                "id": 3,
            }
            process.stdin.write(json.dumps(list_tables_request) + "\n")
            process.stdin.flush()
            list_tables_line = process.stdout.readline()
            if list_tables_line:
                try:
                    list_tables_resp = json.loads(list_tables_line)
                    if "result" in list_tables_resp:
                        content_count = len(list_tables_resp.get("result", {}).get("content", []))
                        print(f"✓ list_tables call successful ({content_count} columns returned)")
                    else:
                        print(f"✗ list_tables returned error: {list_tables_resp.get('error', 'Unknown error')}")
                        return False
                except json.JSONDecodeError:
                    print(f"✗ list_tables returned non-JSON: {list_tables_line[:200]}")
                    return False
            else:
                print("✗ No response from list_tables")
                return False

        # Test execute_sql tool
        if "execute_sql" in tool_names:
            execute_sql_request = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "execute_sql",
                    "arguments": {"sql": "SELECT current_date;"},
                },
                "id": 4,
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
                        print(f"✗ execute_sql returned error: {exec_resp.get('error', 'Unknown error')}")
                        return False
                except json.JSONDecodeError:
                    print(f"✗ execute_sql returned non-JSON: {exec_line[:200]}")
                    return False
            else:
                print("✗ No response from execute_sql")
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
    success = test_mcp_redshift()
    sys.exit(0 if success else 1)


