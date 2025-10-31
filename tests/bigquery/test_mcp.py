#!/usr/bin/env python3
"""
Simple MCP test for BigQuery
Connects to Google BigQuery using credentials from .env (inline JSON),
and tests the MCP server over stdio using the toolbox docker image.
"""

import json
import os
import subprocess
import sys
from pathlib import Path
import tempfile


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


def test_mcp_bigquery() -> bool:
    print("Testing BigQuery MCP server (stdio)...")

    script_dir = Path(__file__).resolve().parent
    env_path = script_dir / ".env"
    env_file = load_env_file(env_path)

    # Required configuration
    bigquery_project = env_file.get("BIGQUERY_PROJECT", "").strip()
    bigquery_dataset = env_file.get("BIGQUERY_DATASET", "").strip()
    creds_json_inline = env_file.get("BIGQUERY_CREDENTIALS_JSON", "").strip()
    creds_file_host = env_file.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    # Validate required vars; require either inline JSON or a host credentials file
    if not bigquery_project:
        print("✗ Missing required variable in .env: BIGQUERY_PROJECT")
        return False
    if not creds_json_inline and not creds_file_host:
        print("✗ Provide either BIGQUERY_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS in .env")
        return False

    # Hardcoded settings (must match README command)
    CONTAINER_CREDS_PATH = "/creds/sa.json"
    DOCKER_IMAGE = "us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest"
    PREBUILT_TARGET = "bigquery"

    temp_creds_file = None
    host_creds_path = None
    try:
        if creds_json_inline:
            with tempfile.NamedTemporaryFile(mode="w", delete=False, prefix="bigquery-creds-", suffix=".json") as tf:
                tf.write(creds_json_inline)
                temp_creds_file = tf.name
                host_creds_path = temp_creds_file
        else:
            # Use provided host credentials file path
            from pathlib import Path as _P
            resolved = str(_P(creds_file_host).expanduser().resolve())
            if not _P(resolved).exists():
                print(f"✗ GOOGLE_APPLICATION_CREDENTIALS not found: {resolved}")
                return False
            host_creds_path = resolved

        # Build docker run command (bind-mount host temp file to container path)
        cmd = [
            "docker", "run", "--rm", "-i",
            "-e", "BIGQUERY_PROJECT",
            "-e", "BIGQUERY_DATASET",
            "-e", "GOOGLE_APPLICATION_CREDENTIALS",
            "-v", f"{host_creds_path}:{CONTAINER_CREDS_PATH}:ro",
            DOCKER_IMAGE,
            "--prebuilt", PREBUILT_TARGET,
            "--stdio",
        ]

        # Prepare environment for the container
        child_env = {
            **os.environ,
            "BIGQUERY_PROJECT": bigquery_project,
            # Dataset is optional
            "BIGQUERY_DATASET": bigquery_dataset,
            "GOOGLE_APPLICATION_CREDENTIALS": CONTAINER_CREDS_PATH,
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

            # Execute a simple query using execute_sql to verify tool calls work
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
    finally:
        if temp_creds_file:
            try:
                Path(temp_creds_file).unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass


if __name__ == "__main__":
    success = test_mcp_bigquery()
    sys.exit(0 if success else 1)


