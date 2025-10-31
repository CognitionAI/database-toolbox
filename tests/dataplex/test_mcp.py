#!/usr/bin/env python3
"""
Simple MCP test for Dataplex
Connects to Google Cloud Dataplex using credentials from .env and a credentials JSON,
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


def test_mcp_dataplex() -> bool:
    print("Testing Dataplex MCP server (stdio)...")

    script_dir = Path(__file__).resolve().parent
    env_path = script_dir / ".env"
    env_file = load_env_file(env_path)

    # Required configuration
    dataplex_project = env_file.get("DATAPLEX_PROJECT", "").strip()
    dataplex_location = env_file.get("DATAPLEX_LOCATION", "").strip()
    # Only support inline JSON credentials via env var
    creds_json_inline = env_file.get("DATAPLEX_CREDENTIALS_JSON", "").strip()

    # Validate required vars; require inline JSON
    missing = [name for name, val in [
        ("DATAPLEX_PROJECT", dataplex_project),
        ("DATAPLEX_LOCATION", dataplex_location),
        ("DATAPLEX_CREDENTIALS_JSON", creds_json_inline),
    ] if not val]
    if missing:
        print(f"✗ Missing required variables in .env: {', '.join(missing)}")
        return False

    # Hardcoded settings (must match README command)
    CONTAINER_CREDS_PATH = "/creds/sa.json"
    DOCKER_IMAGE = "us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest"
    PREBUILT_TARGET = "dataplex"

    # Write inline JSON to a host temp file and mount it into the container
    temp_creds_file = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", delete=False, prefix="dataplex-creds-", suffix=".json") as tf:
            tf.write(creds_json_inline)
            temp_creds_file = tf.name

        # Build docker run command (bind-mount host temp file to container path)
        cmd = [
            "docker", "run", "--rm", "-i",
            "-e", "DATAPLEX_PROJECT",
            "-e", "DATAPLEX_LOCATION",
            "-e", "GOOGLE_APPLICATION_CREDENTIALS",
            "-v", f"{temp_creds_file}:{CONTAINER_CREDS_PATH}:ro",
            DOCKER_IMAGE,
            "--prebuilt", PREBUILT_TARGET,
            "--stdio",
        ]

        # Prepare environment for the container
        child_env = {
            **os.environ,
            "DATAPLEX_PROJECT": dataplex_project,
            "DATAPLEX_LOCATION": dataplex_location,
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

            # Try calling dataplex_search_entries if available
            if "dataplex_search_entries" in tool_names:
                search_args = {"query": "*", "page_size": 1}
                search_request = {
                    "jsonrpc": "2.0",
                    "method": "tools/call",
                    "params": {"name": "dataplex_search_entries", "arguments": search_args},
                    "id": 3,
                }
                process.stdin.write(json.dumps(search_request) + "\n")
                process.stdin.flush()

                search_line = process.stdout.readline()
                if search_line:
                    try:
                        search_resp = json.loads(search_line)
                        if "result" in search_resp:
                            print("✓ dataplex_search_entries call successful")
                            # Print a short preview if textual content is returned
                            items = search_resp.get("result", [])
                            if isinstance(items, list):
                                for item in items:
                                    if isinstance(item, dict) and item.get("type") == "text":
                                        text = item.get("text", "").strip()
                                        if text:
                                            print(f"  First result: {text.splitlines()[0][:120]}")
                                            break
                        else:
                            print(f"✗ dataplex_search_entries failed: {search_resp.get('error', 'Unknown error')}")
                    except json.JSONDecodeError:
                        print(f"✗ dataplex_search_entries returned non-JSON: {search_line[:200]}")

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
    success = test_mcp_dataplex()
    sys.exit(0 if success else 1)


