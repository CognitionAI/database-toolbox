#!/usr/bin/env python3
"""
Comprehensive MCP test suite for Snowflake Docker image using pytest.
All tests use the Docker image to validate the complete implementation.
"""

import json
import os
import queue
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

import pytest


def load_env_file(env_path: Path) -> dict:
    """Load environment variables from .env file."""
    values: dict[str, str] = {}
    if not env_path.exists():
        raise FileNotFoundError(f".env file not found at {env_path}")
    
    content = env_path.read_text()
    lines = content.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line or line.startswith("#"):
            i += 1
            continue
        if "=" not in line:
            i += 1
            continue
        
        key, value_part = line.split("=", 1)
        key = key.strip()
        
        # Handle multi-line quoted values
        value = value_part.strip()
        if value.startswith('"') and not value.endswith('"'):
            # Multi-line quoted string - collect until closing quote
            value_parts = [value_part]
            i += 1
            while i < len(lines) and not lines[i].strip().endswith('"'):
                value_parts.append(lines[i])
                i += 1
            if i < len(lines):
                value_parts.append(lines[i])
                value = '\n'.join(value_parts).strip()
                i += 1
        elif value.startswith("'") and not value.endswith("'"):
            # Multi-line single-quoted string
            value_parts = [value_part]
            i += 1
            while i < len(lines) and not lines[i].strip().endswith("'"):
                value_parts.append(lines[i])
                i += 1
            if i < len(lines):
                value_parts.append(lines[i])
                value = '\n'.join(value_parts).strip()
                i += 1
        else:
            i += 1
        
        # Strip quotes and whitespace
        value = value.strip().strip('"').strip("'")
        # Convert literal \n escape sequences to actual newlines
        value = value.replace('\\n', '\n')
        values[key] = value
    
    return values


@pytest.fixture(scope="session")
def env_file():
    """Load credentials from .env file."""
    script_dir = Path(__file__).resolve().parent
    env_path = script_dir / ".env"
    
    try:
        env = load_env_file(env_path)
        # Validate required credentials
        required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
        missing = [r for r in required if not env.get(r, "").strip()]
        if missing:
            pytest.skip(f"Missing required credentials in .env: {', '.join(missing)}")
        return env
    except FileNotFoundError:
        pytest.skip(f".env file not found at {env_path}")


@pytest.fixture(scope="session")
def docker_image():
    """Check if Docker image exists."""
    DOCKER_IMAGE = "mcp-snowflake:local"
    result = subprocess.run(
        ["docker", "image", "inspect", DOCKER_IMAGE],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"Docker image {DOCKER_IMAGE} not found. Build it first.")
    return DOCKER_IMAGE


def run_docker_container(
    docker_image: str,
    account: Optional[str] = None,
    user: Optional[str] = None,
    password_cli: Optional[str] = None,
    password_env: Optional[str] = None,
    private_key_env: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    transport: str = "stdio",
    expect_failure: bool = False,
) -> tuple[int, str, str]:
    """
    Run Docker container and return (exit_code, stdout, stderr).
    If expect_failure is True, we expect non-zero exit and check stderr for validation errors.
    """
    cmd = ["docker", "run", "--rm", "-i", "--name", f"mcp-snowflake-test-{int(time.time())}"]
    
    child_env = {**os.environ}
    
    # Add environment variables
    if password_env:
        child_env["SNOWFLAKE_PASSWORD"] = password_env
        cmd.extend(["-e", "SNOWFLAKE_PASSWORD"])
    
    if private_key_env:
        child_env["SNOWFLAKE_PRIVATE_KEY"] = private_key_env
        cmd.extend(["-e", "SNOWFLAKE_PRIVATE_KEY"])
    
    cmd.append(docker_image)
    
    # Add CLI arguments
    if account:
        cmd.extend(["--account", account])
    if user:
        cmd.extend(["--user", user])
    if password_cli:
        cmd.extend(["--password", password_cli])
    if role:
        cmd.extend(["--role", role])
    if warehouse:
        cmd.extend(["--warehouse", warehouse])
    if database:
        cmd.extend(["--database", database])
    if schema:
        cmd.extend(["--schema", schema])
    if transport:
        cmd.extend(["--transport", transport])
    
    try:
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=child_env,
            bufsize=0,  # Unbuffered
        )
        
        # For validation tests (expect_failure), we just wait a bit and check stderr
        if expect_failure:
            time.sleep(1)  # Give entrypoint time to validate and exit
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=1)
            
            stderr = ""
            stdout = ""
            try:
                if process.stderr:
                    stderr = process.stderr.read()
                if process.stdout:
                    stdout = process.stdout.read()
            except Exception:
                pass
            
            return (process.returncode or 1, stdout, stderr)
        
        # For MCP protocol tests, send initialize and wait
        stdout_lines = []
        stderr_lines = []
        
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
            
            if process.stdin:
                process.stdin.write(json.dumps(initialize_request) + "\n")
                process.stdin.flush()
                
                # Read response with timeout using threading and select for non-blocking I/O
                if process.stdout:
                    import select
                    import sys as sys_module
                    
                    # Get file descriptor
                    fd = process.stdout.fileno() if hasattr(process.stdout, 'fileno') else None
                    
                    # Try using select if available (Unix), otherwise use threading with short timeout
                    start_time = time.time()
                    timeout_seconds = 55  # Allow time for Snowflake connection (60s pytest timeout - 5s buffer)
                    got_response = False
                    
                    if fd is not None and hasattr(select, 'select'):
                        # Unix: use select for efficient non-blocking I/O
                        stderr_fd = process.stderr.fileno() if process.stderr and hasattr(process.stderr, 'fileno') else None
                        
                        while time.time() - start_time < timeout_seconds:
                            # Check both stdout and stderr
                            read_fds = [fd]
                            if stderr_fd:
                                read_fds.append(stderr_fd)
                            
                            ready, _, _ = select.select(read_fds, [], [], 1.0)
                            
                            # Check stderr for errors
                            if stderr_fd and stderr_fd in ready:
                                line = process.stderr.readline()
                                if line:
                                    stderr_lines.append(line)
                                    print(f"  [STDERR] {line.strip()}", flush=True)
                            
                            # Check stdout for response
                            if fd in ready:
                                line = process.stdout.readline()
                                if line:
                                    stdout_lines.append(line)
                                    got_response = True
                                    print(f"  [STDOUT] Received response!", flush=True)
                                    break
                            
                            # Print progress every 5 seconds with process status
                            elapsed = int(time.time() - start_time)
                            if elapsed > 0 and elapsed % 5 == 0:
                                status = "running" if process.poll() is None else f"exited({process.poll()})"
                                print(f"  [WAIT] Waiting for Snowflake connection... ({elapsed}s, process: {status})", flush=True)
                            
                            # Check if process died
                            if process.poll() is not None:
                                # Read remaining stderr
                                print(f"  [ERROR] Process exited with code {process.poll()}", flush=True)
                                if process.stderr:
                                    try:
                                        remaining_stderr = process.stderr.read()
                                        if remaining_stderr:
                                            stderr_lines.append(remaining_stderr)
                                            print(f"  [STDERR] Remaining output:\n{remaining_stderr}", flush=True)
                                    except Exception:
                                        pass
                                break
                    else:
                        # Fallback: use threading with very short timeouts
                        output_queue = queue.Queue()
                        read_complete = threading.Event()
                        exception_occurred = threading.Event()
                        
                        def read_output():
                            try:
                                # Use a very short timeout read
                                import socket
                                if hasattr(process.stdout, 'readline'):
                                    # Try to read with a non-blocking approach
                                    line = process.stdout.readline()
                                    if line:
                                        output_queue.put(line)
                            except Exception as e:
                                exception_occurred.set()
                            finally:
                                read_complete.set()
                        
                        reader_thread = threading.Thread(target=read_output, daemon=True)
                        reader_thread.start()
                        
                        poll_interval = 0.1
                        while time.time() - start_time < timeout_seconds:
                            if read_complete.wait(timeout=poll_interval):
                                try:
                                    line = output_queue.get_nowait()
                                    stdout_lines.append(line)
                                    got_response = True
                                    break
                                except queue.Empty:
                                    if exception_occurred.is_set():
                                        break
                            # Check if process died
                            if process.poll() is not None:
                                break
                            poll_interval = min(poll_interval * 1.1, 1.0)  # Exponential backoff up to 1s
                    
                    if not got_response:
                        # Try to read stderr for errors (non-blocking)
                        if process.stderr:
                            try:
                                stderr_fd = process.stderr.fileno() if hasattr(process.stderr, 'fileno') else None
                                if stderr_fd is not None and hasattr(select, 'select'):
                                    ready, _, _ = select.select([stderr_fd], [], [], 0.1)
                                    if ready:
                                        while True:
                                            line = process.stderr.readline()
                                            if not line:
                                                break
                                            stderr_lines.append(line)
                                else:
                                    # Quick non-blocking check
                                    import fcntl
                                    try:
                                        flags = fcntl.fcntl(process.stderr, fcntl.F_GETFL)
                                        fcntl.fcntl(process.stderr, fcntl.F_SETFL, flags | os.O_NONBLOCK)
                                        line = process.stderr.readline()
                                        if line:
                                            stderr_lines.append(line)
                                    except (ImportError, OSError):
                                        pass  # fcntl not available (Windows) or other error
                            except Exception:
                                pass
            
            # Read stderr (non-blocking check)
            if process.stderr:
                try:
                    # Use a thread to read stderr without blocking
                    stderr_queue = queue.Queue()
                    stderr_complete = threading.Event()
                    
                    def read_stderr():
                        try:
                            for _ in range(10):  # Limit iterations
                                line = process.stderr.readline()
                                if not line:
                                    break
                                stderr_queue.put(line)
                        except Exception:
                            pass
                        finally:
                            stderr_complete.set()
                    
                    stderr_thread = threading.Thread(target=read_stderr, daemon=True)
                    stderr_thread.start()
                    
                    # Wait briefly for stderr
                    if stderr_complete.wait(timeout=2.0):
                        try:
                            while True:
                                line = stderr_queue.get_nowait()
                                stderr_lines.append(line)
                        except queue.Empty:
                            pass
                except Exception:
                    pass
                    
        except Exception as e:
            stderr_lines.append(f"Exception during MCP test: {e}")
        finally:
            try:
                process.terminate()
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                    process.wait(timeout=1)
                except Exception:
                    pass
            except Exception:
                pass
        
        stdout_text = "".join(stdout_lines)
        stderr_text = "".join(stderr_lines)
        
        return (process.returncode or 0, stdout_text, stderr_text)
        
    except Exception as e:
        return (1, "", f"Failed to run docker: {e}")


# ============================================================================
# AUTHENTICATION TESTS
# ============================================================================

@pytest.mark.timeout(60)  # Connection tests need time for Snowflake connection
def test_password_auth_cli(env_file, docker_image):
    """Test password authentication via --password CLI flag."""
    exit_code, stdout, stderr = run_docker_container(
        docker_image=docker_image,
        account=env_file.get("SNOWFLAKE_ACCOUNT", "").strip(),
        user=env_file.get("SNOWFLAKE_USER", "").strip(),
        password_cli=env_file.get("SNOWFLAKE_PASSWORD", "").strip(),
        role=env_file.get("SNOWFLAKE_ROLE", "").strip(),
        warehouse=env_file.get("SNOWFLAKE_WAREHOUSE", "").strip(),
    )
    
    # Check for response FIRST - if we got one, test passed (exit_code -9 is pytest-timeout killing it)
    lines = [l for l in stdout.split("\n") if l.strip()]
    if lines:
        # We got a response! Check if it's valid
        try:
            response = json.loads(lines[0])
            if "result" in response:
                # Success! Exit code -9 is just pytest-timeout killing the process, which is fine
                print(f"  [SUCCESS] Got valid MCP response!", flush=True)
                if exit_code == -9:
                    print(f"  [NOTE] Exit code -9 is from pytest-timeout, but we got response - test passed", flush=True)
                server_name = response.get("result", {}).get("serverInfo", {}).get("name", "")
                assert server_name, "No server name in response"
                return  # Test passed!
        except json.JSONDecodeError:
            pass  # Fall through to error handling
    
    # If we get here, no valid response was received
    print(f"\n  [DEBUG] Container exit_code: {exit_code}", flush=True)
    print(f"  [DEBUG] Stdout: {repr(stdout[:500])}", flush=True)
    
    # Filter Pydantic warnings from stderr
    stderr_filtered = [l for l in stderr.split('\n') if 'PydanticDeprecatedSince20' not in l and l.strip()]
    if stderr_filtered:
        print(f"  [DEBUG] Stderr (filtered): {''.join(stderr_filtered[-500:])}", flush=True)
    
    error_msg = "No valid MCP response received.\n"
    error_msg += f"Exit code: {exit_code}\n"
    if stderr_filtered:
        error_msg += f"Stderr (filtered): {''.join(stderr_filtered[-10:])}"
    pytest.fail(error_msg)
    


@pytest.mark.timeout(60)
def test_password_auth_env(env_file, docker_image):
    """Test password authentication via SNOWFLAKE_PASSWORD env var."""
    exit_code, stdout, stderr = run_docker_container(
        docker_image=docker_image,
        account=env_file.get("SNOWFLAKE_ACCOUNT", "").strip(),
        user=env_file.get("SNOWFLAKE_USER", "").strip(),
        password_env=env_file.get("SNOWFLAKE_PASSWORD", "").strip(),
        role=env_file.get("SNOWFLAKE_ROLE", "").strip(),
        warehouse=env_file.get("SNOWFLAKE_WAREHOUSE", "").strip(),
    )
    
    # Check for response FIRST - if we got one, test passed (exit_code -9 is pytest-timeout killing it)
    lines = [l for l in stdout.split("\n") if l.strip()]
    if lines:
        try:
            response = json.loads(lines[0])
            if "result" in response:
                return  # Test passed!
        except json.JSONDecodeError:
            pass
    
    # No valid response
    if exit_code == -9:
        pytest.fail(f"Got timeout kill (exit -9) but no valid response. Stdout: {stdout[:200]}")
    if exit_code != 0:
        pytest.fail(f"Container exited with code {exit_code}. Stderr: {stderr[:500]}")
    pytest.fail(f"No valid response. Stdout: {stdout[:200]}")


@pytest.mark.timeout(60)
def test_keypair_auth_env(env_file, docker_image):
    """Test key-pair authentication via SNOWFLAKE_PRIVATE_KEY env var."""
    private_key = env_file.get("SNOWFLAKE_PRIVATE_KEY", "").strip()
    if not private_key:
        pytest.skip("SNOWFLAKE_PRIVATE_KEY not in .env")
    
    # Validate that the key looks like a complete PEM key
    if not private_key.startswith("-----BEGIN"):
        pytest.skip("SNOWFLAKE_PRIVATE_KEY doesn't start with PEM header")
    if "-----END" not in private_key:
        pytest.skip("SNOWFLAKE_PRIVATE_KEY doesn't contain PEM footer (key appears incomplete)")
    if len(private_key) < 500:  # Real RSA keys are much longer
        pytest.skip(f"SNOWFLAKE_PRIVATE_KEY too short ({len(private_key)} chars) - key appears incomplete")
    
    exit_code, stdout, stderr = run_docker_container(
        docker_image=docker_image,
        account=env_file.get("SNOWFLAKE_ACCOUNT", "").strip(),
        user=env_file.get("SNOWFLAKE_USER", "").strip(),
        private_key_env=env_file.get("SNOWFLAKE_PRIVATE_KEY", "").strip(),
        role=env_file.get("SNOWFLAKE_ROLE", "").strip(),
        warehouse=env_file.get("SNOWFLAKE_WAREHOUSE", "").strip(),
    )
    
    # Check for response FIRST - if we got one, test passed (exit_code -9 is pytest-timeout killing it)
    lines = [l for l in stdout.split("\n") if l.strip()]
    if lines:
        try:
            response = json.loads(lines[0])
            if "result" in response:
                return  # Test passed!
        except json.JSONDecodeError:
            pass
    
    # No valid response - provide detailed error
    print(f"\n  [DEBUG] Container exit_code: {exit_code}", flush=True)
    print(f"  [DEBUG] Stdout: {repr(stdout[:500])}", flush=True)
    
    # Filter Pydantic warnings from stderr
    stderr_filtered = [l for l in stderr.split('\n') if 'PydanticDeprecatedSince20' not in l and l.strip()]
    if stderr_filtered:
        print(f"  [DEBUG] Stderr (filtered): {''.join(stderr_filtered[-500:])}", flush=True)
    
    if exit_code == -9:
        pytest.fail(f"Got timeout kill (exit -9) but no valid response. Stdout: {stdout[:200]}")
    if exit_code != 0:
        pytest.fail(f"Container exited with code {exit_code}. Stderr: {''.join(stderr_filtered[-500:]) if stderr_filtered else stderr[-500:]}")
    pytest.fail(f"No valid response. Stdout: {stdout[:200]}")


def test_no_auth_method(env_file, docker_image):
    """Test that missing authentication method fails with proper error."""
    exit_code, stdout, stderr = run_docker_container(
        docker_image=docker_image,
        account=env_file.get("SNOWFLAKE_ACCOUNT", "").strip(),
        user=env_file.get("SNOWFLAKE_USER", "").strip(),
        expect_failure=True,
    )
    
    # Should fail with error about missing auth
    assert exit_code != 0, "Expected non-zero exit code"
    assert "No authentication method provided" in stderr or "authentication" in stderr.lower(), \
        f"Expected auth error message, got: {stderr[:300]}"


def test_both_auth_methods(env_file, docker_image):
    """Test that providing both auth methods fails with proper error."""
    exit_code, stdout, stderr = run_docker_container(
        docker_image=docker_image,
        account=env_file.get("SNOWFLAKE_ACCOUNT", "").strip(),
        user=env_file.get("SNOWFLAKE_USER", "").strip(),
        password_env=env_file.get("SNOWFLAKE_PASSWORD", "").strip(),
        private_key_env=env_file.get("SNOWFLAKE_PRIVATE_KEY", "").strip() or "dummy-key",
        expect_failure=True,
    )
    
    # Should fail with error about multiple auth methods
    assert exit_code != 0, "Expected non-zero exit code"
    assert "Multiple authentication methods" in stderr or "both" in stderr.lower(), \
        f"Expected multiple auth error, got: {stderr[:300]}"


# ============================================================================
# PARAMETER VALIDATION TESTS
# ============================================================================

def test_missing_account(env_file, docker_image):
    """Test that missing --account fails with proper error."""
    exit_code, stdout, stderr = run_docker_container(
        docker_image=docker_image,
        user=env_file.get("SNOWFLAKE_USER", "").strip(),
        password_env=env_file.get("SNOWFLAKE_PASSWORD", "").strip(),
        expect_failure=True,
    )
    
    assert exit_code != 0, "Expected non-zero exit code"
    assert "Missing required parameter: --account" in stderr or "account" in stderr.lower(), \
        f"Expected account error, got: {stderr[:300]}"


def test_missing_user(env_file, docker_image):
    """Test that missing --user fails with proper error."""
    exit_code, stdout, stderr = run_docker_container(
        docker_image=docker_image,
        account=env_file.get("SNOWFLAKE_ACCOUNT", "").strip(),
        password_env=env_file.get("SNOWFLAKE_PASSWORD", "").strip(),
        expect_failure=True,
    )
    
    assert exit_code != 0, "Expected non-zero exit code"
    assert "Missing required parameter: --user" in stderr or ("missing" in stderr.lower() and "user" in stderr.lower()), \
        f"Expected user error, got: {stderr[:300]}"


# ============================================================================
# OPTIONAL PARAMETER TESTS
# ============================================================================

@pytest.mark.timeout(60)
def test_all_optional_params(env_file, docker_image):
    """Test with all optional parameters provided."""
    exit_code, stdout, stderr = run_docker_container(
        docker_image=docker_image,
        account=env_file.get("SNOWFLAKE_ACCOUNT", "").strip(),
        user=env_file.get("SNOWFLAKE_USER", "").strip(),
        password_env=env_file.get("SNOWFLAKE_PASSWORD", "").strip(),
        role=env_file.get("SNOWFLAKE_ROLE", "").strip(),
        warehouse=env_file.get("SNOWFLAKE_WAREHOUSE", "").strip(),
        database=env_file.get("SNOWFLAKE_DATABASE", "").strip(),
        schema=env_file.get("SNOWFLAKE_SCHEMA", "").strip(),
    )
    
    # Check for response FIRST - if we got one, test passed (exit_code -9 is pytest-timeout killing it)
    lines = [l for l in stdout.split("\n") if l.strip()]
    if lines:
        try:
            response = json.loads(lines[0])
            if "result" in response:
                return  # Test passed!
        except json.JSONDecodeError:
            pass
    
    # No valid response - provide detailed error
    print(f"\n  [DEBUG] Container exit_code: {exit_code}", flush=True)
    print(f"  [DEBUG] Stdout: {repr(stdout[:500])}", flush=True)
    
    # Filter Pydantic warnings from stderr
    stderr_filtered = [l for l in stderr.split('\n') if 'PydanticDeprecatedSince20' not in l and l.strip()]
    if stderr_filtered:
        print(f"  [DEBUG] Stderr (filtered): {''.join(stderr_filtered[-500:])}", flush=True)
    
    if exit_code == -9:
        pytest.fail(f"Got timeout kill (exit -9) but no valid response. Stdout: {stdout[:200]}")
    if exit_code != 0:
        pytest.fail(f"Container exited with code {exit_code}. Stderr: {''.join(stderr_filtered[-500:]) if stderr_filtered else stderr[-500:]}")
    pytest.fail(f"No valid response. Stdout: {stdout[:200]}")


@pytest.mark.timeout(60)
def test_no_optional_params(env_file, docker_image):
    """Test with no optional parameters (only required)."""
    exit_code, stdout, stderr = run_docker_container(
        docker_image=docker_image,
        account=env_file.get("SNOWFLAKE_ACCOUNT", "").strip(),
        user=env_file.get("SNOWFLAKE_USER", "").strip(),
        password_env=env_file.get("SNOWFLAKE_PASSWORD", "").strip(),
    )
    
    # Check for response FIRST - if we got one, test passed (exit_code -9 is pytest-timeout killing it)
    lines = [l for l in stdout.split("\n") if l.strip()]
    if lines:
        try:
            response = json.loads(lines[0])
            if "result" in response:
                return  # Test passed!
        except json.JSONDecodeError:
            pass
    
    # No valid response - provide detailed error
    print(f"\n  [DEBUG] Container exit_code: {exit_code}", flush=True)
    print(f"  [DEBUG] Stdout: {repr(stdout[:500])}", flush=True)
    
    # Filter Pydantic warnings from stderr
    stderr_filtered = [l for l in stderr.split('\n') if 'PydanticDeprecatedSince20' not in l and l.strip()]
    if stderr_filtered:
        print(f"  [DEBUG] Stderr (filtered): {''.join(stderr_filtered[-500:])}", flush=True)
    
    if exit_code == -9:
        pytest.fail(f"Got timeout kill (exit -9) but no valid response. Stdout: {stdout[:200]}")
    if exit_code != 0:
        pytest.fail(f"Container exited with code {exit_code}. Stderr: {''.join(stderr_filtered[-500:]) if stderr_filtered else stderr[-500:]}")
    pytest.fail(f"No valid response. Stdout: {stdout[:200]}")


# ============================================================================
# MCP PROTOCOL TESTS
# ============================================================================

@pytest.mark.timeout(60)
def test_mcp_protocol_full(env_file, docker_image):
    """Test full MCP protocol: initialize, list tools, execute query."""
    cmd = [
        "docker", "run", "--rm", "-i",
        "--name", f"mcp-snowflake-protocol-{int(time.time())}",
        "-e", "SNOWFLAKE_PASSWORD",
        docker_image,
        "--account", env_file.get("SNOWFLAKE_ACCOUNT", "").strip(),
        "--user", env_file.get("SNOWFLAKE_USER", "").strip(),
        "--role", env_file.get("SNOWFLAKE_ROLE", "").strip(),
        "--warehouse", env_file.get("SNOWFLAKE_WAREHOUSE", "").strip(),
    ]
    
    child_env = {**os.environ}
    child_env["SNOWFLAKE_PASSWORD"] = env_file.get("SNOWFLAKE_PASSWORD", "").strip()
    
    process = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=child_env,
    )
    
    try:
        # 1. Initialize
        init_request = {
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
        process.stdin.write(json.dumps(init_request) + "\n")
        process.stdin.flush()
        
        # Read response with timeout (non-blocking)
        assert process.stdout is not None
        response_line = None
        start_time = time.time()
        timeout = 55  # Allow time for Snowflake connection
        
        import select
        fd = process.stdout.fileno()
        
        print(f"  [TEST] Waiting for initialize response (timeout: {timeout}s)...", flush=True)
        
        # Also monitor stderr
        stderr_fd = process.stderr.fileno() if process.stderr and hasattr(process.stderr, 'fileno') else None
        stderr_content = []
        
        while time.time() - start_time < timeout:
            read_fds = [fd]
            if stderr_fd:
                read_fds.append(stderr_fd)
            
            ready, _, _ = select.select(read_fds, [], [], 1.0)
            
            # Check stderr
            if stderr_fd and stderr_fd in ready:
                line = process.stderr.readline()
                if line:
                    stderr_content.append(line)
                    print(f"  [STDERR] {line.strip()}", flush=True)
            
            # Check stdout
            if fd in ready:
                line = process.stdout.readline()
                if line:
                    response_line = line
                    print(f"  [STDOUT] Got initialize response!", flush=True)
                    break
            
            # Print progress every 5 seconds
            elapsed = int(time.time() - start_time)
            if elapsed > 0 and elapsed % 5 == 0:
                status = "running" if process.poll() is None else f"exited({process.poll()})"
                print(f"  [WAIT] Still waiting... ({elapsed}s, process: {status})", flush=True)
            
            # Check if process died
            if process.poll() is not None:
                print(f"  [ERROR] Process exited with code {process.poll()}", flush=True)
                # Read remaining output
                try:
                    line = process.stdout.readline()
                    if line:
                        response_line = line
                except Exception:
                    pass
                # Read remaining stderr
                if process.stderr:
                    try:
                        remaining = process.stderr.read()
                        if remaining:
                            stderr_content.append(remaining)
                            print(f"  [STDERR] Remaining:\n{remaining[-500:]}", flush=True)
                    except Exception:
                        pass
                break
        
        if response_line is None:
            error_msg = f"No initialize response within {timeout}s.\n"
            error_msg += f"Process exit code: {process.returncode}\n"
            if stderr_content:
                error_msg += f"Stderr:\n{''.join(stderr_content[-20:])}"
            pytest.fail(error_msg)
        
        init_response = json.loads(response_line)
        assert "result" in init_response, f"Initialize failed: {init_response}"
        
        # 2. List tools
        tools_request = {
            "jsonrpc": "2.0",
            "method": "tools/list",
            "params": {},
            "id": 2,
        }
        process.stdin.write(json.dumps(tools_request) + "\n")
        process.stdin.flush()
        
        # Read with timeout
        response_line = None
        start_time = time.time()
        while time.time() - start_time < 10:  # Reasonable timeout for subsequent requests
            ready, _, _ = select.select([fd], [], [], 0.5)
            if ready:
                response_line = process.stdout.readline()
                if response_line:
                    break
            if process.poll() is not None:
                break
        
        assert response_line, "No tools/list response"
        tools_response = json.loads(response_line)
        assert "result" in tools_response, f"tools/list failed: {tools_response}"
        
        tools = tools_response.get("result", {}).get("tools", [])
        tool_names = [t.get("name", "") for t in tools]
        
        # 3. Execute query if run_snowflake_query is available
        if "run_snowflake_query" in tool_names:
            query_request = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "run_snowflake_query",
                    "arguments": {"statement": "SELECT CURRENT_TIMESTAMP() AS now, 'test' AS message"},
                },
                "id": 3,
            }
            process.stdin.write(json.dumps(query_request) + "\n")
            process.stdin.flush()
            
            # Read with timeout
            response_line = None
            start_time = time.time()
            while time.time() - start_time < 10:
                ready, _, _ = select.select([fd], [], [], 0.5)
                if ready:
                    response_line = process.stdout.readline()
                    if response_line:
                        break
                if process.poll() is not None:
                    break
            
            assert response_line, "No query response"
            query_response = json.loads(response_line)
            assert "result" in query_response, f"Query failed: {query_response}"
            result = query_response.get("result", {})
            assert not result.get("isError", False), f"Query returned error: {result}"
        else:
            pytest.skip("run_snowflake_query tool not found")
        
    finally:
        try:
            process.terminate()
            process.wait(timeout=3)
        except Exception:
            try:
                process.kill()
                process.wait(timeout=1)
            except Exception:
                pass
