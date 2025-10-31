#!/usr/bin/env python3
"""
Simple MCP test for Spanner
Tests the MCP server can connect to Spanner emulator
"""

import json
import subprocess
import sys

def test_mcp_spanner():
    """Test Spanner MCP server connection"""
    print("Testing Spanner MCP server...")
    
    # Environment variables for Spanner
    env = {
        "SPANNER_PROJECT": "test-project",
        "SPANNER_INSTANCE": "test-instance",
        "SPANNER_DATABASE": "test-database",
        "SPANNER_EMULATOR_HOST": "localhost:9010"
    }
    
    # Docker command for Spanner
    cmd = [
        "docker", "run", "--rm", "-i",
        "--network", "host",
        "-e", "SPANNER_PROJECT",
        "-e", "SPANNER_INSTANCE",
        "-e", "SPANNER_DATABASE",
        "-e", "SPANNER_EMULATOR_HOST",
        "us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest",
        "--prebuilt", "spanner",
        "--stdio"
    ]
    
    # Start the process
    process = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**env, **subprocess.os.environ}
    )
    
    try:
        # Send initialize request
        initialize_request = {
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "protocolVersion": "1.0.0",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            },
            "id": 1
        }
        
        process.stdin.write(json.dumps(initialize_request) + "\n")
        process.stdin.flush()
        
        # Read initialize response
        response_line = process.stdout.readline()
        if response_line:
            try:
                response = json.loads(response_line)
                print(f"✓ Initialize response: {response.get('result', {}).get('serverInfo', {}).get('name', 'Unknown')}")
            except json.JSONDecodeError:
                # Check stderr for errors
                stderr_output = process.stderr.read()
                print(f"✗ Failed to parse response. Raw output: {response_line}")
                if stderr_output:
                    print(f"✗ Stderr: {stderr_output}")
                return False
        else:
            print("✗ No response from server")
            return False
        
        # List available tools
        list_tools_request = {
            "jsonrpc": "2.0",
            "method": "tools/list",
            "params": {},
            "id": 2
        }
        
        process.stdin.write(json.dumps(list_tools_request) + "\n")
        process.stdin.flush()
        
        # Read tools list response
        response_line = process.stdout.readline()
        if response_line:
            response = json.loads(response_line)
            if "result" in response:
                tools = response.get("result", {}).get("tools", [])
                print(f"✓ Available tools: {', '.join([tool.get('name', 'unknown') for tool in tools])}")
                
                # For Spanner, we'll just check that we got some tools
                if len(tools) > 0:
                    print("✓ Spanner MCP server is operational")
                    return True
                else:
                    print("✗ No tools available")
                    return False
            else:
                print(f"✗ Failed to list tools: {response.get('error', 'Unknown error')}")
                return False
        
    except Exception as e:
        print(f"✗ Error: {e}")
        stderr_output = process.stderr.read()
        if stderr_output:
            print(f"✗ Stderr: {stderr_output}")
        return False
    finally:
        # Terminate the process
        process.terminate()
        process.wait()

if __name__ == "__main__":
    success = test_mcp_spanner()
    sys.exit(0 if success else 1)
