#!/usr/bin/env python3
"""
Example usage of the Redshift MCP Server
Demonstrates how to use the list_tables and execute_sql tools
"""

import json
import subprocess
import sys
from pathlib import Path

# Configuration - update these with your Redshift credentials
REDSHIFT_HOST = "redshift-prod-cluster.cd6o5qin6bij.us-west-2.redshift.amazonaws.com"
REDSHIFT_DATABASE = "redshift"
REDSHIFT_USER = "redshift_readonly"
REDSHIFT_PASSWORD = "your-password"
REDSHIFT_PORT = "5439"
DOCKER_IMAGE = "redshift-toolbox:latest"  # Or use the built image


def send_mcp_request(process, method, params, request_id):
    """Send an MCP JSON-RPC request"""
    request = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": request_id,
    }
    process.stdin.write(json.dumps(request) + "\n")
    process.stdin.flush()


def read_mcp_response(process):
    """Read an MCP JSON-RPC response"""
    line = process.stdout.readline()
    if not line:
        return None
    return json.loads(line)


def list_tables_example(process, table_names=""):
    """Example: List tables"""
    print(f"\n📋 Listing tables (table_names='{table_names}')...")
    send_mcp_request(
        process,
        "tools/call",
        {
            "name": "list_tables",
            "arguments": {"table_names": table_names},
        },
        request_id=100,
    )
    
    response = read_mcp_response(process)
    if response and "result" in response:
        content = response["result"].get("content", [])
        print(f"✓ Found {len(content)} columns")
        
        # Parse and show first few columns
        if content:
            print("\nFirst few columns:")
            for i, item in enumerate(content[:5]):
                if item.get("type") == "text":
                    col_info = json.loads(item["text"])
                    print(f"  - {col_info['schema_name']}.{col_info['table_name']}.{col_info['column_name']} ({col_info['data_type']})")
        return True
    else:
        print(f"✗ Error: {response.get('error', 'Unknown error')}")
        return False


def execute_sql_example(process, sql_query):
    """Example: Execute SQL query"""
    print(f"\n🔍 Executing SQL: {sql_query[:50]}...")
    send_mcp_request(
        process,
        "tools/call",
        {
            "name": "execute_sql",
            "arguments": {"sql": sql_query},
        },
        request_id=200,
    )
    
    response = read_mcp_response(process)
    if response and "result" in response:
        content = response["result"].get("content", [])
        print("✓ Query executed successfully")
        
        # Show results
        for item in content:
            if item.get("type") == "text":
                print(f"  {item['text']}")
        return True
    else:
        print(f"✗ Error: {response.get('error', 'Unknown error')}")
        return False


def main():
    print("🚀 Redshift MCP Server Examples\n")
    
    # Start the Docker container
    cmd = [
        "docker", "run", "--rm", "-i",
        "-e", "POSTGRES_HOST",
        "-e", "POSTGRES_DATABASE",
        "-e", "POSTGRES_USER",
        "-e", "POSTGRES_PASSWORD",
        "-e", "POSTGRES_PORT",
        DOCKER_IMAGE,
        "--stdio",
    ]
    
    env = {
        "POSTGRES_HOST": REDSHIFT_HOST,
        "POSTGRES_DATABASE": REDSHIFT_DATABASE,
        "POSTGRES_USER": REDSHIFT_USER,
        "POSTGRES_PASSWORD": REDSHIFT_PASSWORD,
        "POSTGRES_PORT": REDSHIFT_PORT,
    }
    
    process = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    
    try:
        # Initialize
        print("1. Initializing MCP connection...")
        send_mcp_request(
            process,
            "initialize",
            {
                "protocolVersion": "1.0.0",
                "capabilities": {},
                "clientInfo": {"name": "example-client", "version": "1.0.0"},
            },
            request_id=1,
        )
        
        init_response = read_mcp_response(process)
        if init_response and "result" in init_response:
            print("✓ Connected to Redshift MCP Server")
        else:
            print(f"✗ Failed to initialize: {init_response}")
            return
        
        # Example 1: List all tables
        list_tables_example(process, table_names="")
        
        # Example 2: List specific tables
        list_tables_example(process, table_names="stripe_subscriptions,teams_aggregate")
        
        # Example 3: Simple query
        execute_sql_example(process, "SELECT current_date AS today;")
        
        # Example 4: Count rows
        execute_sql_example(process, "SELECT COUNT(*) as total FROM analytics.stripe_subscriptions;")
        
        # Example 5: Query with filters
        execute_sql_example(
            process,
            "SELECT org_id, subscription_status, plan_slug FROM analytics.teams_aggregate LIMIT 5;"
        )
        
        print("\n✅ Examples completed!")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        stderr_output = process.stderr.read() if process.stderr else ""
        if stderr_output:
            print(f"✗ Stderr: {stderr_output}")
    finally:
        process.terminate()
        process.wait(timeout=5)


if __name__ == "__main__":
    main()

