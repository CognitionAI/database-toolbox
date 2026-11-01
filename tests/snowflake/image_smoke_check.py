#!/usr/bin/env python3
"""
Smoke check that runs inside the built mcp-snowflake:local image (no pytest needed).

Verifies that the installed package versions match images/snowflake-mcp/requirements.txt
and that the query policy wrapper refuses unsafe statements before they reach the connector.

Usage (from tests/snowflake):
  docker run --rm --entrypoint python3 \
    -v "$PWD/image_smoke_check.py:/tmp/image_smoke_check.py:ro" \
    -v "$PWD/../../images/snowflake-mcp/requirements.txt:/tmp/requirements.txt:ro" \
    mcp-snowflake:local /tmp/image_smoke_check.py /tmp/requirements.txt
"""

import asyncio
import importlib.metadata
import sys


def check_versions(requirements_path: str) -> None:
    with open(requirements_path) as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            name, expected = line.split("==")
            installed = importlib.metadata.version(name)
            assert installed == expected, (
                f"{name}: installed {installed}, expected {expected}"
            )
            print(f"  {name}=={installed}")


class RecordingService:
    def __init__(self):
        self.executed = []
        self.sql_statement_allowed = ["select", "show", "describe", "use"]
        self.sql_statement_disallowed = ["unknown", "command"]

    def get_query_tag_param(self):
        return None

    def get_connection(self, use_dict_cursor=False, session_parameters=None):
        service = self

        class Cursor:
            def execute(self, statement):
                service.executed.append(statement)

            def fetchall(self):
                return [{"OK": 1}]

        class Context:
            def __enter__(self):
                return (None, Cursor())

            def __exit__(self, *exc):
                return False

        return Context()


async def check_policy() -> None:
    from fastmcp import Client, FastMCP
    from mcp_server_snowflake.query_manager.tools import initialize_query_manager_tool
    from mcp_server_snowflake.server_utils import initialize_middleware
    from snowflake_mcp_guard import QUERY_TOOL_NAME, QueryPolicy, install

    install(QueryPolicy())
    service = RecordingService()
    server = FastMCP("smoke")
    initialize_query_manager_tool(server, service)
    initialize_middleware(server, service)

    refused = [
        "SELECT SYSTEM$ABORT_SESSION(123456789)",
        "SELECT 1; DROP TABLE t",
        "SELECT unknown_fn(1)",
        "DELETE FROM t",
    ]
    allowed = "SELECT CURRENT_TIMESTAMP() AS now, 'test' AS message"

    async with Client(server) as client:
        for statement in refused:
            result = await client.call_tool(
                QUERY_TOOL_NAME, {"statement": statement}, raise_on_error=False
            )
            assert result.is_error, f"expected refusal: {statement}"
            print(f"  refused: {statement}")
        assert service.executed == [], service.executed

        result = await client.call_tool(QUERY_TOOL_NAME, {"statement": allowed})
        assert not result.is_error, result
        assert service.executed == [allowed], service.executed
        print(f"  executed: {allowed}")


def main() -> int:
    requirements_path = sys.argv[1] if len(sys.argv) > 1 else "/app/requirements.txt"
    print("Checking installed versions...")
    check_versions(requirements_path)
    print("Checking query policy wiring...")
    asyncio.run(check_policy())
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
