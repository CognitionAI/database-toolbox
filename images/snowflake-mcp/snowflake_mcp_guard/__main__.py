"""Start snowflake-labs-mcp with the query policy installed."""

from __future__ import annotations

import sys

from snowflake_mcp_guard import QueryPolicy, install


def main() -> None:
    try:
        policy = QueryPolicy.from_env()
    except ValueError as error:
        sys.stderr.write(f"ERROR: {error}\n")
        sys.exit(2)

    install(policy)
    sys.stderr.write(f"Query policy active: {policy.describe()}\n")

    from mcp_server_snowflake import main as upstream_main

    upstream_main()


if __name__ == "__main__":
    main()
