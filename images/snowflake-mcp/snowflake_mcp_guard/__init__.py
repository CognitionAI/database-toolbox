"""Pre-execution query policy for the snowflake-labs-mcp server.

`install()` wraps the upstream query execution path so that every statement
submitted to the run_snowflake_query tool is checked by `QueryPolicy` before
it is sent to Snowflake. Run the server through `python -m snowflake_mcp_guard`
to get the policy applied automatically.
"""

from __future__ import annotations

from snowflake_mcp_guard.policy import (
    ALLOWED_FUNCTIONS_ENV,
    UNKNOWN_FUNCTIONS_ENV,
    PolicyViolation,
    QueryPolicy,
)

__all__ = [
    "ALLOWED_FUNCTIONS_ENV",
    "QUERY_TOOL_NAME",
    "UNKNOWN_FUNCTIONS_ENV",
    "PolicyViolation",
    "QueryPolicy",
    "install",
]

QUERY_TOOL_NAME = "run_snowflake_query"
_GUARD_MARKER = "_snowflake_mcp_guard_original"


def install(policy: QueryPolicy | None = None) -> QueryPolicy:
    """Apply the query policy to the installed snowflake-labs-mcp package.

    Both the tool-call middleware (early, descriptive errors) and the function
    that actually executes statements are wrapped, so a statement cannot reach
    the connector without passing the policy. Re-installing replaces the
    policy. Raises RuntimeError when the upstream package does not have the
    expected layout so the server fails closed rather than running unguarded.
    """
    from fastmcp.exceptions import ToolError
    from mcp_server_snowflake import server_utils
    from mcp_server_snowflake.query_manager import tools as query_tools
    from mcp_server_snowflake.utils import SnowflakeException

    policy = policy or QueryPolicy.from_env()

    original_run_query = getattr(query_tools, "run_query", None)
    original_run_query = getattr(original_run_query, _GUARD_MARKER, original_run_query)
    middleware_cls = getattr(server_utils, "CheckQueryType", None)
    original_on_call_tool = getattr(middleware_cls, "on_call_tool", None)
    original_on_call_tool = getattr(
        original_on_call_tool, _GUARD_MARKER, original_on_call_tool
    )
    if not callable(original_run_query) or not callable(original_on_call_tool):
        raise RuntimeError(  # noqa: TRY004
            "Installed snowflake-labs-mcp does not expose the expected query "
            "execution hooks; refusing to start without the query policy."
        )

    def guarded_run_query(statement, snowflake_service, *args, **kwargs):
        try:
            policy.check(statement)
        except PolicyViolation as error:
            raise SnowflakeException(
                tool="query_manager", message=str(error), status_code=400
            ) from None
        return original_run_query(statement, snowflake_service, *args, **kwargs)

    async def guarded_on_call_tool(self, context, call_next):
        if context.message.name.lower() == QUERY_TOOL_NAME:
            arguments = context.message.arguments or {}
            try:
                policy.check(arguments.get("statement"))
            except PolicyViolation as error:
                raise ToolError(f"Statement refused by query policy: {error}") from None
        return await original_on_call_tool(self, context, call_next)

    setattr(guarded_run_query, _GUARD_MARKER, original_run_query)
    setattr(guarded_on_call_tool, _GUARD_MARKER, original_on_call_tool)
    query_tools.run_query = guarded_run_query
    middleware_cls.on_call_tool = guarded_on_call_tool
    return policy
