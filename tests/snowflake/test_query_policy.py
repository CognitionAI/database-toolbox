"""
Regression tests for the Snowflake image query policy (images/snowflake-mcp/snowflake_mcp_guard).

These tests run against the real snowflake-labs-mcp package pinned in
images/snowflake-mcp/requirements.txt and need no Snowflake credentials.
"""

import asyncio
import sys
from pathlib import Path

import pytest

IMAGE_DIR = Path(__file__).resolve().parents[2] / "images" / "snowflake-mcp"
if str(IMAGE_DIR) not in sys.path:
    sys.path.insert(0, str(IMAGE_DIR))

pytest.importorskip("sqlglot", reason="install images/snowflake-mcp/requirements.txt")
pytest.importorskip(
    "mcp_server_snowflake", reason="install images/snowflake-mcp/requirements.txt"
)
yaml = pytest.importorskip("yaml")

from snowflake_mcp_guard import QUERY_TOOL_NAME, PolicyViolation, QueryPolicy, install
from snowflake_mcp_guard.policy import (
    ALLOWED_FUNCTIONS_ENV,
    READ_ONLY_BUILTIN_FUNCTIONS,
    READ_ONLY_SYSTEM_FUNCTIONS,
    UNKNOWN_FUNCTIONS_ENV,
)

SIDE_EFFECT_SELECTS = [
    "SELECT SYSTEM$ABORT_SESSION(123456789)",
    "select system$abort_session(123456789)",
    'SELECT "SYSTEM$ABORT_SESSION"(123456789)',
    "SELECT SYSTEM$CANCEL_ALL_QUERIES(123456789)",
    "SELECT SYSTEM$ABORT_TRANSACTION(1)",
    "SELECT SYSTEM$SEND_EMAIL('int', 'a@example.com', 'subject', 'body')",
    "SELECT SYSTEM$WAIT(60)",
    "SELECT SYSTEM$PIPE_FORCE_RESUME('p')",
    "WITH x AS (SELECT SYSTEM$ABORT_SESSION(1) AS a) SELECT * FROM x",
    "SELECT * FROM t WHERE x = (SELECT SYSTEM$CANCEL_ALL_QUERIES(1))",
    "SELECT 1 UNION ALL SELECT SYSTEM$ABORT_SESSION(1)",
    "SELECT * FROM TABLE(SYSTEM$ABORT_SESSION(1))",
]

MULTI_STATEMENTS = [
    "SELECT 1; DROP TABLE t",
    "SELECT 1; SELECT 2",
    "USE ROLE ACCOUNTADMIN; SELECT 1",
]

NON_READ_STATEMENTS = [
    "DELETE FROM t",
    "INSERT INTO t VALUES (1)",
    "UPDATE t SET a = 1",
    "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
    "CREATE TABLE t (a INT)",
    "DROP TABLE t",
    "TRUNCATE TABLE t",
    "ALTER SESSION SET MULTI_STATEMENT_COUNT = 0",
    "ALTER WAREHOUSE w SUSPEND",
    "GRANT SELECT ON TABLE t TO ROLE r",
    "CALL my_proc()",
    "EXECUTE IMMEDIATE 'DROP TABLE t'",
    "COPY INTO t FROM @stage",
    "PUT file:///tmp/x @stage",
    "BEGIN",
    "COMMIT",
]

ORDINARY_READS = [
    "SELECT CURRENT_TIMESTAMP() AS now, 'test' AS message",
    "SELECT 1;",
    "  SELECT 1  ",
    "select 1 -- ; drop table t",
    "SELECT * FROM my_db.my_schema.my_table WHERE id = 1 ORDER BY id LIMIT 10",
    "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
    "SELECT t.a, u.b FROM t JOIN u ON t.id = u.id LEFT JOIN v ON v.id = t.id",
    "WITH cte AS (SELECT a FROM t) SELECT * FROM cte",
    "SELECT 1 UNION SELECT 2",
    "SELECT 1 INTERSECT SELECT 1",
    "SELECT 1 EXCEPT SELECT 2",
    "SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) FROM t",
    "SELECT * FROM t SAMPLE (10)",
    "SELECT * FROM t AT(OFFSET => -60)",
    "SELECT * FROM t PIVOT(SUM(a) FOR b IN ('x', 'y'))",
    "SELECT * EXCLUDE (a) FROM t",
    "SELECT TOP 10 * FROM t",
    "SELECT a:b.c::int FROM t",
    "SELECT DATEADD(day, 1, CURRENT_DATE()), TO_VARCHAR(1), OBJECT_CONSTRUCT('a', 1), IFF(1 = 1, 1, 0), NVL(a, b), TRY_TO_NUMBER('1') FROM t",
    "SELECT PARSE_JSON('{}'):a::string FROM t, LATERAL FLATTEN(input => PARSE_JSON('[1]')) f",
    "SELECT ARRAY_AGG(x) WITHIN GROUP (ORDER BY x), LISTAGG(x, ',') FROM t",
    "SELECT SEQ4(), UNIFORM(1, 10, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT => 10))",
    "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))",
    "SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())",
    "SELECT GET_DDL('table', 't')",
    "SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()",
    "SELECT SYSTEM$TYPEOF(1), SYSTEM$CLUSTERING_INFORMATION('t')",
    "SELECT ST_DISTANCE(a, b) FROM t",
    "SELECT $1 FROM @mystage",
    "SHOW TABLES",
    "SHOW TABLES IN SCHEMA db.sch",
    "DESCRIBE TABLE t",
    "DESC TABLE t",
    "USE WAREHOUSE compute_wh",
    "USE SCHEMA db.sch",
]

UNKNOWN_FUNCTION_READS = [
    "SELECT db.sch.my_udf(1)",
    "SELECT my_external_fn(z) FROM u",
    "SELECT SNOWFLAKE.CORTEX.COMPLETE('model', 'prompt')",
]


@pytest.fixture
def policy():
    return QueryPolicy()


@pytest.fixture
def service_config():
    with open(IMAGE_DIR / "service_config.yaml") as handle:
        return yaml.safe_load(handle)


# ============================================================================
# UPSTREAM CLASSIFIER BEHAVIOUR (the reason the policy exists)
# ============================================================================


def test_upstream_classifier_accepts_selects_regardless_of_function_effects(
    service_config,
):
    from mcp_server_snowflake.query_manager.tools import validate_sql_type
    from mcp_server_snowflake.utils import unpack_sql_statement_permissions

    allowed, disallowed = unpack_sql_statement_permissions(
        service_config["sql_statement_permissions"]
    )
    statement_type, valid = validate_sql_type(
        SIDE_EFFECT_SELECTS[0], allowed, disallowed
    )
    assert statement_type == "Select"
    assert valid is True


# ============================================================================
# POLICY UNIT TESTS
# ============================================================================


@pytest.mark.parametrize("statement", SIDE_EFFECT_SELECTS)
def test_policy_rejects_side_effecting_system_functions(policy, statement):
    with pytest.raises(PolicyViolation, match="SYSTEM\\$"):
        policy.check(statement)


@pytest.mark.parametrize("statement", MULTI_STATEMENTS)
def test_policy_rejects_multiple_statements(policy, statement):
    with pytest.raises(PolicyViolation, match="Exactly one SQL statement"):
        policy.check(statement)


@pytest.mark.parametrize("statement", NON_READ_STATEMENTS)
def test_policy_rejects_non_read_statements(policy, statement):
    with pytest.raises(PolicyViolation):
        policy.check(statement)


@pytest.mark.parametrize("statement", ["", "   ", None, ";"])
def test_policy_rejects_empty_statements(policy, statement):
    with pytest.raises(PolicyViolation):
        policy.check(statement)


@pytest.mark.parametrize(
    "statement", ["SELECT FROM WHERE", "SELEC 1", "SELECT 1 FROM ((("]
)
def test_policy_rejects_unparseable_statements(policy, statement):
    with pytest.raises(PolicyViolation):
        policy.check(statement)


@pytest.mark.parametrize("statement", ORDINARY_READS)
def test_policy_allows_ordinary_reads(policy, statement):
    policy.check(statement)


@pytest.mark.parametrize("statement", UNKNOWN_FUNCTION_READS)
def test_policy_rejects_unknown_functions_by_default(policy, statement):
    with pytest.raises(PolicyViolation, match="not in the list of permitted functions"):
        policy.check(statement)


def test_policy_unknown_function_error_names_the_override(policy):
    with pytest.raises(PolicyViolation, match=ALLOWED_FUNCTIONS_ENV):
        policy.check("SELECT db.sch.my_udf(1)")


def test_policy_allowlist_accepts_bare_and_qualified_names():
    policy = QueryPolicy.from_env(
        {ALLOWED_FUNCTIONS_ENV: "my_udf, snowflake.cortex.complete"}
    )
    policy.check("SELECT db.sch.my_udf(1)")
    policy.check("SELECT MY_UDF(1)")
    policy.check("SELECT SNOWFLAKE.CORTEX.COMPLETE('model', 'prompt')")
    with pytest.raises(PolicyViolation):
        policy.check("SELECT other.schema.complete('model', 'prompt')")


def test_policy_allowlist_can_opt_into_a_system_function():
    policy = QueryPolicy.from_env({ALLOWED_FUNCTIONS_ENV: "SYSTEM$WAIT"})
    policy.check("SELECT SYSTEM$WAIT(1)")
    with pytest.raises(PolicyViolation):
        policy.check("SELECT SYSTEM$ABORT_SESSION(1)")


def test_policy_allow_unknown_keeps_system_functions_restricted():
    policy = QueryPolicy.from_env({UNKNOWN_FUNCTIONS_ENV: "allow"})
    for statement in UNKNOWN_FUNCTION_READS:
        policy.check(statement)
    for statement in SIDE_EFFECT_SELECTS:
        with pytest.raises(PolicyViolation):
            policy.check(statement)


def test_policy_from_env_defaults_and_validation():
    default = QueryPolicy.from_env({})
    assert default.allow_unknown_functions is False
    assert default.extra_allowed_functions == frozenset()
    assert QueryPolicy.from_env(
        {UNKNOWN_FUNCTIONS_ENV: "ALLOW"}
    ).allow_unknown_functions
    with pytest.raises(ValueError):
        QueryPolicy.from_env({UNKNOWN_FUNCTIONS_ENV: "maybe"})


def test_builtin_allowlists_are_normalized():
    assert all(name == name.upper() for name in READ_ONLY_BUILTIN_FUNCTIONS)
    assert all(name.startswith("SYSTEM$") for name in READ_ONLY_SYSTEM_FUNCTIONS)
    assert not any(name.startswith("SYSTEM$") for name in READ_ONLY_BUILTIN_FUNCTIONS)


def test_service_config_only_enables_read_oriented_statement_types(service_config):
    assert service_config["other_services"] == {
        "object_manager": False,
        "query_manager": True,
        "semantic_manager": False,
    }
    permissions = {}
    for entry in service_config["sql_statement_permissions"]:
        permissions.update(entry)
    enabled = {name for name, value in permissions.items() if value}
    assert enabled == {
        "Select",
        "Union",
        "Intersect",
        "Except",
        "Show",
        "Describe",
        "Explain",
        "Use",
    }
    assert permissions["Unknown"] is False
    assert permissions["Command"] is False


# ============================================================================
# INTEGRATION WITH THE REAL UPSTREAM SERVER
# ============================================================================


class RecordingService:
    """Stand-in for SnowflakeService that records what reaches the connector."""

    def __init__(self):
        self.executed = []
        self.sql_statement_allowed = []
        self.sql_statement_disallowed = []

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


@pytest.fixture
def guarded_server(service_config):
    from fastmcp import FastMCP
    from mcp_server_snowflake.query_manager.tools import initialize_query_manager_tool
    from mcp_server_snowflake.server_utils import initialize_middleware
    from mcp_server_snowflake.utils import unpack_sql_statement_permissions

    install(QueryPolicy())
    service = RecordingService()
    service.sql_statement_allowed, service.sql_statement_disallowed = (
        unpack_sql_statement_permissions(service_config["sql_statement_permissions"])
    )
    server = FastMCP("test")
    initialize_query_manager_tool(server, service)
    initialize_middleware(server, service)
    return server, service


def call_query_tool(server, statement):
    from fastmcp import Client

    async def run():
        async with Client(server) as client:
            return await client.call_tool(
                QUERY_TOOL_NAME, {"statement": statement}, raise_on_error=False
            )

    return asyncio.run(run())


def test_guarded_server_executes_ordinary_reads(guarded_server):
    server, service = guarded_server
    result = call_query_tool(server, ORDINARY_READS[0])
    assert result.is_error is False, result
    assert service.executed == [ORDINARY_READS[0]]


@pytest.mark.parametrize(
    "statement", SIDE_EFFECT_SELECTS + MULTI_STATEMENTS + UNKNOWN_FUNCTION_READS
)
def test_guarded_server_refuses_before_execution(guarded_server, statement):
    server, service = guarded_server
    result = call_query_tool(server, statement)
    assert result.is_error is True, result
    assert "query policy" in result.content[0].text
    assert service.executed == []


@pytest.mark.parametrize("statement", NON_READ_STATEMENTS)
def test_guarded_server_refuses_non_read_statements(guarded_server, statement):
    server, service = guarded_server
    result = call_query_tool(server, statement)
    assert result.is_error is True, result
    assert service.executed == []


def test_run_query_hook_refuses_without_middleware():
    from mcp_server_snowflake.query_manager import tools as query_tools
    from mcp_server_snowflake.utils import SnowflakeException

    install(QueryPolicy())
    service = RecordingService()
    with pytest.raises(SnowflakeException) as excinfo:
        query_tools.run_query(SIDE_EFFECT_SELECTS[0], service)
    assert excinfo.value.status_code == 400
    assert service.executed == []

    assert query_tools.run_query(ORDINARY_READS[0], service) == [{"OK": 1}]
    assert service.executed == [ORDINARY_READS[0]]


def test_install_is_idempotent_and_replaces_policy():
    from mcp_server_snowflake.query_manager import tools as query_tools

    install(QueryPolicy())
    first = query_tools.run_query
    install(QueryPolicy(allow_unknown_functions=True))
    second = query_tools.run_query
    assert first is not second
    original = second._snowflake_mcp_guard_original
    assert getattr(original, "_snowflake_mcp_guard_original", None) is None

    service = RecordingService()
    query_tools.run_query(UNKNOWN_FUNCTION_READS[0], service)
    assert service.executed == [UNKNOWN_FUNCTION_READS[0]]
    install(QueryPolicy())
