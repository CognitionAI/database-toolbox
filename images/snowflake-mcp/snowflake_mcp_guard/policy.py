"""Statement-level policy for the Snowflake MCP query tool.

The policy runs before a statement reaches the Snowflake connector and refuses
anything that is not a single, parseable, read-oriented statement whose
function calls are all known to be free of side effects.

Snowflake role privileges remain the authorization boundary. Run the server
with a dedicated least-privilege role; this policy is defense in depth.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

DIALECT = "snowflake"

ALLOWED_FUNCTIONS_ENV = "SNOWFLAKE_MCP_ALLOWED_FUNCTIONS"
UNKNOWN_FUNCTIONS_ENV = "SNOWFLAKE_MCP_UNKNOWN_FUNCTIONS"

SYSTEM_FUNCTION_PREFIX = "SYSTEM$"

# Root expression types that may be executed. Everything else is refused
# regardless of the upstream statement-permission configuration.
READ_ONLY_ROOTS = frozenset(
    {"Select", "Union", "Intersect", "Except", "Show", "Describe", "Use"}
)

# SYSTEM$ functions that only report information. Every other SYSTEM$ function
# is refused unless it is listed in SNOWFLAKE_MCP_ALLOWED_FUNCTIONS.
READ_ONLY_SYSTEM_FUNCTIONS = frozenset(
    {
        "SYSTEM$ALLOWLIST",
        "SYSTEM$ALLOWLIST_PRIVATELINK",
        "SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS",
        "SYSTEM$CLIENT_VERSION_INFO",
        "SYSTEM$CLUSTERING_DEPTH",
        "SYSTEM$CLUSTERING_INFORMATION",
        "SYSTEM$CLUSTERING_RATIO",
        "SYSTEM$CURRENT_USER_TASK_NAME",
        "SYSTEM$DATABASE_REFRESH_HISTORY",
        "SYSTEM$DATABASE_REFRESH_PROGRESS",
        "SYSTEM$DATABASE_REFRESH_PROGRESS_BY_JOB",
        "SYSTEM$ESTIMATE_AUTOMATIC_CLUSTERING_COSTS",
        "SYSTEM$ESTIMATE_QUERY_ACCELERATION",
        "SYSTEM$ESTIMATE_SEARCH_OPTIMIZATION_COSTS",
        "SYSTEM$EXPLAIN_JSON_TO_TEXT",
        "SYSTEM$EXPLAIN_PLAN_JSON",
        "SYSTEM$EXTERNAL_TABLE_PIPE_STATUS",
        "SYSTEM$GET_AWS_SNS_IAM_POLICY",
        "SYSTEM$GET_CLASSIFICATION_RESULT",
        "SYSTEM$GET_DIRECTORY_TABLE_STATUS",
        "SYSTEM$GET_ICEBERG_TABLE_INFORMATION",
        "SYSTEM$GET_PREDECESSOR_RETURN_VALUE",
        "SYSTEM$GET_PRIVATELINK_CONFIG",
        "SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO",
        "SYSTEM$GET_TAG",
        "SYSTEM$GET_TAG_ALLOWED_VALUES",
        "SYSTEM$GET_TAG_ON_CURRENT_COLUMN",
        "SYSTEM$GET_TAG_ON_CURRENT_TABLE",
        "SYSTEM$INTERNAL_STAGES_PUBLIC_ACCESS_STATUS",
        "SYSTEM$LAST_CHANGE_COMMIT_TIME",
        "SYSTEM$PIPE_STATUS",
        "SYSTEM$SHOW_ACTIVE_BEHAVIOR_CHANGE_BUNDLES",
        "SYSTEM$STREAM_GET_TABLE_TIMESTAMP",
        "SYSTEM$STREAM_HAS_DATA",
        "SYSTEM$TASK_RUNTIME_INFO",
        "SYSTEM$TYPEOF",
        "SYSTEM$VALIDATE_STORAGE_INTEGRATION",
        "SYSTEM$WHITELIST",
        "SYSTEM$WHITELIST_PRIVATELINK",
    }
)

# Documented Snowflake built-ins without side effects that sqlglot parses as
# generic (Anonymous) functions rather than typed expressions. Functions that
# sqlglot does model as typed expressions are accepted without being listed.
READ_ONLY_BUILTIN_FUNCTIONS = frozenset(
    {
        # Context, session and metadata
        "CURRENT_ACCOUNT",
        "CURRENT_ACCOUNT_NAME",
        "CURRENT_AVAILABLE_ROLES",
        "CURRENT_CLIENT",
        "CURRENT_IP_ADDRESS",
        "CURRENT_ORGANIZATION_NAME",
        "CURRENT_REGION",
        "CURRENT_ROLE",
        "CURRENT_ROLE_TYPE",
        "CURRENT_SECONDARY_ROLES",
        "CURRENT_SESSION",
        "CURRENT_STATEMENT",
        "CURRENT_TRANSACTION",
        "CURRENT_VERSION",
        "CURRENT_WAREHOUSE",
        "GET_DDL",
        "GETVARIABLE",
        "INVOKER_ROLE",
        "INVOKER_SHARE",
        "IS_DATABASE_ROLE_IN_SESSION",
        "IS_GRANTED_TO_INVOKER_ROLE",
        "IS_ROLE_IN_SESSION",
        "LAST_QUERY_ID",
        "LAST_TRANSACTION",
        "POLICY_CONTEXT",
        "RESULT_SCAN",
        # Type predicates and conversions
        "AS_ARRAY",
        "AS_BINARY",
        "AS_BOOLEAN",
        "AS_CHAR",
        "AS_DATE",
        "AS_DECIMAL",
        "AS_DOUBLE",
        "AS_INTEGER",
        "AS_NUMBER",
        "AS_OBJECT",
        "AS_REAL",
        "AS_TIME",
        "AS_TIMESTAMP_LTZ",
        "AS_TIMESTAMP_NTZ",
        "AS_TIMESTAMP_TZ",
        "AS_VARCHAR",
        "IS_BINARY",
        "IS_BOOLEAN",
        "IS_CHAR",
        "IS_DATE",
        "IS_DATE_VALUE",
        "IS_DECIMAL",
        "IS_DOUBLE",
        "IS_INTEGER",
        "IS_NULL_VALUE",
        "IS_OBJECT",
        "IS_REAL",
        "IS_TIME",
        "IS_TIMESTAMP_LTZ",
        "IS_TIMESTAMP_NTZ",
        "IS_TIMESTAMP_TZ",
        "IS_VARCHAR",
        "TO_GEOGRAPHY",
        "TO_GEOMETRY",
        "TO_OBJECT",
        "TO_VARIANT",
        "TRY_TO_GEOGRAPHY",
        "TRY_TO_GEOMETRY",
        "TYPEOF",
        # Semi-structured data
        "ARRAY_POSITION",
        "ARRAYS_OVERLAP",
        "ARRAYS_TO_OBJECT",
        "CHECK_JSON",
        "CHECK_XML",
        "GET_IGNORE_CASE",
        "OBJECT_DELETE",
        "OBJECT_INSERT",
        "OBJECT_PICK",
        "PARSE_XML",
        "SPLIT_TO_TABLE",
        "STRIP_NULL_VALUE",
        "STRTOK_SPLIT_TO_TABLE",
        "XMLGET",
        # Hashing, statistics and numeric helpers
        "APPROX_PERCENTILE_ACCUMULATE",
        "APPROX_PERCENTILE_COMBINE",
        "APPROX_PERCENTILE_ESTIMATE",
        "APPROX_TOP_K_ACCUMULATE",
        "APPROX_TOP_K_COMBINE",
        "APPROX_TOP_K_ESTIMATE",
        "CONDITIONAL_CHANGE_EVENT",
        "CONDITIONAL_TRUE_EVENT",
        "HASH",
        "HLL_ACCUMULATE",
        "HLL_COMBINE",
        "HLL_ESTIMATE",
        "HLL_EXPORT",
        "HLL_IMPORT",
        "MINHASH_COMBINE",
        "RATIO_TO_REPORT",
        "TRUNC",
        # Geospatial
        "H3_CELL_TO_PARENT",
        "H3_LATLNG_TO_CELL",
        "ST_AREA",
        "ST_ASGEOJSON",
        "ST_ASTEXT",
        "ST_ASWKT",
        "ST_BUFFER",
        "ST_CENTROID",
        "ST_CONTAINS",
        "ST_DWITHIN",
        "ST_GEOGFROMTEXT",
        "ST_GEOGRAPHYFROMWKT",
        "ST_INTERSECTS",
        "ST_LENGTH",
        "ST_PERIMETER",
        "ST_SETSRID",
        "ST_SRID",
        "ST_TRANSFORM",
        "ST_UNION",
        "ST_WITHIN",
        "ST_X",
        "ST_Y",
        # Information Schema table functions
        "ALERT_HISTORY",
        "AUTO_REFRESH_REGISTRATION_HISTORY",
        "AUTOMATIC_CLUSTERING_HISTORY",
        "COMPLETE_TASK_GRAPHS",
        "COPY_HISTORY",
        "CURRENT_TASK_GRAPHS",
        "DATA_METRIC_FUNCTION_REFERENCES",
        "DATA_TRANSFER_HISTORY",
        "DATABASE_REPLICATION_USAGE_HISTORY",
        "DATABASE_STORAGE_USAGE_HISTORY",
        "DYNAMIC_TABLE_GRAPH_HISTORY",
        "DYNAMIC_TABLE_REFRESH_HISTORY",
        "EXPLAIN_JSON",
        "EXTERNAL_TABLE_FILE_REGISTRATION_HISTORY",
        "EXTERNAL_TABLE_FILES",
        "GENERATE_COLUMN_DESCRIPTION",
        "GET_OBJECT_REFERENCES",
        "GET_QUERY_OPERATOR_STATS",
        "INFER_SCHEMA",
        "LOGIN_HISTORY",
        "LOGIN_HISTORY_BY_USER",
        "MATERIALIZED_VIEW_REFRESH_HISTORY",
        "NOTIFICATION_HISTORY",
        "OBJECT_DEPENDENCIES",
        "PIPE_USAGE_HISTORY",
        "POLICY_REFERENCES",
        "QUERY_ACCELERATION_HISTORY",
        "QUERY_HISTORY",
        "QUERY_HISTORY_BY_SESSION",
        "QUERY_HISTORY_BY_USER",
        "QUERY_HISTORY_BY_WAREHOUSE",
        "REPLICATION_USAGE_HISTORY",
        "REST_EVENT_HISTORY",
        "SEARCH_OPTIMIZATION_HISTORY",
        "SERVERLESS_TASK_HISTORY",
        "STAGE_STORAGE_USAGE_HISTORY",
        "TAG_REFERENCES",
        "TAG_REFERENCES_ALL_COLUMNS",
        "TAG_REFERENCES_WITH_LINEAGE",
        "TASK_HISTORY",
        "VALIDATE",
        "VALIDATE_PIPE_LOAD",
        "WAREHOUSE_LOAD_HISTORY",
        "WAREHOUSE_METERING_HISTORY",
    }
)


class PolicyViolation(ValueError):
    """Raised when a statement is refused by the query policy."""


def normalize_function_name(name: str) -> str:
    return name.strip().strip('"').upper()


def parse_function_list(value: str | None) -> frozenset[str]:
    if not value:
        return frozenset()
    return frozenset(
        normalize_function_name(part) for part in value.split(",") if part.strip()
    )


def _identifier_parts(node: exp.Expression) -> list[str]:
    if isinstance(node, exp.Dot):
        return _identifier_parts(node.this) + _identifier_parts(node.expression)
    return [normalize_function_name(node.name)]


def function_names(node: exp.Func) -> tuple[str, str]:
    """Return the (bare, qualified) upper-cased names of a function call."""
    if isinstance(node, exp.Anonymous):
        raw = node.this
        bare = raw.name if isinstance(raw, exp.Expression) else str(raw)
    else:
        bare = node.sql_name()
    bare = normalize_function_name(bare)

    parent = node.parent
    if isinstance(parent, exp.Dot) and parent.expression is node:
        qualified = ".".join(_identifier_parts(parent.this) + [bare])
    else:
        qualified = bare
    return bare, qualified


@dataclass(frozen=True)
class QueryPolicy:
    allow_unknown_functions: bool = False
    extra_allowed_functions: frozenset[str] = frozenset()

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> QueryPolicy:
        environ = os.environ if environ is None else environ
        mode = environ.get(UNKNOWN_FUNCTIONS_ENV, "deny").strip().lower() or "deny"
        if mode not in {"allow", "deny"}:
            raise ValueError(
                f"{UNKNOWN_FUNCTIONS_ENV} must be 'allow' or 'deny', got {mode!r}"
            )
        return cls(
            allow_unknown_functions=(mode == "allow"),
            extra_allowed_functions=parse_function_list(
                environ.get(ALLOWED_FUNCTIONS_ENV)
            ),
        )

    def describe(self) -> str:
        unknown = "allowed" if self.allow_unknown_functions else "denied"
        extra = ", ".join(sorted(self.extra_allowed_functions)) or "none"
        return (
            f"single read-only statement per request; SYSTEM$ functions limited to a "
            f"read-only list; unknown functions {unknown}; extra allowed functions: {extra}"
        )

    def check(self, statement: str | None) -> exp.Expression:
        """Return the parsed statement or raise PolicyViolation."""
        if statement is None or not str(statement).strip():
            raise PolicyViolation("Statement is empty.")

        try:
            parsed = sqlglot.parse(statement, read=DIALECT)
        except SqlglotError as error:
            summary = str(error).splitlines()[0]
            raise PolicyViolation(
                f"Statement could not be parsed and was not executed: {summary}"
            ) from None

        expressions = [expression for expression in parsed if expression is not None]
        if len(expressions) != 1:
            raise PolicyViolation(
                f"Exactly one SQL statement per request is allowed; received {len(expressions)}."
            )

        root = expressions[0]
        root_type = type(root).__name__
        if root_type not in READ_ONLY_ROOTS:
            raise PolicyViolation(
                f"Statement type {root_type} is not allowed; only read-only statements "
                f"({', '.join(sorted(READ_ONLY_ROOTS))}) may be executed."
            )

        for node in root.walk():
            if isinstance(node, exp.Command):
                raise PolicyViolation(
                    "Statement contains syntax that could not be analyzed and was not executed."
                )
            if isinstance(node, exp.Func):
                self._check_function(node)
        return root

    def is_allowed(self, statement: str | None) -> bool:
        try:
            self.check(statement)
        except PolicyViolation:
            return False
        return True

    def _check_function(self, node: exp.Func) -> None:
        bare, qualified = function_names(node)
        if self._explicitly_allowed((bare, qualified)):
            return

        if bare.startswith(SYSTEM_FUNCTION_PREFIX):
            if bare in READ_ONLY_SYSTEM_FUNCTIONS:
                return
            raise PolicyViolation(
                f"System function {qualified} is not permitted by the query policy."
            )

        if not isinstance(node, exp.Anonymous):
            return
        if bare in READ_ONLY_BUILTIN_FUNCTIONS or self.allow_unknown_functions:
            return
        raise PolicyViolation(
            f"Function {qualified} is not in the list of permitted functions. "
            f"Add it to {ALLOWED_FUNCTIONS_ENV} if it is required."
        )

    def _explicitly_allowed(self, names: Iterable[str]) -> bool:
        return any(name in self.extra_allowed_functions for name in names)
