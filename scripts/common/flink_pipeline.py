"""Flink pipeline helpers — extracted from scripts/deploy.py.

The bulk of the deploy-side Flink pipeline machinery still lives inside
`_create_flink_dml_statements` (deploy.py) because that 631-line function
with 4 nested closures needs thorough behavior coverage of the
closure-capture semantics before extraction is safe.

This module extracts the small, standalone helpers that have no
closure dependency:
- ``check_mcp_health(url, token) -> bool``
- ``CONNECTION_DRIFT_TRIGGERS`` / ``CONNECTION_TF_RESOURCES``
- ``detect_connection_drift(env_old, env_new) -> set[str]``

These three were already pure functions / module-level dicts in
deploy.py; moving them here reduces deploy.py size and exposes a
proper API surface that future tests can import directly.

Larger extractions (the DML submission loop, topic recreation,
stability validation) remain in deploy.py for now.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request


# mapping from drifted
# env var → Flink connection name(s) that must be `-replaced` and
# have their downstream catalog objects dropped. Symbolic names below
# match the literal `CREATE CONNECTION IF NOT EXISTS \`<name>\`` strings
# in terraform/agents/main.tf.
CONNECTION_DRIFT_TRIGGERS: dict[str, list[str]] = {
    # MCP URL change cascades through the entire MCP chain:
    "TF_VAR_mcp_server_url":            ["mongodb-mcp-connection"],
    "TF_VAR_mcp_auth_token":            ["mongodb-mcp-connection"],
    # Mongo creds drive the vector-DB connection used by AI_RUN_AGENT
    # and the vector_search_aggregate stage:
    "TF_VAR_mongodb_connection_string": ["mongodb-connection"],
    "TF_VAR_mongodb_username":          ["mongodb-connection"],
    "TF_VAR_mongodb_password":          ["mongodb-connection"],
    # Voyage AI key drives the embedding model connection:
    "TF_VAR_voyage_api_key":            ["voyage_connection"],
}

# Map symbolic Flink CONNECTION name → terraform resource address in
# terraform/agents/. Used to translate drift detection into `-replace=`
# args for `terraform apply`.
CONNECTION_TF_RESOURCES: dict[str, str] = {
    "mongodb-connection":     "confluent_flink_statement.mongodb_connection_statement",
    "mongodb-mcp-connection": "confluent_flink_statement.mongodb_mcp_connection",
    "voyage_connection":      "confluent_flink_statement.voyage_connection",
}


def detect_connection_drift(env_old: dict, env_new: dict) -> set[str]:
    """Compare two .env snapshots and return Flink connection names
    whose backing credentials have changed.

    the 4 Flink CONNECTION resources in
    terraform/agents declare ``ignore_changes = [statement]``, so
    credential rotations (mongo password, voyage key, etc.) do NOT
    trigger terraform to re-apply them. The deploy must detect drift
    explicitly and `-replace` the affected resources.

    Returns names matching the symbolic names used inside SQL
    `CREATE CONNECTION` statements (e.g. "mongodb-mcp-connection").
    """
    drifted: set[str] = set()
    for var, conn_names in CONNECTION_DRIFT_TRIGGERS.items():
        old = env_old.get(var, "")
        new = env_new.get(var, "")
        # "old empty AND new non-empty" is INITIAL PROVISIONING, not
        # rotation — don't trigger a replace for that case.
        if old and new and old != new:
            drifted.update(conn_names)
    return drifted


def check_mcp_health(url: str, token: str, timeout: int = 15) -> bool:
    """Probe the MCP server's `/mcp` endpoint with a JSON-RPC
    `initialize`. Returns True on HTTP 200, False otherwise.

    Used by deploy.py to gate `dispatch-insert` submission: if MCP is
    unreachable, the dispatch agent's Flink INSERT would FAIL at
    runtime, so the deploy skips it with an actionable error rather
    than producing a FAILED statement.
    """
    check_url = f"{url}/mcp"
    body = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "deploy-check", "version": "1.0"},
        },
        "id": 1,
    }).encode()
    try:
        req = urllib.request.Request(
            check_url, data=body, method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "Authorization": f"Bearer {token}",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False
