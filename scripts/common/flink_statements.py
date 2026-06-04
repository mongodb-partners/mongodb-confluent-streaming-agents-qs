"""Single source of truth for Flink statement names.

deploy, destroy, and pipeline_reset previously each
defined their own DDL_STATEMENTS / DML_STATEMENTS lists. They had to
stay in lockstep for correctness, and a recent review found drift:
destroy.py omitted agent/tool statement names that deploy.py creates,
so they accumulated in the Confluent environment across destroy/deploy
cycles.

This module centralises the lists; the three callers import from here.
"""
from __future__ import annotations

# DDL statements (CREATE TABLE — expected phase is COMPLETED).
DDL_STATEMENTS: tuple[str, ...] = (
    "anomalies-enriched-ctas",
    "completed-actions-ctas",
)

# DML statements (INSERT INTO — expected phase is RUNNING).
# Order matters: each statement may depend on artifacts the previous
# created.
DML_STATEMENTS: tuple[str, ...] = (
    "zone-traffic-sink-insert",
    "anomaly-detection-insert",
    "anomalies-enriched-insert",
    "anomalies-sink-insert",
    "dispatch-insert",
)

# Agent / tool / model bootstrap statements created at deploy time
# alongside the DML chain.
AGENT_BOOTSTRAP_STATEMENTS: tuple[str, ...] = (
    "create-tool-mongodb-fleet",
    "create-agent-boat-dispatch",
)

# Transient drop statements created during MCP catalog cleanup. These
# are submitted, executed, then deleted — but if the deploy crashes
# between create and delete they linger as orphans in the Confluent
# statement catalog.
MCP_DROP_STATEMENTS: tuple[str, ...] = (
    "mcp-drop-agent",
    "mcp-drop-tool",
    "mcp-drop-model",
    "mcp-drop-connection",
)

# dashboard's "Run Agent Dispatch" button creates these
# statements (see scripts/dashboard.py:646-650). They share namespace
# with the deploy-bootstrapped agent statements above, but use
# distinct names (`dashboard-*`) so the button can recreate them
# without colliding with the deploy chain. Workshop participants who
# click the button then run `uv run destroy` previously left these as
# orphans that 409'd the next deploy.
DASHBOARD_AGENT_STATEMENTS: tuple[str, ...] = (
    "dashboard-create-tool",
    "dashboard-create-agent",
    "dashboard-create-completed-actions",
    "dashboard-create-completed-actions-table",
)

# All statements destroy.py should attempt to remove (in this order,
# because of dependency directions).
ALL_DELETABLE_STATEMENTS: tuple[str, ...] = (
    *DML_STATEMENTS,
    *AGENT_BOOTSTRAP_STATEMENTS,
    *DASHBOARD_AGENT_STATEMENTS,
    *MCP_DROP_STATEMENTS,
    *DDL_STATEMENTS,
)
