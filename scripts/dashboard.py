#!/usr/bin/env python3
"""Real-Time Workshop Dashboard.

Streamlit-based visualization dashboard for Atlas-Enhanced Agents that
displays the full pipeline architecture with live data from MongoDB Atlas.

Launch:  uv run dashboard
"""

import argparse
import html
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from scripts.common.http_auth import basic_auth_token

# ---------------------------------------------------------------------------
# Try-imports with HAS_* flags (project convention)
# ---------------------------------------------------------------------------

try:
    import streamlit as st

    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import plotly.express as px

    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

try:
    # These imports are the HAS_PYMONGO availability sentinel (the module
    # queries Mongo via scripts.common.mongo.get_client). Removing them would
    # make the try-block unconditionally set HAS_PYMONGO=True, so keep (noqa).
    from pymongo import MongoClient  # noqa: F401
    from pymongo.errors import ConnectionFailure  # noqa: F401

    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False

try:
    from bson.decimal128 import Decimal128

    HAS_BSON = True
except ImportError:
    HAS_BSON = False

try:
    from dotenv import dotenv_values

    HAS_DOTENV = True
except ImportError:
    HAS_DOTENV = False

try:
    import requests as _requests_mod

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import pydeck as pdk

    HAS_PYDECK = True
except ImportError:
    HAS_PYDECK = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Map geometry + window constants live in scripts/common/geo.py (shared with
# the Mission Control HUD via live_server's /api/bootstrap). Re-exported here
# so existing imports/tests (`dashboard.ZONE_COORDS`, …) keep working.
from scripts.common.geo import (  # noqa: F401
    FALLBACK_ZONES,
    MAP_VIEW,
    RIVER_WAYPOINTS,
    TRIPS_DURATION_MS,
    TRIPS_LOOP_MS,
    TRIPS_TRAIL_MS,
    WINDOW_MINUTES,
    ZONE_COORDS,
    ZONE_DOCK_COORDS,
    ZONE_RIVER_INDEX,
)

COLLECTIONS = {
    "zone_traffic": ("analytics", "zone_traffic"),
    "anomalies": ("analytics", "zone_anomalies"),
    "dispatches": ("fleet", "dispatch_log"),
    "knowledge_base": ("events", "knowledge_base"),
}

EMPTY_STATE_MESSAGES = {
    "zone_traffic": (
        "No traffic data yet. Use the **Start Data Generation** button in the "
        "Actions sidebar (or run `uv run datagen`). ShadowTraffic produces "
        "simulated ride requests into Kafka; Flink aggregates them into 1-minute "
        "tumbling windows. First data points typically appear after ~1 minute."
    ),
    "anomalies": (
        "No anomalies detected yet. Flink's DETECT_ANOMALIES function needs a "
        "baseline of traffic windows before it can identify surges. Ensure data "
        "generation is running and the Flink anomaly detection statements are active "
        "(see **Run Agent Dispatch** in the Actions sidebar)."
    ),
    "dispatches": (
        "No agent dispatches yet. Dispatches appear after the full pipeline runs: "
        "anomalies are detected → the Flink AI_RUN_AGENT invokes the boat dispatch "
        "agent via MCP → completed actions flow through Kafka → Atlas Stream "
        "Processing writes them to fleet.dispatch_log. Use **Run Agent Dispatch** "
        "in the Actions sidebar to set up the Flink agent pipeline."
    ),
    "knowledge_base": (
        "No knowledge base events found. Use the **Seed Events** button in the "
        "Actions sidebar (or run `uv run asp-setup --seed-only`) to populate "
        "events.knowledge_base with local event data (Jazz Fest, Mardi Gras, etc.). "
        "These events are vectorized via Voyage AI and used for RAG context."
    ),
}

TIME_RANGE_OPTIONS = {
    "Last 15 min": timedelta(minutes=15),
    "Last 1 hour": timedelta(hours=1),
    "Last 6 hours": timedelta(hours=6),
    "Last 24 hours": timedelta(hours=24),
    "All time": None,
}

# Technology colors
COLOR_KAFKA = "#0078FF"
COLOR_FLINK = "#FF9800"
COLOR_MONGODB = "#00ED64"
COLOR_ASP = "#A855F7"

# MongoDB branding
MONGODB_SPRING_GREEN = "#00ED64"
MONGODB_DARK_NAVY = "#001E2B"
MONGODB_FOREST_GREEN = "#00684A"

# ---------------------------------------------------------------------------
# MongoDB-branded dark theme CSS
# Adapted from MongoDB Solutions Library design system
# ---------------------------------------------------------------------------

MONGODB_THEME_CSS = """
<style>
@import url('https://static.mongodb.com/com/fonts/EuclidCircularA-Regular-WebXL.woff2');

@font-face {
    font-family: 'Euclid Circular A';
    src: url('https://static.mongodb.com/com/fonts/EuclidCircularA-Regular-WebXL.woff2') format('woff2');
    font-weight: normal;
    font-display: swap;
}
@font-face {
    font-family: 'Euclid Circular A';
    src: url('https://static.mongodb.com/com/fonts/EuclidCircularA-Medium-WebXL.woff2') format('woff2');
    font-weight: 500;
    font-display: swap;
}
@font-face {
    font-family: 'MongoDB Value Serif';
    src: url('https://static.mongodb.com/com/fonts/MongoDBValueSerif-Medium.woff2') format('woff2');
    font-weight: 500;
    font-display: swap;
}

/* Root variables */
:root {
    --mdb-green: #00ED64;
    --mdb-forest: #00684A;
    --mdb-navy: #001E2B;
    --mdb-bg: #060A0F;
    --mdb-surface: #0C1117;
    --mdb-surface-card: rgba(12, 17, 23, 0.85);
    --mdb-border: rgba(255, 255, 255, 0.06);
    --mdb-border-hover: rgba(0, 237, 100, 0.2);
    --mdb-text: #F0F4F8;
    --mdb-text-secondary: #C8D5DE;
    --mdb-glow: rgba(0, 237, 100, 0.15);
    --mdb-kafka: #0078FF;
    --mdb-flink: #FF9800;
    --mdb-asp: #A855F7;
}

/* Global overrides */
.stApp {
    background: var(--mdb-bg) !important;
    font-family: 'Euclid Circular A', -apple-system, BlinkMacSystemFont, sans-serif !important;
}

/* Aurora background effect */
.stApp::before {
    content: '';
    position: fixed;
    inset: -50%;
    background:
        radial-gradient(ellipse at 20% 50%, rgba(0, 237, 100, 0.06) 0%, transparent 50%),
        radial-gradient(ellipse at 80% 20%, rgba(0, 120, 255, 0.04) 0%, transparent 40%),
        radial-gradient(ellipse at 50% 80%, rgba(168, 85, 247, 0.03) 0%, transparent 45%);
    animation: aurora 60s linear infinite;
    pointer-events: none;
    z-index: 0;
}

@keyframes aurora {
    0% { transform: translate(0, 0) rotate(0deg) scale(1); }
    25% { transform: translate(-3%, 2%) rotate(0.5deg) scale(1.01); }
    50% { transform: translate(3%, -1%) rotate(-0.5deg) scale(0.99); }
    75% { transform: translate(-2%, -3%) rotate(0.3deg) scale(1.005); }
    100% { transform: translate(0, 0) rotate(0deg) scale(1); }
}

@keyframes pulse-glow {
    0%, 100% { box-shadow: 0 0 8px rgba(0, 237, 100, 0.3); }
    50% { box-shadow: 0 0 20px rgba(0, 237, 100, 0.6); }
}

@keyframes shimmer {
    0% { background-position: -200% center; }
    100% { background-position: 200% center; }
}

@keyframes data-flow {
    0% { transform: translateX(-100%); opacity: 0; }
    20% { opacity: 1; }
    80% { opacity: 1; }
    100% { transform: translateX(100%); opacity: 0; }
}

/* Main content area */
.main .block-container {
    background: transparent !important;
    position: relative;
    z-index: 1;
}

/* Headers */
h1, .stTitle > div > h1 {
    font-family: 'MongoDB Value Serif', Georgia, serif !important;
    color: var(--mdb-text) !important;
    font-weight: 500 !important;
    letter-spacing: -0.5px;
}

h2, h3 {
    color: var(--mdb-text) !important;
    font-family: 'Euclid Circular A', sans-serif !important;
}

/* Accent text styling */
.stTitle > div > h1 span {
    background: linear-gradient(135deg, #00ED64, #7CF5A5, #0078FF, #00ED64);
    background-size: 300% 100%;
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    animation: shimmer 6s ease-in-out infinite;
}

/* Body text */
p, span, label, .stCaption, .stMarkdown {
    color: var(--mdb-text-secondary) !important;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: var(--mdb-surface) !important;
    border-right: 1px solid var(--mdb-border) !important;
}

section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    color: var(--mdb-text) !important;
}

section[data-testid="stSidebar"] .stMarkdown p {
    color: var(--mdb-text-secondary) !important;
}

/* Metric cards */
[data-testid="stMetric"] {
    background: var(--mdb-surface-card) !important;
    border: 1px solid var(--mdb-border) !important;
    border-radius: 12px !important;
    padding: 16px 20px !important;
    backdrop-filter: blur(12px) !important;
    transition: all 0.3s ease !important;
}

[data-testid="stMetric"]:hover {
    border-color: var(--mdb-border-hover) !important;
    box-shadow: 0 0 20px var(--mdb-glow) !important;
}

[data-testid="stMetric"] label {
    color: var(--mdb-text-secondary) !important;
    font-size: 0.75rem !important;
    text-transform: uppercase !important;
    letter-spacing: 1.5px !important;
}

[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: var(--mdb-green) !important;
    font-weight: 600 !important;
    font-size: 1.8rem !important;
    text-shadow: 0 0 30px rgba(0, 237, 100, 0.2);
}

/* Buttons */
.stButton > button {
    background: linear-gradient(135deg, var(--mdb-forest), var(--mdb-green)) !important;
    color: var(--mdb-navy) !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-family: 'Euclid Circular A', sans-serif !important;
    padding: 8px 24px !important;
    transition: all 0.3s ease !important;
    text-shadow: none !important;
}

.stButton > button:hover {
    box-shadow: 0 0 24px rgba(0, 237, 100, 0.4), 0 4px 12px rgba(0, 0, 0, 0.3) !important;
    transform: translateY(-1px) !important;
}

.stButton > button:active {
    transform: translateY(0) !important;
}

/* Progress bar */
.stProgress > div > div {
    background: linear-gradient(90deg, var(--mdb-forest), var(--mdb-green)) !important;
    border-radius: 4px !important;
    box-shadow: 0 0 8px rgba(0, 237, 100, 0.3) !important;
}

.stProgress > div {
    background: rgba(255, 255, 255, 0.05) !important;
}

/* Selectbox / multiselect */
.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: var(--mdb-surface) !important;
    border-color: var(--mdb-border) !important;
    color: var(--mdb-text) !important;
}

/* Expander */
.streamlit-expanderHeader {
    background: var(--mdb-surface-card) !important;
    border: 1px solid var(--mdb-border) !important;
    border-radius: 8px !important;
    color: var(--mdb-text) !important;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    border-bottom: 1px solid var(--mdb-border) !important;
}

.stTabs [data-baseweb="tab"] {
    color: var(--mdb-text-secondary) !important;
}

.stTabs [aria-selected="true"] {
    color: var(--mdb-green) !important;
    border-bottom-color: var(--mdb-green) !important;
}

/* Success / info / error alerts */
.stSuccess {
    background: rgba(0, 237, 100, 0.08) !important;
    border: 1px solid rgba(0, 237, 100, 0.2) !important;
    color: var(--mdb-green) !important;
    border-radius: 8px !important;
}

.stInfo {
    background: rgba(0, 120, 255, 0.08) !important;
    border: 1px solid rgba(0, 120, 255, 0.2) !important;
    color: #60A5FA !important;
    border-radius: 8px !important;
}

.stWarning {
    background: rgba(255, 152, 0, 0.08) !important;
    border: 1px solid rgba(255, 152, 0, 0.2) !important;
    border-radius: 8px !important;
}

.stError {
    background: rgba(239, 68, 68, 0.08) !important;
    border: 1px solid rgba(239, 68, 68, 0.2) !important;
    border-radius: 8px !important;
}

/* Divider */
hr {
    border-color: var(--mdb-border) !important;
}

/* Plotly charts - dark background */
.js-plotly-plot .plotly .main-svg {
    background: transparent !important;
}

/* Dataframes / tables */
.stDataFrame {
    border: 1px solid var(--mdb-border) !important;
    border-radius: 8px !important;
    overflow: hidden;
}

/* Caption text */
.stCaption p {
    color: var(--mdb-text-secondary) !important;
}

/* Radio buttons */
.stRadio > label {
    color: var(--mdb-text) !important;
}

/* Scrollbar */
::-webkit-scrollbar {
    width: 6px;
    height: 6px;
}
::-webkit-scrollbar-track {
    background: var(--mdb-bg);
}
::-webkit-scrollbar-thumb {
    background: rgba(255, 255, 255, 0.1);
    border-radius: 3px;
}
::-webkit-scrollbar-thumb:hover {
    background: rgba(0, 237, 100, 0.3);
}
</style>
"""

IMPACT_COLORS = {
    "high": "#E53935",
    "medium": "#FB8C00",
    "low": "#43A047",
}

# ---------------------------------------------------------------------------
# Agent SQL constants (verbatim from walkthrough steps 6-7)
# Statement names prefixed with "dashboard-" to avoid terraform collisions.
# ---------------------------------------------------------------------------

AGENT_SQL_CREATE_TOOL = """\
CREATE TOOL IF NOT EXISTS mongodb_fleet
USING CONNECTION `mongodb-mcp-connection`
WITH (
  'type' = 'mcp',
  'allowed_tools' = 'get_vessel_catalog, dispatch_boats',
  'request_timeout' = '15'
);"""

AGENT_SQL_CREATE_AGENT = """\
CREATE AGENT IF NOT EXISTS `boat_dispatch_agent`
USING MODEL `mongodb_mcp_model`
USING PROMPT 'You are an intelligent boat dispatch coordinator for a riverboat ride-sharing service.

Your workflow:
1. ANALYZE the surge information provided (zone, time, request count, anomaly reason)
2. REVIEW the available vessels list using the get_vessel_catalog tool
3. SELECT appropriate boats to dispatch based on:
   - Proximity to the target zone
   - Boat capacity
   - Current availability
   - Surge magnitude (dispatch up to 8 boats for large surges)
4. USE the dispatch_boats tool to dispatch selected boats to the target zone.
   Pass the zone and an array of boats with vessel_id, new_zone, and new_availability.

5. FORMAT your final response with these THREE sections:

Dispatch Summary:
Due to the surge in demand in [zone] as a result of [event], we dispatched [n] additional boats from [list of zones].

Dispatch JSON:
{the dispatch_boats parameters you sent}

API Response:
{the response from the dispatch_boats tool}

CRITICAL INSTRUCTIONS:
- Dispatch boats from nearby zones first
- Dispatch more boats with larger capacities for big surges (up to 8 boats)
- Your response MUST contain the three labeled sections
- The dispatch JSON must be valid
- Always execute the dispatch and include the tool response
- Do NOT include any other explanatory text outside these three sections'
USING TOOLS `mongodb_fleet`
WITH (
  'max_iterations' = '10'
);"""

AGENT_SQL_CREATE_COMPLETED_ACTIONS_TABLE = """\
CREATE TABLE IF NOT EXISTS completed_actions (
    pickup_zone STRING,
    window_time TIMESTAMP(3),
    request_count BIGINT,
    anomaly_reason STRING,
    dispatch_summary STRING,
    dispatch_json STRING,
    api_response STRING
) WITH ('changelog.mode' = 'append');"""

AGENT_SQL_INSERT_COMPLETED_ACTIONS = """\
INSERT INTO completed_actions
SELECT
    pickup_zone,
    window_time,
    request_count,
    CONCAT('Surge detected: ', CAST(request_count AS STRING), ' requests (expected ', CAST(expected_requests AS STRING), ')') AS anomaly_reason,
    COALESCE(
        TRIM(REGEXP_EXTRACT(CAST(response AS STRING), 'Dispatch Summary[:\\s]*\\n(.+?)(?=\\n\\n(?:Dispatch JSON|$))', 1)),
        CAST(response AS STRING)
    ) AS dispatch_summary,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), 'Dispatch JSON[:\\s]*\\n(?:```(?:json)?\\s*)?([\\s\\S]+?)(?:```)?(?=\\n\\n(?:API Response|$))', 1)) AS dispatch_json,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), 'API Response[:\\s]*\\n(?:```(?:json)?\\s*)?([\\s\\S]+?)(?:```)?\\s*$', 1)) AS api_response
FROM anomalies_per_zone,
LATERAL TABLE(AI_RUN_AGENT(
    `boat_dispatch_agent`,
    CONCAT('Demand surge in ', pickup_zone, ': ', CAST(request_count AS STRING), ' ride requests in 5 minutes (expected ', CAST(expected_requests AS STRING), '). Surge ratio: ', COALESCE(CAST(ROUND(CAST(request_count AS DOUBLE) / NULLIF(CAST(expected_requests AS DOUBLE), 0), 1) AS STRING), 'N/A'), 'x'),
    `pickup_zone`
))
WHERE is_surge = true;"""

AGENT_SQL_STEPS = [
    ("dashboard-create-tool", AGENT_SQL_CREATE_TOOL),
    ("dashboard-create-agent", AGENT_SQL_CREATE_AGENT),
    (
        "dashboard-create-completed-actions-table",
        AGENT_SQL_CREATE_COMPLETED_ACTIONS_TABLE,
    ),
    ("dashboard-create-completed-actions", AGENT_SQL_INSERT_COMPLETED_ACTIONS),
]

# `uv run deploy` creates the SAME shared catalog objects (mongodb_fleet tool,
# boat_dispatch_agent, completed_actions) but under CANONICAL statement-record
# names, not the dashboard-* names above. The dashboard must recognise a
# deploy-provisioned pipeline so it does NOT offer to "Run Agent Dispatch" and
# then DROP the shared objects the deployed dispatch-insert depends on.
# These names mirror scripts/deploy.py's DML_STATEMENTS / bootstrap steps.
DEPLOY_DISPATCH_STATEMENT = "dispatch-insert"
DEPLOY_AGENT_STATEMENTS = (
    "create-tool-mongodb-fleet",
    "create-agent-boat-dispatch",
    "dispatch-insert",
)


# ---------------------------------------------------------------------------
# Streamlit detection
# ---------------------------------------------------------------------------


def _is_running_in_streamlit() -> bool:
    """Check whether we are currently running inside a Streamlit server."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Credential Resolution
# ---------------------------------------------------------------------------


def _load_env_defaults(project_root: Optional[Path] = None) -> dict:
    """Load defaults from .env if available.

    When project_root is given, ONLY search that directory.
    Otherwise, search from the script directory upward.
    """
    if not HAS_DOTENV:
        return {}
    if project_root:
        env_file = project_root / ".env"
        if env_file.exists():
            return {k: v for k, v in dotenv_values(env_file).items() if v}
        return {}
    here = Path(__file__).resolve().parent
    for p in [here, *here.parents]:
        env_file = p / ".env"
        if env_file.exists():
            return {k: v for k, v in dotenv_values(env_file).items() if v}
    return {}


def _parse_tfvars(tfvars_path: Path) -> dict:
    """Parse a terraform.tfvars file into a dict of key=value pairs."""
    result = {}
    if not tfvars_path.exists():
        return result
    for line in tfvars_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r'^(\w+)\s*=\s*"([^"]*)"', line)
        if match:
            result[match.group(1)] = match.group(2)
    return result


def _build_uri(connection_string: str, username: str, password: str) -> str:
    """Build a MongoDB URI from components.

    Thin wrapper that delegates to scripts.common.mongo.build_uri so all
    URI parsing lives in one place.
    """
    from scripts.common.mongo import build_uri

    return build_uri(connection_string, username, password)


def _resolve_mongodb_uri(project_root: Optional[Path] = None) -> Optional[str]:
    """Resolve MongoDB URI using the shared 4-source credential chain.

    Delegates to scripts.common.mongo_uri so the dashboard and the live SSE
    sidecar resolve the connection string identically (spec INV-005). The
    resolution order (first match wins) is: .env -> terraform.tfvars ->
    $MONGODB_URI -> None (Streamlit sidebar fallback).
    """
    from scripts.common.mongo_uri import resolve_mongodb_uri

    return resolve_mongodb_uri(project_root=project_root)


def _get_project_root() -> Optional[Path]:
    """Find the project root by looking for pyproject.toml."""
    here = Path(__file__).resolve().parent
    for p in [here, *here.parents]:
        if (p / "pyproject.toml").exists():
            return p
    return None


@st.cache_resource
def _get_boat_data_uri() -> Optional[str]:
    """Return the boat icon as a data:image/png;base64,... URI.

    cached for the lifetime of the Streamlit session
    via @st.cache_resource. Without this, every 1-second fragment
    tick re-read 30KB from disk, base64-encoded, and streamed 40KB
    through the websocket — ~3.6MB/min of redundant traffic per
    connected client.
    """
    import base64 as _b64

    root = _get_project_root() or Path(".")
    boat_path = root / "assets" / "boat-icon.png"
    try:
        return (
            "data:image/png;base64," + _b64.b64encode(boat_path.read_bytes()).decode()
        )
    except OSError:
        return None


# ---------------------------------------------------------------------------
# Flink REST API helpers
# ---------------------------------------------------------------------------


def _load_flink_credentials(
    project_root: Optional[Path] = None,
) -> Optional[Dict[str, str]]:
    """Load Flink credentials from terraform/core/terraform.tfstate.

    Reads the state file directly (same pattern as sql_extractors.py) and
    extracts the outputs needed to submit Flink SQL statements.

    Returns dict with keys: flink_api_key, flink_api_secret, flink_rest_endpoint,
    organization_id, environment_id, compute_pool_id, environment_display_name,
    cluster_display_name.  Returns None if state file is missing or unreadable.
    """
    root = project_root or _get_project_root()
    if root is None:
        return None
    state_file = root / "terraform" / "core" / "terraform.tfstate"
    if not state_file.exists():
        return None
    try:
        with open(state_file) as f:
            state = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None

    # Build outputs dict from state
    outputs: Dict[str, Any] = {}
    for key, val in state.get("outputs", {}).items():
        outputs[key] = val.get("value")

    KEY_MAP = {
        "app_manager_flink_api_key": "flink_api_key",
        "app_manager_flink_api_secret": "flink_api_secret",
        "confluent_flink_rest_endpoint": "flink_rest_endpoint",
        "confluent_organization_id": "organization_id",
        "confluent_environment_id": "environment_id",
        "confluent_flink_compute_pool_id": "compute_pool_id",
        "confluent_environment_display_name": "environment_display_name",
        "confluent_kafka_cluster_display_name": "cluster_display_name",
    }

    creds: Dict[str, str] = {}
    for tf_key, cred_key in KEY_MAP.items():
        value = outputs.get(tf_key)
        if value is None:
            return None
        creds[cred_key] = str(value)
    return creds


def _submit_flink_sql(
    creds: Dict[str, str], statement_name: str, sql: str
) -> Dict[str, Any]:
    """Submit a Flink SQL statement via the Confluent Flink REST API.

    Returns the response JSON on success or ``{"error": "..."}`` on failure.
    """
    if not HAS_REQUESTS:
        return {"error": "requests package not installed"}
    endpoint = creds["flink_rest_endpoint"].rstrip("/")
    url = (
        f"{endpoint}/sql/v1/organizations/{creds['organization_id']}"
        f"/environments/{creds['environment_id']}/statements"
    )
    body = {
        "name": statement_name,
        "organization_id": creds["organization_id"],
        "environment_id": creds["environment_id"],
        "spec": {
            "statement": sql,
            "properties": {
                "sql.current-catalog": creds["environment_display_name"],
                "sql.current-database": creds["cluster_display_name"],
            },
            "compute_pool_id": creds["compute_pool_id"],
        },
    }
    try:
        resp = _requests_mod.post(
            url,
            json=body,
            auth=(creds["flink_api_key"], creds["flink_api_secret"]),
            timeout=30,
        )
        if resp.status_code == 409:
            return {"already_exists": True}
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        return {"error": str(exc)}


def _delete_flink_statement(creds: Dict[str, str], name: str) -> bool:
    """Delete a Flink statement by name. Returns True on success."""
    if not HAS_REQUESTS:
        return False
    endpoint = creds["flink_rest_endpoint"].rstrip("/")
    url = (
        f"{endpoint}/sql/v1/organizations/{creds['organization_id']}"
        f"/environments/{creds['environment_id']}/statements/{name}"
    )
    try:
        resp = _requests_mod.delete(
            url, auth=(creds["flink_api_key"], creds["flink_api_secret"]), timeout=15
        )
        return resp.status_code in (200, 202, 404)
    except Exception:
        return False


def _wait_for_statement_deleted(
    creds: Dict[str, str],
    name: str,
    timeout: int = 30,
) -> bool:
    """Poll until Flink reports the statement no longer exists.

    a fixed `time.sleep(N)` after DELETE permits the
    create-while-deleting race. This helper
    polls `_check_flink_statement_exists` (which returns None on 404)
    every 2 seconds until it returns None or the timeout elapses.

    Returns True when the statement is confirmed gone, False on timeout.
    Failing closed is intentional — caller should refuse to CREATE if
    we can't confirm DELETE.
    """
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        phase = _check_flink_statement_exists(creds, name)
        if phase is None:
            return True
        time.sleep(2)
    return False


def _check_flink_statement_exists(creds: Dict[str, str], name: str) -> Optional[str]:
    """Check whether a Flink statement already exists by name.

    Returns the status phase string (e.g. ``"RUNNING"``, ``"COMPLETED"``)
    or ``None`` if the statement does not exist / lookup fails.

    Paginates through all result pages to avoid missing statements that
    fall beyond the first page.
    """
    if not HAS_REQUESTS:
        return None
    endpoint = creds["flink_rest_endpoint"].rstrip("/")
    base_url = (
        f"{endpoint}/sql/v1/organizations/{creds['organization_id']}"
        f"/environments/{creds['environment_id']}/statements"
    )
    auth = (creds["flink_api_key"], creds["flink_api_secret"])
    try:
        url: Optional[str] = base_url
        while url:
            resp = _requests_mod.get(url, auth=auth, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            for stmt in data.get("data", []):
                if stmt.get("name") == name:
                    return stmt.get("status", {}).get("phase")
            # Follow pagination link if present
            next_link = data.get("metadata", {}).get("next")
            url = next_link if next_link else None
    except Exception:
        pass
    return None


def _connect_mongodb(uri: str) -> Optional[Any]:
    """Connect to MongoDB and verify with a ping. Returns MongoClient or None."""
    if not HAS_PYMONGO:
        return None
    try:
        from scripts.common.mongo import get_client

        client = get_client(uri, app_name="streaming-agents-dashboard")
        client.admin.command("ping")
        return client
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Data type helpers
# ---------------------------------------------------------------------------


def _convert_decimal128(value: Any) -> Any:
    """Convert Decimal128 to float, pass through other types."""
    if HAS_BSON and isinstance(value, Decimal128):
        return float(str(value))
    return value


def _format_datetime(dt: Optional[datetime]) -> str:
    """Format a datetime in UTC for display."""
    if dt is None:
        return "N/A"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def _parse_dispatch_json(raw: Any) -> Any:
    """Parse dispatch_json: return dict if valid JSON, raw string otherwise."""
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw


# Strip the agent's tool-calling transcript. dispatch-insert.sql tries to
# REGEXP_EXTRACT a clean "Dispatch Summary" paragraph out of the agent's
# response, but when that regex misses (the agent wrote the marker inline,
# or prepended its <tool_call>/<tool_response> reasoning), COALESCE falls
# back to the ENTIRE raw transcript. This cleaner makes the dashboard
# robust regardless of what landed in dispatch_summary.
_TOOL_CALL_RE = re.compile(r"<tool_call>.*?</tool_call>", re.DOTALL | re.IGNORECASE)
_TOOL_RESPONSE_RE = re.compile(
    r"<tool_response>.*?</tool_response>", re.DOTALL | re.IGNORECASE
)
_BLANK_LINES_RE = re.compile(r"\n{3,}")


def _clean_dispatch_summary(raw: Any) -> str:
    """Return a human-readable dispatch summary from a possibly-raw agent
    response.

    - Strips `<tool_call>` / `<tool_response>` blocks (the agent's
      tool-calling transcript).
    - If a "Dispatch Summary:" marker is present, returns only that
      paragraph (up to the next "Dispatch JSON" / "API Response" marker
      or end) — the structured JSON renders separately via st.json.
    - Otherwise returns the tool-stripped prose, with runs of blank
      lines collapsed.
    - None / non-str / empty → "".
    """
    if not raw or not isinstance(raw, str):
        return ""

    text = _TOOL_CALL_RE.sub("", raw)
    text = _TOOL_RESPONSE_RE.sub("", text)

    # The agent wraps section markers in markdown emphasis, e.g.
    # `**Dispatch Summary:**` / `**Dispatch JSON:**`. The marker patterns
    # below consume optional leading/trailing emphasis (`*`/`_`) so the
    # stray `**` bold delimiters don't leak into the extracted summary.
    emph = r"[*_]{0,2}"

    # Prefer the explicit "Dispatch Summary:" section when present.
    m = re.search(rf"{emph}\s*Dispatch\s+Summary\s*:?\s*{emph}\s*", text, re.IGNORECASE)
    if m:
        after = text[m.end() :]
        # Cut at the next structured-section marker (also emphasis-wrapped).
        stop = re.search(
            rf"\n*\s*{emph}\s*(?:Dispatch\s+JSON|API\s+Response)\s*:?",
            after,
            re.IGNORECASE,
        )
        summary = after[: stop.start()] if stop else after
        cleaned = _strip_stray_emphasis(summary)
        if cleaned:
            return _BLANK_LINES_RE.sub("\n\n", cleaned)

    # No marker (or empty after it): return tool-stripped prose, but also
    # drop any trailing structured dumps so the card stays readable.
    for marker in ("Dispatch JSON", "API Response"):
        idx = re.search(rf"\n*\s*{emph}\s*{marker}\s*:?", text, re.IGNORECASE)
        if idx:
            text = text[: idx.start()]
    return _BLANK_LINES_RE.sub("\n\n", _strip_stray_emphasis(text))


def _strip_stray_emphasis(text: str) -> str:
    """Trim leading/trailing markdown emphasis (`*`/`_`) and whitespace.

    Handles the stray `** ` / `**` boundaries left when a section sits
    between emphasis-wrapped markers (`**Dispatch Summary:** ... **Dispatch
    JSON:**`). Inner emphasis is preserved — only the boundaries are
    trimmed.
    """
    return (text or "").strip().strip("*_ \t\r\n").strip()


# ---------------------------------------------------------------------------
# Dynamic zone list
# ---------------------------------------------------------------------------


def _fetch_distinct_zones_uncached(client: Optional[Any]) -> List[str]:
    """Return sorted distinct zones from analytics.zone_traffic.

    Falls back to FALLBACK_ZONES when client is None or the query fails.
    """
    if client is None:
        return list(FALLBACK_ZONES)
    try:
        zones = client["analytics"]["zone_traffic"].distinct("zone")
        if zones:
            return sorted(zones)
    except Exception:
        pass
    return list(FALLBACK_ZONES)


# cache distinct zones for the lifetime of the Streamlit session.
# distinct() is O(collection) without an index; the result is essentially
# static (7 zones) so a 5-minute cache is safe.
#
# We can't use @st.cache_data here: MongoClient isn't hashable, and the
# `_arg` escape hatch (which tells Streamlit to skip hashing that arg)
# would let unrelated calls share a cache slot — bad for tests, and
# fragile if multiple clients ever coexist. Instead we cache in
# st.session_state, keyed by id(client), with a 5-minute TTL.
_DISTINCT_ZONES_TTL_S = 300


def _fetch_distinct_zones(client: Optional[Any]) -> List[str]:
    if not HAS_STREAMLIT:
        return _fetch_distinct_zones_uncached(client)
    cache_key = f"_distinct_zones_cache_{id(client)}"
    entry = st.session_state.get(cache_key)
    now = time.time()
    if entry and now - entry["ts"] < _DISTINCT_ZONES_TTL_S:
        return entry["value"]
    value = _fetch_distinct_zones_uncached(client)
    st.session_state[cache_key] = {"value": value, "ts": now}
    return value


# ---------------------------------------------------------------------------
# Filter builders
# ---------------------------------------------------------------------------


def _build_zone_filter(zones: List[str], field: str = "zone") -> dict:
    """Build a MongoDB zone filter."""
    if not zones:
        return {}
    return {field: {"$in": zones}}


def _build_time_filter(
    cutoff: Optional[datetime], field: str = "window_start", epoch_millis: bool = False
) -> dict:
    """Build a MongoDB time range filter.

    Args:
        cutoff: Minimum datetime (inclusive).
        field: Field name to filter on.
        epoch_millis: If True, convert cutoff to epoch milliseconds (int).
            Use this for fields stored as Flink TUMBLE window timestamps.
    """
    if cutoff is None:
        return {}
    value = cutoff
    if epoch_millis:
        value = int(cutoff.timestamp() * 1000)
    return {field: {"$gte": value}}


# ---------------------------------------------------------------------------
# Data fetching functions
# ---------------------------------------------------------------------------


def _get_ride_requests_count() -> int:
    """Get total ride requests from Kafka topic offsets (cached in session_state).

    Queries the Kafka REST API for partition offsets on first call, then caches
    the result in st.session_state for the session lifetime. Falls back to the
    JSONL source file line count if Kafka REST is unavailable.
    """
    if HAS_STREAMLIT:
        cached = st.session_state.get("_ride_requests_count")
        if cached is not None:
            return cached

    count = _fetch_ride_requests_from_kafka()
    if count is None:
        count = _count_ride_requests_jsonl()

    if HAS_STREAMLIT:
        st.session_state["_ride_requests_count"] = count
    return count


def _fetch_ride_requests_from_kafka() -> Optional[int]:
    """Query Kafka REST API for ride_requests topic message count."""
    import urllib.error
    import urllib.request

    root = _get_project_root()
    if root is None:
        return None
    creds = _load_env_defaults(root)
    rest_endpoint = creds.get("CONFLUENT_KAFKA_REST_ENDPOINT", "")
    cluster_id = creds.get("CONFLUENT_KAFKA_CLUSTER_ID", "")
    api_key = creds.get("CONFLUENT_KAFKA_API_KEY", "")
    api_secret = creds.get("CONFLUENT_KAFKA_API_SECRET", "")

    if not all([rest_endpoint, cluster_id, api_key, api_secret]):
        return None

    cred = basic_auth_token(api_key, api_secret)
    headers = {"Authorization": f"Basic {cred}"}
    topic = "ride_requests"

    url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics/{topic}/partitions"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except Exception:
        return None

    partitions = data.get("data", [])
    if not partitions:
        return 0

    total = 0
    for part in partitions:
        pid = part.get("partition_id", 0)
        for offset_type in ["earliest", "latest"]:
            offset_url = (
                f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}"
                f"/topics/{topic}/partitions/{pid}/offsets/{offset_type}"
            )
            try:
                req = urllib.request.Request(offset_url, headers=headers)
                with urllib.request.urlopen(req, timeout=5) as resp:
                    offset_data = json.loads(resp.read().decode())
                    offset_val = offset_data.get("offset", 0)
                    if offset_type == "latest":
                        total += offset_val
                    else:
                        total -= offset_val
            except Exception:
                return None
    return total


def _count_ride_requests_jsonl() -> int:
    """Fallback: count lines in the JSONL source file."""
    root = _get_project_root()
    if root is None:
        return 0
    data_file = root / "assets" / "data" / "ride_requests.jsonl"
    if not data_file.exists():
        return 0
    try:
        with open(data_file, "rb") as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def _get_collection_counts(
    client: Any, time_filter: Optional[datetime] = None
) -> Dict[str, int]:
    """Get document counts for all monitored collections.

    knowledge_base uses estimatedDocumentCount (O(1) metadata
    read) since no time filter applies. Filtered collections still call
    count_documents.

    ASP `$toDate` stages convert all window
    timestamps to BSON Date. MongoDB type-bracketed comparison means
    `$gte: <int>` against a Date field matches nothing. Pass datetime
    through unchanged.
    """
    counts = {}
    for key, (db_name, coll_name) in COLLECTIONS.items():
        try:
            coll = client[db_name][coll_name]
            if key == "knowledge_base":
                # O(1) metadata count
                counts[key] = coll.estimated_document_count()
            elif time_filter:
                time_field = {
                    "zone_traffic": "window_start",
                    "anomalies": "window_time",
                    "dispatches": "dispatched_at",
                }[key]
                filt = _build_time_filter(time_filter, field=time_field)
                counts[key] = coll.count_documents(filt)
            else:
                counts[key] = coll.estimated_document_count()
        except Exception:
            # distinguish "zero documents" from "couldn't
            # check". A timeout / auth failure must surface to the UI
            # as "—" via the None sentinel, not as a misleading 0.
            counts[key] = None
    counts["ride_requests"] = _get_ride_requests_count()
    return counts


def _fetch_zone_traffic(
    client: Any, zones: List[str], cutoff: Optional[datetime]
) -> List[dict]:
    """Fetch zone traffic data from analytics.zone_traffic."""
    db = client["analytics"]
    coll = db["zone_traffic"]
    query = {}
    query.update(_build_zone_filter(zones, field="zone"))
    # ASP stores window_start as BSON Date.
    query.update(_build_time_filter(cutoff, field="window_start"))
    try:
        docs = list(coll.find(query).sort("window_start", 1).limit(500))
        for doc in docs:
            doc.pop("_id", None)
            for field in [
                "total_revenue",
                "avg_fare",
                "total_passengers",
                "request_count",
            ]:
                if field in doc:
                    doc[field] = _convert_decimal128(doc[field])
            # Convert epoch millis to datetime for Plotly
            for ts_field in ["window_start", "window_end"]:
                if ts_field in doc and isinstance(doc[ts_field], (int, float)):
                    doc[ts_field] = datetime.fromtimestamp(
                        doc[ts_field] / 1000, tz=timezone.utc
                    )
        return docs
    except Exception:
        return []


def _fetch_anomalies(
    client: Any, zones: List[str], cutoff: Optional[datetime]
) -> List[dict]:
    """Fetch anomalies from analytics.zone_anomalies."""
    db = client["analytics"]
    coll = db["zone_anomalies"]
    query = {}
    query.update(_build_zone_filter(zones, field="pickup_zone"))
    # ASP stores window_time as BSON Date.
    query.update(_build_time_filter(cutoff, field="window_time"))
    try:
        docs = list(coll.find(query).sort("window_time", -1).limit(50))
        for doc in docs:
            doc.pop("_id", None)
            # Convert epoch millis timestamps to datetime for display
            for ts_field in ["window_time", "detected_at"]:
                if ts_field in doc and isinstance(doc[ts_field], (int, float)):
                    doc[ts_field] = datetime.fromtimestamp(
                        doc[ts_field] / 1000, tz=timezone.utc
                    )
            for field in ["request_count", "expected_requests"]:
                if field in doc:
                    doc[field] = _convert_decimal128(doc[field])
        return docs
    except Exception:
        return []


def _fetch_dispatches_for_map(
    client: Any,
    recent_window_minutes: int = 15,
    fallback_limit: int = 5,
) -> List[dict]:
    """Fetch dispatches for the Live Dispatch Map.

    Tries the recent-window query first so the map prefers dispatches that
    JUST happened (the demo intent). If that returns zero rows — which
    happens when surges fire in bursts and the user opens the dashboard
    later — falls back to the most recent N dispatches regardless of age,
    so the map is never silently empty when dispatch_log has data.

    Returned rows have epoch-millis or datetime `dispatched_at` (callers
    don't depend on type since the map only uses pickup_zone +
    dispatch_json + vessel_id).
    """
    coll = client["fleet"]["dispatch_log"]
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=recent_window_minutes)
    recent = list(
        coll.find({"dispatched_at": {"$gte": cutoff}})
        .sort("dispatched_at", -1)
        .limit(50)
    )
    if recent:
        return recent
    return list(coll.find({}).sort("dispatched_at", -1).limit(fallback_limit))


def _fetch_dispatches(client: Any, cutoff: Optional[datetime]) -> List[dict]:
    """Fetch dispatch log entries from fleet.dispatch_log."""
    db = client["fleet"]
    coll = db["dispatch_log"]
    query = _build_time_filter(cutoff, field="dispatched_at")
    try:
        docs = list(coll.find(query).sort("dispatched_at", -1).limit(50))
        for doc in docs:
            doc.pop("_id", None)
        return docs
    except Exception:
        return []


def _fetch_knowledge_base(client: Any) -> List[dict]:
    """Fetch knowledge base events from events.knowledge_base."""
    db = client["events"]
    coll = db["knowledge_base"]
    try:
        docs = list(coll.find({}, {"embedding": 0}))
        for doc in docs:
            doc.pop("_id", None)
        return docs
    except Exception:
        return []


def _fetch_vessel_home_zones(client: Any) -> Dict[str, str]:
    """Map vessel_id -> base_zone from fleet.vessel_catalog.

    base_zone is the vessel's permanent dock and the right origin for the
    animated path: current_zone gets overwritten each time the dispatch
    agent reassigns a boat, so by the time we render the trip the doc
    no longer remembers where the vessel was *before* it left.
    """
    try:
        coll = client["fleet"]["vessel_catalog"]
        return {
            doc.get("vessel_id"): doc.get("base_zone")
            for doc in coll.find({}, {"vessel_id": 1, "base_zone": 1, "_id": 0})
            if doc.get("vessel_id") and doc.get("base_zone")
        }
    except Exception:
        return {}


def _build_dispatch_trips(
    dispatches: List[dict],
    vessel_home: Dict[str, str],
    loop_ms: int = TRIPS_LOOP_MS,
    duration_ms: int = TRIPS_DURATION_MS,
) -> List[Dict[str, Any]]:
    """Convert recent dispatch_log rows into TripsLayer-ready trips.

    Each output entry has shape::

        {"path": [[lon,lat], [lon,lat]],
         "timestamps": [start_ms, end_ms],
         "vessel_id": "...",
         "destination": "..."}

    Boat starts are staggered by hashing vessel_id into the loop window
    so that simultaneous dispatches don't all begin at t=0.

    when the agent's `dispatch_json` is unparseable
    (the LLM didn't conform to the prompt's section format), fall
    back to synthesizing trips from `pickup_zone` + `vessel_home`.
    Without this, valid dispatches in `fleet.dispatch_log` would
    produce zero animations on the map.
    """
    trips: List[Dict[str, Any]] = []
    for doc in dispatches:
        dest_name = doc.get("pickup_zone")
        dest = ZONE_COORDS.get(dest_name)
        if dest is None:
            continue
        per_dispatch_trips = _build_trips_from_dispatch_json(
            doc,
            vessel_home,
            loop_ms,
            duration_ms,
        )
        if not per_dispatch_trips:
            per_dispatch_trips = _build_fallback_trips_for_dispatch(
                doc,
                vessel_home,
                loop_ms,
                duration_ms,
            )
        trips.extend(per_dispatch_trips)
    return trips


def _zone_dock(zone_name: Optional[str]) -> Optional[List[float]]:
    """Return the river-side dock coordinate for a zone, falling back
    to the zone-center coord if no dock is mapped (defensive)."""
    if zone_name is None:
        return None
    return ZONE_DOCK_COORDS.get(zone_name) or ZONE_COORDS.get(zone_name)


def _river_path_between(
    origin_zone: str, dest_zone: str
) -> Optional[List[List[float]]]:
    """Return the river-centerline polyline from origin_zone's dock to
    dest_zone's dock, including all intermediate waypoints. Boats render
    along this multi-segment path so they follow the actual Mississippi
    crescent and never visually cross land.

    Returns None if either zone has no river index. Returns a flat list
    of `[lon, lat]` points in travel order.
    """
    a = ZONE_RIVER_INDEX.get(origin_zone)
    b = ZONE_RIVER_INDEX.get(dest_zone)
    if a is None or b is None or a == b:
        return None
    if a < b:
        return [list(p) for p in RIVER_WAYPOINTS[a : b + 1]]
    return [list(p) for p in RIVER_WAYPOINTS[b : a + 1][::-1]]


def _evenly_spaced_timestamps(
    path: List[List[float]], t0: int, duration_ms: int
) -> List[int]:
    """Distribute timestamps evenly across the path's segments,
    weighted by segment length so the boat moves at constant speed
    along the river (matching the visual expectation of a real boat
    holding a steady cruising speed)."""
    if len(path) <= 1:
        return [t0]
    seg_lens: List[float] = []
    for i in range(len(path) - 1):
        a, b = path[i], path[i + 1]
        # Approximate Euclidean distance in degrees (good enough at
        # this latitude/zoom for proportional pacing).
        dx = b[0] - a[0]
        dy = b[1] - a[1]
        seg_lens.append((dx * dx + dy * dy) ** 0.5)
    total = sum(seg_lens) or 1.0
    out = [t0]
    cum = 0.0
    for length in seg_lens:
        cum += length / total
        out.append(int(t0 + cum * duration_ms))
    return out


def _make_river_trip(
    vessel_id: str,
    origin_zone: str,
    dest_zone: str,
    loop_ms: int,
    duration_ms: int,
) -> Optional[Dict[str, Any]]:
    """Build a single trip dict whose `path` follows the Mississippi
    centerline between two zones. Returns None when origin/dest are
    unmapped, identical, or the path can't be built.
    """
    if origin_zone == dest_zone:
        return None
    path = _river_path_between(origin_zone, dest_zone)
    if not path or len(path) < 2:
        return None
    t0 = hash(vessel_id) % max(1, loop_ms - duration_ms)
    timestamps = _evenly_spaced_timestamps(path, t0, duration_ms)
    return {
        "path": path,
        "timestamps": timestamps,
        "vessel_id": vessel_id,
        "destination": dest_zone,
    }


def _build_trips_from_dispatch_json(
    doc: dict,
    vessel_home: Dict[str, str],
    loop_ms: int,
    duration_ms: int,
) -> List[Dict[str, Any]]:
    """Extract trips from doc.dispatch_json. Returns [] if unparseable
    or no usable boats. Preserves the original behavior so a future
    deploy with a working agent still uses the LLM's selection."""
    dest_name = doc.get("pickup_zone")
    if dest_name not in ZONE_RIVER_INDEX:
        return []
    boats = _parse_dispatch_json(doc.get("dispatch_json", ""))
    if not isinstance(boats, list):
        return []
    out: List[Dict[str, Any]] = []
    for boat in boats:
        if not isinstance(boat, dict):
            continue
        vessel_id = boat.get("vessel_id")
        if not vessel_id:
            continue
        origin_name = vessel_home.get(vessel_id)
        if not origin_name or origin_name not in ZONE_RIVER_INDEX:
            continue
        trip = _make_river_trip(vessel_id, origin_name, dest_name, loop_ms, duration_ms)
        if trip is not None:
            out.append(trip)
    return out


def _build_fallback_trips_for_dispatch(
    doc: dict,
    vessel_home: Dict[str, str],
    loop_ms: int = TRIPS_LOOP_MS,
    duration_ms: int = TRIPS_DURATION_MS,
    max_boats: int = 3,
) -> List[Dict[str, Any]]:
    """synthesize up to `max_boats` deterministic trips
    into doc.pickup_zone from vessels whose base_zone is NOT the
    surge zone. Used when dispatch_json is unparseable.

    Vessel selection is sorted by vessel_id so the same dispatch
    always produces the same animation across re-renders.
    """
    dest_name = doc.get("pickup_zone")
    if dest_name not in ZONE_RIVER_INDEX:
        return []
    candidates = sorted(
        vid
        for vid, home in vessel_home.items()
        if home and home != dest_name and home in ZONE_RIVER_INDEX
    )
    if not candidates:
        return []
    out: List[Dict[str, Any]] = []
    for vessel_id in candidates[:max_boats]:
        origin_name = vessel_home[vessel_id]
        trip = _make_river_trip(vessel_id, origin_name, dest_name, loop_ms, duration_ms)
        if trip is not None:
            out.append(trip)
    return out


def _interpolate_boat_positions(
    trips: List[Dict[str, Any]], current_time: int
) -> List[Dict[str, Any]]:
    """Compute boat icon positions at the current playhead.

    Walks multi-segment paths so trips that follow the river's centerline
    (a list of N>=2 points) interpolate piecewise: find the segment whose
    timestamp window contains `current_time`, then linearly interpolate
    between that segment's endpoints. Also computes a heading angle so
    the icon can rotate to face its direction of travel.
    """
    import math

    icons: List[Dict[str, Any]] = []
    for trip in trips:
        path = trip.get("path") or []
        ts = trip.get("timestamps") or []
        if len(path) < 2 or len(ts) != len(path):
            continue
        t_start, t_end = ts[0], ts[-1]
        if not (t_start <= current_time <= t_end):
            continue
        # Locate the active segment by its [ts[i], ts[i+1]] window.
        seg = 0
        for i in range(len(ts) - 1):
            if ts[i] <= current_time <= ts[i + 1]:
                seg = i
                break
        a = path[seg]
        b = path[seg + 1]
        ta, tb = ts[seg], ts[seg + 1]
        denom = max(1, tb - ta)
        ratio = (current_time - ta) / denom
        x = a[0] + (b[0] - a[0]) * ratio
        y = a[1] + (b[1] - a[1]) * ratio
        # Heading in degrees, 0 = east, 90 = north (standard math angle).
        heading = math.degrees(math.atan2(b[1] - a[1], b[0] - a[0]))
        icons.append(
            {
                "position": [x, y],
                "heading": heading,
                "vessel_id": trip["vessel_id"],
                "destination": trip["destination"],
                "tooltip": f"{trip['vessel_id']} → {trip['destination']}",
            }
        )
    return icons


def _build_zone_markers(active_destinations: set) -> List[Dict[str, Any]]:
    """Build the static zone-label scatterplot data.

    Surge zones (zones receiving dispatches right now) are highlighted in
    MongoDB green; idle zones are dim grey.
    """
    markers = []
    seen = set()
    for name, coord in ZONE_COORDS.items():
        # Skip the alias entry — same coord under both "CBD" and the long form.
        if tuple(coord) in seen:
            continue
        seen.add(tuple(coord))
        is_surge = name in active_destinations or (
            name == "Central Business District (CBD)" and "CBD" in active_destinations
        )
        markers.append(
            {
                "position": coord,
                "name": name,
                # Tone down the surge highlight: the boat icons are the
                # visual hero now (REQ for actual boat sprites). The
                # zone marker is a small accent dot, never larger than
                # the boat icon. Surge zones get a green pulse via the
                # halo we paint underneath, not a giant oval.
                "color": [0, 237, 100] if is_surge else [200, 213, 222],
                "radius": 80 if is_surge else 60,
                "tooltip": name,
            }
        )
    return markers


# ---------------------------------------------------------------------------
# Data preparation helpers (pure functions, no Streamlit dependency)
# ---------------------------------------------------------------------------


def _build_kpi_data(counts: Dict[str, int]) -> List[Dict[str, Any]]:
    """Build KPI metric card data from collection counts."""
    return [
        {
            "label": "Ride Requests",
            "value": counts.get("ride_requests", 0),
            "icon": "car",
        },
        {
            "label": "Traffic Windows",
            "value": counts.get("zone_traffic", 0),
            "icon": "road",
        },
        {
            "label": "Anomalies Detected",
            "value": counts.get("anomalies", 0),
            "icon": "warning",
        },
        {
            "label": "Dispatches Logged",
            "value": counts.get("dispatches", 0),
            "icon": "truck",
        },
        {
            "label": "KB Events",
            "value": counts.get("knowledge_base", 0),
            "icon": "calendar",
        },
    ]


def _seconds_to_next_window(now: Optional[datetime] = None) -> int:
    """Seconds remaining until the next tumbling-window boundary.

    Window size is WINDOW_MINUTES (matches the Flink windowed_traffic view).
    """
    if now is None:
        now = datetime.now(timezone.utc)
    window_secs = WINDOW_MINUTES * 60
    elapsed = (now.minute % WINDOW_MINUTES) * 60 + now.second
    return window_secs - elapsed


def _build_architecture_html(counts: Dict[str, int]) -> str:
    """Build HTML/CSS pipeline architecture diagram with dark theme and glow effects."""

    def _dot(count: int) -> str:
        if count > 0:
            return (
                '<span style="display:inline-block;width:8px;height:8px;'
                "border-radius:50%;background:#00ED64;margin-right:6px;"
                "box-shadow:0 0 6px #00ED64;"
                'animation:node-pulse 2s ease-in-out infinite;"></span>'
            )
        return (
            '<span style="display:inline-block;width:8px;height:8px;'
            'border-radius:50%;background:#3d4f58;margin-right:6px;"></span>'
        )

    def _node(label: str, color: str, count: Optional[int] = None) -> str:
        badge = ""
        dot = ""
        if count is not None:
            dot = _dot(count)
            badge = (
                f' <span style="font-size:0.7em;color:{color};'
                f'font-weight:600;opacity:0.9;">{count:,}</span>'
            )
        glow = f"0 0 12px {color}33" if count and count > 0 else "none"
        return (
            f'<div style="display:inline-flex;align-items:center;'
            f"border:1px solid {color}44;border-radius:8px;"
            f"padding:6px 14px;margin:3px;font-size:0.8em;"
            f"background:rgba(12,17,23,0.8);color:#F0F4F8;"
            f"backdrop-filter:blur(8px);box-shadow:{glow};"
            f'transition:all 0.3s ease;white-space:nowrap;">'
            f"{dot}<span>{label}</span>{badge}</div>"
        )

    def _arrow() -> str:
        return (
            '<span style="display:inline-flex;align-items:center;'
            'margin:0 1px;color:rgba(0,237,100,0.4);font-size:0.9em;">→</span>'
        )

    zt = counts.get("zone_traffic", 0)
    an = counts.get("anomalies", 0)
    dp = counts.get("dispatches", 0)
    kb = counts.get("knowledge_base", 0)

    html = f"""
    <style>
    @keyframes node-pulse {{
        0%, 100% {{ opacity: 1; box-shadow: 0 0 6px #00ED64; }}
        50% {{ opacity: 0.6; box-shadow: 0 0 12px #00ED64; }}
    }}
    .pipeline-row {{
        margin: 6px 0;
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 2px;
    }}
    .pipeline-container {{
        background: rgba(12, 17, 23, 0.6);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 16px;
        padding: 20px 24px;
        margin-bottom: 16px;
        backdrop-filter: blur(12px);
        position: relative;
        overflow: hidden;
    }}
    .pipeline-container::before {{
        content: '';
        position: absolute;
        inset: 0;
        background: linear-gradient(135deg, rgba(0,237,100,0.02) 0%, transparent 50%, rgba(0,120,255,0.02) 100%);
        pointer-events: none;
    }}
    .pipeline-label {{
        font-size: 0.65em;
        color: #C8D5DE;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        margin-bottom: 4px;
        font-weight: 500;
    }}
    </style>
    <div class="pipeline-container">
    <div class="pipeline-label">Data Ingestion & Windowing</div>
    <div class="pipeline-row">
        {_node("ride_requests", COLOR_KAFKA)}
        {_arrow()}
        {_node("Flink Windowing", COLOR_FLINK)}
        {_arrow()}
        {_node("zone_traffic_sink", COLOR_KAFKA)}
        {_arrow()}
        {_node("ASP", COLOR_ASP)}
        {_arrow()}
        {_node("zone_traffic", COLOR_MONGODB, zt)}
    </div>
    <div class="pipeline-label" style="margin-top:12px;">Anomaly Detection & Agent Dispatch</div>
    <div class="pipeline-row">
        {_node("ML Detect Anomalies", COLOR_FLINK)}
        {_arrow()}
        {_node("RAG Enrichment", COLOR_FLINK)}
        {_arrow()}
        {_node("AI_RUN_AGENT", COLOR_FLINK)}
        {_arrow()}
        {_node("MCP Server", "#F59E0B")}
        {_arrow()}
        {_node("dispatch_log", COLOR_MONGODB, dp)}
    </div>
    <div class="pipeline-row" style="margin-left:24px;">
        {_node("anomalies_sink", COLOR_KAFKA)}
        {_arrow()}
        {_node("ASP", COLOR_ASP)}
        {_arrow()}
        {_node("zone_anomalies", COLOR_MONGODB, an)}
    </div>
    <div class="pipeline-label" style="margin-top:12px;">Knowledge Base (RAG Context)</div>
    <div class="pipeline-row">
        {_node("events.calendar", COLOR_MONGODB)}
        {_arrow()}
        {_node("Voyage AI Embed", COLOR_ASP)}
        {_arrow()}
        {_node("knowledge_base", COLOR_MONGODB, kb)}
        <span style="font-size:0.7em;color:#C8D5DE;margin-left:8px;">
            feeds Vector Search for anomaly context
        </span>
    </div>
    </div>
    """
    return html


def _prepare_traffic_chart_data(raw: List[dict]) -> Any:
    """Convert raw zone traffic docs to a DataFrame for Plotly."""
    if not HAS_PANDAS:
        return None
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    for col in ["total_revenue", "avg_fare", "request_count", "total_passengers"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _prepare_heatmap_data(raw: List[dict]) -> Any:
    """Aggregate zone traffic data by zone for heatmap display."""
    if not HAS_PANDAS:
        return None
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    for col in ["request_count", "total_passengers", "total_revenue"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    agg = df.groupby("zone", as_index=False).agg(
        {
            "request_count": "sum",
            "total_passengers": "sum",
            "total_revenue": "sum",
        }
    )
    return agg


def _prepare_anomaly_cards(raw: List[dict]) -> List[Dict[str, Any]]:
    """Prepare anomaly data for card-based display."""
    cards = []
    for doc in raw:
        actual = _convert_decimal128(
            doc.get("request_count", doc.get("actual_count", 0))
        )
        expected = _convert_decimal128(
            doc.get("expected_requests", doc.get("expected_count", 0))
        )
        try:
            actual = float(actual) if actual else 0
            expected = float(expected) if expected else 0
        except (ValueError, TypeError):
            actual = 0
            expected = 0
        rag_chunks = []
        for i in range(1, 4):
            chunk = doc.get(f"top_chunk_{i}")
            if chunk:
                rag_chunks.append(chunk)
        cards.append(
            {
                "zone": doc.get("pickup_zone", "Unknown"),
                "window_time": doc.get("window_time"),
                "actual_count": actual,
                "expected_count": expected,
                "surplus": actual - expected,
                "anomaly_reason": doc.get("anomaly_reason", ""),
                "rag_chunks": rag_chunks,
            }
        )
    return cards


def _prepare_dispatch_entries(raw: List[dict]) -> List[Dict[str, Any]]:
    """Prepare dispatch log entries for display."""
    entries = []
    for doc in raw:
        # dispatched_at can be null (ASP $_stream_meta.source.ts resolves
        # null for the Kafka-sourced dispatch_log_ingestion). window_time
        # is the meaningful per-window time and is always populated — use
        # it as the fallback so the card header never shows "N/A".
        ts = doc.get("dispatched_at") or doc.get("window_time")
        entries.append(
            {
                "zone": doc.get("pickup_zone", "Unknown"),
                "dispatched_at": ts,
                "summary": _clean_dispatch_summary(doc.get("dispatch_summary", "")),
                "parsed_json": _parse_dispatch_json(doc.get("dispatch_json", "")),
            }
        )
    return entries


def _prepare_kb_cards(raw: List[dict]) -> List[Dict[str, Any]]:
    """Prepare knowledge base event cards for display."""
    cards = []
    for doc in raw:
        cards.append(
            {
                "event_name": doc.get("event_name", "Unknown"),
                "zone": doc.get("zone", "Unknown"),
                "venue": doc.get("venue", ""),
                "expected_attendance": doc.get("expected_attendance", 0),
                "impact_level": doc.get("impact_level", "low"),
                "event_type": doc.get("event_type", ""),
                "description": doc.get("description", ""),
            }
        )
    return cards


# ---------------------------------------------------------------------------
# Streamlit rendering functions
# ---------------------------------------------------------------------------


def _render_sidebar(client: Optional[Any]) -> Dict[str, Any]:
    """Render sidebar controls and return filter configuration."""
    if not HAS_STREAMLIT:
        return {}

    with st.sidebar:
        st.title("Controls")
        st.caption("Filter and control the dashboard view.")

        # Connection status
        if client is not None:
            st.success("Connected to MongoDB Atlas")
        else:
            st.error("Not connected to MongoDB Atlas")

        st.divider()

        # Auto-refresh: each panel reruns its own fragment (st.fragment with
        # run_every) instead of a global script rerun. This keeps the page
        # mounted and only re-renders the data panels — no header/sidebar
        # flicker, no 2-3s "stuck" feeling.
        auto_refresh = st.toggle("Auto-refresh", value=True)
        refresh_interval = st.slider(
            "Refresh interval (seconds)", min_value=5, max_value=120, value=15
        )

        st.divider()

        # Zone filter
        all_zones = _fetch_distinct_zones(client)
        selected_zones = st.multiselect(
            "Filter by Zone",
            options=all_zones,
            default=[],
            help="Select one or more New Orleans zones to filter all panels. Leave empty to show all zones.",
        )

        # Time range
        time_range = st.selectbox(
            "Time Range",
            options=list(TIME_RANGE_OPTIONS.keys()),
            index=4,
            help="Controls how far back to query. Traffic windows are 1-minute Flink TUMBLE windows.",
        )
        td = TIME_RANGE_OPTIONS[time_range]
        cutoff = (datetime.now(timezone.utc) - td) if td else None

        st.divider()

        # Quick stats
        st.subheader("Quick Stats")
        st.caption("Live document counts from MongoDB Atlas collections.")
        if client is not None:
            # Stats fragment refreshes at the user's chosen cadence (or
            # never, if auto-refresh is off) without rerunning the rest of
            # the sidebar (which contains buttons whose state must persist).
            stats_refresh_s = int(refresh_interval) if auto_refresh else None

            # handle None sentinel returned by
            # _get_collection_counts on Mongo error. Without this, the
            # f"{None:,}" formatter on line "Ride Requests" raises
            # TypeError and the dashboard crashes mid-render.
            def _kpi_value(v) -> str:
                return "—" if v is None else f"{v:,}"

            @st.fragment(run_every=stats_refresh_s)
            def _quick_stats_fragment():
                counts = _get_collection_counts(client, cutoff)
                st.metric(
                    "Ride Requests",
                    _kpi_value(counts.get("ride_requests")),
                    help="Total ride request messages published to Kafka (from pre-generated dataset).",
                )
                st.metric(
                    "Traffic Windows",
                    _kpi_value(counts.get("zone_traffic")),
                    help="Documents in analytics.zone_traffic — 1-minute Flink TUMBLE window aggregates of ride requests per zone.",
                )
                st.metric(
                    "Anomalies",
                    _kpi_value(counts.get("anomalies")),
                    help="Documents in analytics.zone_anomalies — zones where ride requests significantly exceeded the expected baseline.",
                )
                st.metric(
                    "Dispatches",
                    _kpi_value(counts.get("dispatches")),
                    help="Documents in fleet.dispatch_log — agent-dispatched boat reassignments in response to detected anomalies.",
                )
                st.metric(
                    "KB Events",
                    _kpi_value(counts.get("knowledge_base")),
                    help="Documents in events.knowledge_base — local events (sports, concerts, festivals) with vector embeddings for RAG.",
                )

            _quick_stats_fragment()

            # Window timer — wrapped in a 1s fragment so it ticks live
            # without rerunning the whole sidebar / page.
            @st.fragment(run_every=1)
            def _window_timer_fragment():
                secs = _seconds_to_next_window()
                mins, s = divmod(secs, 60)
                st.metric(
                    "Next Window",
                    f"{mins}:{s:02d}",
                    help="Countdown to the next 1-minute Flink TUMBLE window boundary (UTC-aligned).",
                )

            _window_timer_fragment()

        st.divider()

        # ── Actions ──────────────────────────────────────
        st.subheader("Actions")
        st.caption(
            "Trigger pipeline stages interactively. In production these "
            "run automatically — buttons are for demo visibility."
        )

        # -- 1. Agent Dispatch (must run once before seeding produces dispatches) --
        st.markdown("**1. Agent Dispatch** (Flink SQL)")
        st.caption(
            "Creates the MCP tool connection, boat_dispatch_agent, and "
            "completed_actions streaming job on Confluent Cloud Flink. "
            "Only needs to run once — statements persist until destroyed."
        )
        if "agent_dispatch_results" not in st.session_state:
            st.session_state.agent_dispatch_results = []

        # Check if all agent statements are already active
        agent_all_active = False
        deploy_dispatch_active = False
        creds = _load_flink_credentials()
        _phases: list = []
        if creds is not None:
            # First, check the DEPLOY-created canonical dispatch statement. If a
            # `uv run deploy` already stood up the pipeline, its dispatch-insert
            # is RUNNING under a canonical name (not dashboard-*). In that case
            # the shared catalog objects (tool/agent/table) already exist and
            # are in use — the dashboard must NOT offer to recreate them, which
            # would DROP objects the deployed pipeline depends on.
            deploy_phase = _check_flink_statement_exists(
                creds, DEPLOY_DISPATCH_STATEMENT
            )
            deploy_dispatch_active = deploy_phase in ("RUNNING", "COMPLETED")

            for stmt_name, _ in AGENT_SQL_STEPS:
                _phases.append(_check_flink_statement_exists(creds, stmt_name))
            # The INSERT (last step) is what matters — tool/agent FAILED with
            # "already exists" is benign if the INSERT is RUNNING.
            insert_phase = _phases[-1] if _phases else None
            agent_all_active = (
                insert_phase in ("RUNNING", "COMPLETED") or deploy_dispatch_active
            )

        # Check for real failures (INSERT failed = problem)
        agent_any_failed = False
        if creds is not None and not agent_all_active:
            agent_any_failed = any(p == "FAILED" for p in _phases)

        if agent_all_active:
            if deploy_dispatch_active:
                st.success(
                    "Agent dispatch active (provisioned by `uv run deploy`) — "
                    "the shared Flink tool/agent/dispatch statements are running."
                )
            else:
                st.success("Agent dispatch active — all Flink statements running.")
        else:
            if agent_any_failed:
                failed_stmts = [
                    n for (n, _), p in zip(AGENT_SQL_STEPS, _phases) if p == "FAILED"
                ]
                st.warning(
                    f"Agent dispatch has FAILED statements: {', '.join(failed_stmts)}. Click below to retry."
                )

            if "agent_dispatch_running" not in st.session_state:
                st.session_state.agent_dispatch_running = False

            if st.session_state.agent_dispatch_running:
                # In-flight: render a fragment that polls the worker's
                # mailbox (a plain dict mutated by the background thread).
                # Because the dict is appended to from the worker thread
                # and read from the script thread, both sides see the
                # latest state without needing the worker to touch
                # st.session_state directly.
                @st.fragment(run_every=1)
                def _agent_dispatch_progress():
                    mb = st.session_state.get("agent_dispatch_mailbox") or {}
                    if mb.get("done"):
                        st.session_state.agent_dispatch_results = list(
                            mb.get("results", [])
                        )
                        st.session_state.agent_dispatch_running = False
                        st.session_state.pop("agent_dispatch_mailbox", None)
                        st.rerun()
                        return
                    completed = len(mb.get("results", []))
                    total = len(AGENT_SQL_STEPS)
                    st.markdown(
                        f":green[Submitting Flink statements... ({completed}/{total})]"
                    )

                _agent_dispatch_progress()
            elif st.button(
                "Run Agent Dispatch",
                # prevent double-click from spawning a
                # second worker thread before the rerun lands.
                disabled=st.session_state.get("agent_dispatch_running", False),
            ):
                if creds is None:
                    st.error(
                        "Cannot load Flink credentials from terraform/core/terraform.tfstate"
                    )
                else:
                    # Run the long-running submission off the Streamlit
                    # script thread so the UI stays responsive. Streamlit
                    # gates session_state access by ScriptRunContext;
                    # add_script_run_ctx() attaches the current request's
                    # context to the worker thread so its mutations to
                    # st.session_state are picked up by polling fragments.
                    import threading as _threading

                    try:
                        from streamlit.runtime.scriptrunner import add_script_run_ctx
                    except ImportError:
                        add_script_run_ctx = None  # type: ignore

                    # Snapshot session-scoped values into local variables
                    # so the worker doesn't need to read st.session_state
                    # at all (writes still go through it via the ctx).
                    _creds = creds

                    # Use a thread-safe shared mailbox written by the
                    # worker and drained by the polling fragment. This
                    # isolates the worker from session_state reads, which
                    # were the failure mode in the prior implementation.
                    mailbox = {"results": [], "done": False}
                    st.session_state["agent_dispatch_mailbox"] = mailbox

                    def _do_agent_dispatch():
                        """Recreate the agent dispatch chain.

                        Critical safety invariant: NEVER drop a catalog
                        object (TABLE/TOOL/AGENT) without an immediate
                        successful recreate, because the chain has
                        dependencies (TABLE <- TOOL <- AGENT <- INSERT)
                        and a half-applied state poisons later runs.

                        Strategy: walk the chain step-by-step. For each
                        step, if its prior statement is FAILED/STOPPED,
                        delete that statement record. Drop the catalog
                        object only just-in-time before its CREATE.
                        Stop on first error so we never half-apply.
                        """

                        def _drop_catalog_object(
                            step_name: str, drop_sql: str
                        ) -> Optional[str]:
                            """Submit a just-in-time DROP, then delete+await its
                            statement record. Returns an error string on a failed
                            DROP submission, or None on success. (No _time.sleep —
                            ; relies on _wait_for_statement_deleted.)"""
                            drop_resp = _submit_flink_sql(
                                _creds, f"{step_name}-drop", drop_sql
                            )
                            if "error" in drop_resp:
                                return drop_resp["error"]
                            _delete_flink_statement(_creds, f"{step_name}-drop")
                            _wait_for_statement_deleted(_creds, f"{step_name}-drop")
                            return None

                        try:
                            for stmt_name, sql in AGENT_SQL_STEPS:
                                phase = _check_flink_statement_exists(_creds, stmt_name)
                                if phase in ("RUNNING", "COMPLETED"):
                                    mailbox["results"].append(
                                        {
                                            "name": stmt_name,
                                            "status": "skipped",
                                            "detail": f"Already {phase}",
                                        }
                                    )
                                    continue
                                if phase in ("FAILED", "STOPPED", "DEGRADED"):
                                    _delete_flink_statement(_creds, stmt_name)
                                    # poll for actual
                                    # deletion before issuing CREATE.
                                    _wait_for_statement_deleted(_creds, stmt_name)
                                # removed redundant
                                # _time.sleep(N) calls. The
                                # _wait_for_statement_deleted helper
                                # already polls for confirmed 404; the
                                # additional fixed sleeps were
                                # belt-and-braces leftover from before
                                # the polling helper existed.
                                #
                                # Just-in-time DROP of the catalog object this
                                # step (re)creates. Track did_drop so a CREATE
                                # that 409s AFTER we dropped the object is
                                # treated as a half-applied error (the
                                # drop-but-not-recreate hazard) rather than a benign "skipped".
                                did_drop = False
                                drop_err = None
                                if "CREATE TABLE" in sql and "completed_actions" in sql:
                                    did_drop = True
                                    drop_err = _drop_catalog_object(
                                        stmt_name,
                                        "DROP TABLE IF EXISTS completed_actions",
                                    )
                                elif "CREATE AGENT" in sql:
                                    did_drop = True
                                    drop_err = _drop_catalog_object(
                                        stmt_name,
                                        "DROP AGENT IF EXISTS boat_dispatch_agent",
                                    )
                                elif "CREATE TOOL" in sql:
                                    did_drop = True
                                    drop_err = _drop_catalog_object(
                                        stmt_name, "DROP TOOL IF EXISTS mongodb_fleet"
                                    )

                                if drop_err is not None:
                                    mailbox["results"].append(
                                        {
                                            "name": stmt_name,
                                            "status": "error",
                                            "detail": f"DROP failed: {drop_err}",
                                        }
                                    )
                                    break

                                resp = _submit_flink_sql(_creds, stmt_name, sql)
                                if resp.get("already_exists"):
                                    if did_drop:
                                        # We just dropped this object; a 409 means
                                        # the CREATE did not recreate it, so the
                                        # catalog is half-applied. Stop rather than
                                        # INSERT into a missing table/agent/tool.
                                        mailbox["results"].append(
                                            {
                                                "name": stmt_name,
                                                "status": "error",
                                                "detail": "Dropped but CREATE returned 409 (half-applied) — re-run dispatch",
                                            }
                                        )
                                        break
                                    mailbox["results"].append(
                                        {
                                            "name": stmt_name,
                                            "status": "skipped",
                                            "detail": "Already exists (409)",
                                        }
                                    )
                                elif "error" in resp:
                                    mailbox["results"].append(
                                        {
                                            "name": stmt_name,
                                            "status": "error",
                                            "detail": resp["error"],
                                        }
                                    )
                                    break
                                else:
                                    mailbox["results"].append(
                                        {
                                            "name": stmt_name,
                                            "status": "submitted",
                                            "detail": "OK",
                                        }
                                    )
                        except Exception as exc:
                            mailbox["results"].append(
                                {
                                    "name": "_runtime",
                                    "status": "error",
                                    "detail": str(exc),
                                }
                            )
                        finally:
                            mailbox["done"] = True

                    st.session_state.agent_dispatch_running = True
                    st.session_state.agent_dispatch_results = []
                    t = _threading.Thread(target=_do_agent_dispatch, daemon=True)
                    if add_script_run_ctx is not None:
                        add_script_run_ctx(t)
                    t.start()
                    st.rerun()

            for r in st.session_state.agent_dispatch_results:
                if r["status"] == "submitted":
                    st.success(f"{r['name']}: {r['detail']}")
                elif r["status"] == "skipped":
                    st.info(f"{r['name']}: {r['detail']}")
                else:
                    st.error(f"{r['name']}: {r['detail']}")

        # -- 2. Seed Data --
        st.markdown("**2. Seed Data**")
        st.caption(
            "Publish the next batch of ride requests into Kafka. "
            "Each batch has escalating surge intensity (3x→12x) in French Quarter. "
            "10 batches available — auto-resets after the last one."
        )
        if "datagen_process" not in st.session_state:
            st.session_state.datagen_process = None
        if "datagen_running" not in st.session_state:
            st.session_state.datagen_running = False
        if "datagen_queue" not in st.session_state:
            # Pending follow-up subprocess specs (list of [cmd, kwargs]) to
            # run after the current process completes. Used for the first-run
            # case that needs baseline → batch_01.
            st.session_state.datagen_queue = []
        if "datagen_status" not in st.session_state:
            st.session_state.datagen_status = ""

        # subprocess completion is handled by the
        # @st.fragment(run_every=1) `_datagen_progress_fragment` below.
        # The duplicate top-level poll that lived here used to race the
        # fragment — both could pop from `datagen_queue` and call
        # `subprocess.Popen(next_cmd)`, double-spawning the queued batch.
        # Single owner: the fragment.

        # Determine current batch number. The path is duplicated from
        # scripts.common.datagen_helpers.BATCH_COUNTER_RELATIVE so the
        # dashboard, pipeline_reset, and destroy stay in sync.
        from scripts.common.datagen_helpers import BATCH_COUNTER_RELATIVE

        root = _get_project_root() or "."
        batch_counter_file = Path(root).joinpath(*BATCH_COUNTER_RELATIVE)
        current_batch = 0
        if batch_counter_file.exists():
            try:
                current_batch = int(batch_counter_file.read_text().strip())
            except (ValueError, OSError):
                current_batch = 0

        # Surge config per batch: (multiplier, zone)
        _surge_config = [
            (3, "French Quarter"),
            (4, "CBD"),
            (5, "Bywater"),
            (6, "Marigny"),
            (7, "Uptown"),
            (8, "Garden District"),
            (9, "French Quarter"),
            (10, "CBD"),
            (11, "Warehouse District"),
            (12, "French Quarter"),
        ]

        if current_batch == 0:
            next_label = "Baseline + Batch 1"
            next_mult, next_zone = _surge_config[0]
        elif current_batch < 10:
            next_label = f"Batch {current_batch + 1}"
            next_mult, next_zone = _surge_config[current_batch]
        else:
            next_label = "Reset + Batch 1"
            next_mult, next_zone = _surge_config[0]

        st.caption(f"Next: **{next_label}** — {next_mult}x surge in **{next_zone}**")
        st.progress(
            min(current_batch, 10) / 10, text=f"{current_batch}/10 batches seeded"
        )

        if st.session_state.datagen_running:
            # Live progress fragment: polls the subprocess every 1s without
            # rerunning the rest of the page. When the process completes
            # (or the queue advances), trigger a full rerun so the batch
            # counter refreshes and the "Seed Next Batch" button reappears.
            @st.fragment(run_every=1)
            def _datagen_progress_fragment():
                proc = st.session_state.datagen_process
                if proc is None:
                    # Already cleared by the top-level poll; trigger a rerun
                    # so the parent block re-renders without the spinner.
                    st.rerun()
                    return
                rc = proc.poll()
                if rc is None:
                    st.markdown(":green[Publishing ride requests...]")
                    return
                # Process finished — advance queue or finalize.
                if rc == 0 and st.session_state.datagen_queue:
                    next_cmd, next_kwargs = st.session_state.datagen_queue.pop(0)
                    st.session_state.datagen_process = subprocess.Popen(
                        next_cmd, **next_kwargs
                    )
                    st.markdown(":green[Publishing ride requests...]")
                    return
                # All done — clear state and rerun the parent block.
                st.session_state.datagen_running = False
                st.session_state.datagen_process = None
                st.session_state.datagen_queue = []
                if rc == 0:
                    st.session_state.datagen_status = "success"
                    # commit the batch counter only on
                    # subprocess success. Pending value was stashed at
                    # launch time; clear it after writing.
                    pending = st.session_state.pop("_pending_batch_num", None)
                    pending_file = st.session_state.pop(
                        "_pending_batch_counter_file", None
                    )
                    if pending is not None and pending_file:
                        try:
                            Path(pending_file).write_text(str(pending))
                        except OSError as exc:
                            st.warning(f"Could not update batch counter: {exc}")
                    # invalidate the ride_requests count
                    # cache so the KPI tile reflects the new data.
                    st.session_state.pop("_ride_requests_count", None)
                else:
                    st.session_state.datagen_status = (
                        f"error: publish_data exited with code {rc}"
                    )
                    # Drop the pending pointers on failure too.
                    st.session_state.pop("_pending_batch_num", None)
                    st.session_state.pop("_pending_batch_counter_file", None)
                st.rerun()

            _datagen_progress_fragment()
        else:
            if st.button("Seed Next Batch"):
                try:
                    root_path = Path(root)
                    data_dir = root_path / "assets" / "data"

                    if current_batch >= 10:
                        # Auto-reset: delete counter, start over
                        batch_counter_file.unlink(missing_ok=True)
                        current_batch = 0

                    popen_kwargs = {
                        "cwd": str(root_path),
                        "stdin": subprocess.DEVNULL,
                        "stdout": subprocess.DEVNULL,
                        "stderr": subprocess.DEVNULL,
                    }
                    if current_batch == 0:
                        # First run: publish the baseline only. ride_requests.jsonl
                        # IS batch 1's data (generate_batch_data regenerates it from
                        # batch 1), so also publishing batch_01.jsonl would send the
                        # identical batch twice. Advance the counter to 1 so the next
                        # click seeds batch_02.
                        baseline_file = data_dir / "ride_requests.jsonl"
                        first_cmd = [
                            sys.executable,
                            "-m",
                            "scripts.publish_data",
                            "--data-file",
                            str(baseline_file),
                            "--force",
                        ]
                        p = subprocess.Popen(first_cmd, **popen_kwargs)
                        st.session_state.datagen_queue = []
                        next_batch_num = 1
                    else:
                        # Subsequent batches
                        next_batch_num = current_batch + 1
                        batch_file = data_dir / f"batch_{next_batch_num:02d}.jsonl"
                        cmd = [
                            sys.executable,
                            "-m",
                            "scripts.publish_data",
                            "--data-file",
                            str(batch_file),
                            "--force",
                        ]
                        p = subprocess.Popen(cmd, **popen_kwargs)
                        st.session_state.datagen_queue = []

                    # do NOT increment the batch counter
                    # here — wait for the publish subprocess to exit
                    # with rc=0. If we increment immediately and publish
                    # fails (auth, topic delete mid-publish, Kafka
                    # outage), the counter falsely advances and the next
                    # click skips a batch with no data published.
                    # Store the pending batch number in session_state so
                    # the completion callback can commit it on success.
                    st.session_state["_pending_batch_num"] = next_batch_num
                    st.session_state["_pending_batch_counter_file"] = str(
                        batch_counter_file
                    )

                    st.session_state.datagen_process = p
                    st.session_state.datagen_running = True
                    st.session_state.datagen_status = ""
                    st.rerun()
                except Exception as exc:
                    st.error(f"Failed to seed data: {exc}")

        if st.session_state.datagen_status == "success":
            st.success(f"Batch {current_batch} published successfully")
        elif st.session_state.datagen_status.startswith("error"):
            st.error(st.session_state.datagen_status)

        # Seed Events — handled automatically by deploy (asp-setup seeds events.calendar).

    return {
        "zones": selected_zones,
        "cutoff": cutoff,
        "auto_refresh": auto_refresh,
        "refresh_interval": refresh_interval,
    }


def _render_live_dispatch_map(client: Any) -> None:
    """Render the live boat-dispatch animation on a New Orleans map.

    Driven by a wall-clock playhead so the animation survives Streamlit
    reruns: each refresh re-emits a deck.gl chart at the next frame,
    GPU-interpolating between frames. Latest 50 dispatches drive the
    trips; vessel base_zone provides the origin point.
    """
    if not HAS_STREAMLIT:
        return
    st.subheader("Live Dispatch Map")
    st.caption(
        "Real-time view of agent-dispatched boats. "
        "**Boat icons** mark the vessel's current position along its trip; "
        "**glowing green trails** fade behind each boat over a 30-second loop. "
        "**Zone marker dots** label each New Orleans neighborhood: "
        "**green** = active surge zone receiving dispatches now, "
        "**white** = idle."
    )

    if not HAS_PYDECK:
        st.info(
            "Install `pydeck` to enable the live dispatch map: "
            "`uv add pydeck` (or `pip install pydeck`)."
        )
        return

    import time as _time

    # try the last 15 minutes first; if no dispatches
    # land in that window (surges happen in bursts, user may open the dashboard
    # later), widen to the most recent N regardless of age so the map is never
    # silently empty when dispatch_log has data.
    dispatches = _fetch_dispatches_for_map(
        client, recent_window_minutes=15, fallback_limit=5
    )
    vessel_home = _fetch_vessel_home_zones(client)
    trips = _build_dispatch_trips(dispatches, vessel_home)

    # Wall-clock playhead, modulo the loop window. A fresh render lands
    # on the same frame that the previous render *would* have computed,
    # so the animation appears continuous across Streamlit reruns.
    current_time = int(_time.time() * 1000) % TRIPS_LOOP_MS
    icons = _interpolate_boat_positions(trips, current_time)
    active_destinations = {t["destination"] for t in trips}
    zone_markers = _build_zone_markers(active_destinations)

    layers = [
        pdk.Layer(
            "ScatterplotLayer",
            data=zone_markers,
            get_position="position",
            get_fill_color="color",
            get_radius="radius",
            radius_min_pixels=3,
            radius_max_pixels=7,
            pickable=True,
            opacity=0.75,
        ),
        pdk.Layer(
            "TextLayer",
            data=zone_markers,
            get_position="position",
            get_text="name",
            get_size=11,
            get_color=[240, 244, 248, 200],
            get_alignment_baseline="'top'",
            get_pixel_offset=[0, 14],
            font_family="Euclid Circular A, sans-serif",
        ),
        pdk.Layer(
            "TripsLayer",
            data=trips,
            get_path="path",
            get_timestamps="timestamps",
            get_color=[0, 237, 100],
            opacity=0.9,
            width_min_pixels=4,
            rounded=True,
            trail_length=TRIPS_TRAIL_MS,
            current_time=current_time,
        ),
    ]
    if icons:
        # Render boat sprites at trip leading edges. deck.gl's IconLayer
        # rejected our SVG data-URI silently in some browsers; using a
        # PNG file shipped under assets/ is reliable. Encode as
        # data:image/png;base64 so there's no HTTP fetch in the
        # browser (the dashboard still works offline).
        # cached via @st.cache_resource so we don't
        # re-read + re-encode + re-stream 40KB to the browser on every
        # 1-second fragment tick (~3.6MB/min of redundant traffic).
        boat_uri = _get_boat_data_uri()

        if boat_uri:
            for icon in icons:
                icon["icon"] = {
                    "url": boat_uri,
                    "width": 128,
                    "height": 128,
                    "anchorY": 64,
                    "anchorX": 64,
                    "mask": False,
                }
            layers.append(
                pdk.Layer(
                    "IconLayer",
                    data=icons,
                    get_icon="icon",
                    get_position="position",
                    get_angle="heading",
                    get_size=4,
                    size_scale=10,
                    size_min_pixels=22,
                    size_max_pixels=44,
                    pickable=True,
                    billboard=False,
                )
            )
        else:
            # Fallback: bright dot if the icon asset is missing.
            layers.append(
                pdk.Layer(
                    "ScatterplotLayer",
                    data=icons,
                    get_position="position",
                    get_fill_color=[0, 237, 100],
                    get_line_color=[10, 31, 18],
                    line_width_min_pixels=2,
                    get_radius=120,
                    radius_min_pixels=8,
                    radius_max_pixels=14,
                    stroked=True,
                    pickable=True,
                )
            )

    # Center the view on the midpoint of the river crescent. Zoom out
    # enough that the full Industrial-Canal-to-Audubon arc is visible
    # (lon range ~-90.13 to -90.015, lat range ~29.917 to 29.976).
    deck = pdk.Deck(
        map_style=pdk.map_styles.CARTO_DARK,
        initial_view_state=pdk.ViewState(
            latitude=29.945,
            longitude=-90.075,
            zoom=11.4,
            pitch=30,
            bearing=0,
        ),
        layers=layers,
        tooltip={"text": "{tooltip}"},
    )
    st.pydeck_chart(deck, width="stretch")

    # Status line under the map
    in_flight = len(icons)
    if trips:
        st.caption(
            f"{in_flight} boat{'s' if in_flight != 1 else ''} in flight · "
            f"{len(trips)} active trail{'s' if len(trips) != 1 else ''} · "
            f"{TRIPS_LOOP_MS // 1000}s loop"
        )
    else:
        st.caption(
            "No recent dispatches. Once the agent dispatches boats in response "
            "to a surge, you'll see them animate from their base zones to the "
            "surge zone here."
        )


def _render_kpi_row(counts: Dict[str, int]) -> None:
    """Render the 4-metric KPI row."""
    if not HAS_STREAMLIT:
        return
    kpi = _build_kpi_data(counts)
    cols = st.columns(4)
    for col, metric in zip(cols, kpi):
        # None means the count couldn't be fetched (Mongo
        # timeout, auth failure). Render as "—" with help text rather
        # than a misleading 0.
        v = metric["value"]
        if v is None:
            col.metric(
                label=metric["label"],
                value="—",
                help="Could not query this collection — check the Atlas connection.",
            )
        else:
            col.metric(label=metric["label"], value=v)


def _render_architecture_diagram(counts: Dict[str, int]) -> None:
    """Render the pipeline architecture diagram."""
    if not HAS_STREAMLIT:
        return
    st.subheader("Pipeline Architecture")
    st.caption(
        "Data flows left to right: ShadowTraffic → Kafka → Flink (windowing, "
        "anomaly detection, agent dispatch) → MongoDB Atlas (via sink connectors "
        "and Atlas Stream Processing). Live counts update as data moves through "
        "each stage."
    )
    html = _build_architecture_html(counts)
    st.markdown(html, unsafe_allow_html=True)


def _render_zone_traffic(data: List[dict]) -> None:
    """Render the zone traffic time-series chart."""
    if not HAS_STREAMLIT:
        return
    st.subheader("Zone Traffic")
    st.caption(
        "Time-series view of ride requests per zone. Each line represents a "
        "Flink tumbling-window aggregate written to analytics.zone_traffic via "
        "the MongoDB Flink sink connector."
    )
    if not data:
        st.info(EMPTY_STATE_MESSAGES["zone_traffic"])
        return
    df = _prepare_traffic_chart_data(data)
    if df is None or df.empty:
        st.info(EMPTY_STATE_MESSAGES["zone_traffic"])
        return
    if HAS_PLOTLY:
        fig = px.line(
            df,
            x="window_start",
            y="request_count",
            color="zone",
            title="Ride Requests by Zone Over Time",
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(12,17,23,0.6)",
            font_color="#F0F4F8",
            font_family="Euclid Circular A, sans-serif",
            title_font_color="#F0F4F8",
            legend_font_color="#C8D5DE",
            xaxis=dict(
                gridcolor="rgba(255,255,255,0.04)",
                zerolinecolor="rgba(255,255,255,0.06)",
            ),
            yaxis=dict(
                gridcolor="rgba(255,255,255,0.04)",
                zerolinecolor="rgba(255,255,255,0.06)",
            ),
        )
        st.plotly_chart(fig, width="stretch")
    else:
        st.dataframe(df)


def _render_zone_heatmap(data: List[dict]) -> None:
    """Render the zone heatmap bar chart."""
    if not HAS_STREAMLIT:
        return
    st.subheader("Zone Heatmap")
    st.caption(
        "Aggregated metrics per zone — total ride requests, passengers, and revenue. "
        "Helps identify high-demand zones at a glance."
    )
    if not data:
        st.info(EMPTY_STATE_MESSAGES["zone_traffic"])
        return
    df = _prepare_heatmap_data(data)
    if df is None or df.empty:
        st.info(EMPTY_STATE_MESSAGES["zone_traffic"])
        return
    if HAS_PLOTLY:
        fig = px.bar(
            df,
            x="zone",
            y=["request_count", "total_passengers", "total_revenue"],
            barmode="group",
            title="Zone Metrics Summary",
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(12,17,23,0.6)",
            font_color="#F0F4F8",
            font_family="Euclid Circular A, sans-serif",
            title_font_color="#F0F4F8",
            legend_font_color="#C8D5DE",
            xaxis=dict(
                gridcolor="rgba(255,255,255,0.04)",
                zerolinecolor="rgba(255,255,255,0.06)",
            ),
            yaxis=dict(
                gridcolor="rgba(255,255,255,0.04)",
                zerolinecolor="rgba(255,255,255,0.06)",
            ),
        )
        st.plotly_chart(fig, width="stretch")
    else:
        st.dataframe(df)


def _render_anomalies(data: List[dict]) -> None:
    """Render the anomaly detection panel."""
    if not HAS_STREAMLIT:
        return
    st.subheader("Anomaly Detection")
    st.caption(
        "Flink's DETECT_ANOMALIES function flags zones where actual ride requests "
        "significantly exceed the expected count. Each card shows the surplus, "
        "the LLM-generated explanation, and RAG context from the knowledge base."
    )
    if not data:
        st.info(EMPTY_STATE_MESSAGES["anomalies"])
        return
    cards = _prepare_anomaly_cards(data)
    for card in cards:
        with st.expander(
            f"**{card['zone']}** — {_format_datetime(card['window_time'])} "
            f"(+{card['surplus']:.0f} surplus)",
            expanded=False,
        ):
            c1, c2, c3 = st.columns(3)
            c1.metric("Actual", f"{card['actual_count']:.0f}")
            c2.metric("Expected", f"{card['expected_count']:.0f}")
            c3.metric("Surplus", f"+{card['surplus']:.0f}")
            if card["anomaly_reason"]:
                # escape LLM output before rendering as
                # markdown. The agent's free-text could contain markdown
                # special chars or HTML that would render as live links
                # / images. Use st.text/st.write of escaped content for
                # display-as-prose, st.code for fixed-width.
                st.markdown(
                    f"**LLM Explanation:** {html.escape(str(card['anomaly_reason']))}"
                )
            if card["rag_chunks"]:
                with st.expander("RAG Context", expanded=False):
                    for i, chunk in enumerate(card["rag_chunks"], 1):
                        # chunks are operator-seeded but
                        # mutable (events.knowledge_base). A poisoned
                        # chunk could inject markdown.
                        st.markdown(f"**Chunk {i}:** {html.escape(str(chunk))}")


def _render_dispatches(data: List[dict]) -> None:
    """Render the dispatch log panel."""
    if not HAS_STREAMLIT:
        return
    st.subheader("Dispatch Log")
    st.caption(
        "Completed agent actions from the Flink AI_RUN_AGENT pipeline. "
        "Each entry shows the agent's response for an anomalous zone — "
        "ingested from Kafka into fleet.dispatch_log via Atlas Stream Processing."
    )
    if not data:
        st.info(EMPTY_STATE_MESSAGES["dispatches"])
        return
    entries = _prepare_dispatch_entries(data)
    for entry in entries:
        with st.expander(
            f"**{html.escape(str(entry['zone']))}** — {_format_datetime(entry['dispatched_at'])}",
            expanded=False,
        ):
            # agent dispatch summary is LLM-authored free text.
            # Render via st.markdown WITHOUT manual html.escape: Streamlit's
            # markdown (unsafe_allow_html=False, the default) already escapes
            # HTML/script and sanitizes dangerous URLs. The old html.escape
            # was both ineffective for its stated purpose (it does not touch
            # markdown link syntax `[ ]( )`) AND corrupted display — it
            # turned every `"` into the literal entity `&quot;`. The summary
            # is also pre-cleaned of <tool_call>/<tool_response> transcript
            # noise by _clean_dispatch_summary.
            st.markdown(str(entry["summary"] or "_(no summary)_"))
            if isinstance(entry["parsed_json"], dict):
                st.json(entry["parsed_json"])
            elif entry["parsed_json"]:
                st.code(str(entry["parsed_json"]))


def _render_knowledge_base(data: List[dict]) -> None:
    """Render the knowledge base events panel."""
    if not HAS_STREAMLIT:
        return
    st.subheader("Events")
    st.caption(
        "Local events seeded into events.knowledge_base with Voyage AI vector "
        "embeddings via Atlas ai.mongodb.com. The Flink anomaly-detection agent "
        "uses these for RAG-based explanations of traffic surges."
    )
    if not data:
        st.info(EMPTY_STATE_MESSAGES["knowledge_base"])
        return
    cards = _prepare_kb_cards(data)
    cols = st.columns(min(len(cards), 3))
    for i, card in enumerate(cards):
        col = cols[i % len(cols)]
        impact_color = IMPACT_COLORS.get(card["impact_level"], "#666")
        # every field interpolated into raw HTML is escaped.
        # The KB cards source `event_name`, `zone`, `venue`, `event_type`
        # from events.knowledge_base — operator-seeded today but mutable
        # via mongosh, future ASP changes, or a misconfigured Voyage
        # response.
        col.markdown(
            f"""
            <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(12,17,23,0.85);
                        border-radius:8px;padding:12px;margin:4px 0;
                        border-left:4px solid {impact_color};">
            <strong style="color:#F0F4F8;">{html.escape(str(card['event_name']))}</strong><br>
            <span style="color:#C8D5DE;">{html.escape(str(card['zone']))} — {html.escape(str(card['venue']))}</span><br>
            <span>Attendance: {card['expected_attendance']:,}</span><br>
            <span style="color:{impact_color};font-weight:bold;">{html.escape(str(card['impact_level']).upper())} impact</span>
            &nbsp;|&nbsp; {html.escape(str(card['event_type']))}
            </div>
            """,
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Main dashboard flow
# ---------------------------------------------------------------------------


def _run_dashboard() -> None:
    """Main Streamlit dashboard rendering function."""
    if not HAS_STREAMLIT:
        print("Error: 'streamlit' package is required. Install with: uv add streamlit")
        sys.exit(1)

    st.set_page_config(
        page_title="Streaming Agents | MongoDB + Confluent",
        page_icon="https://www.mongodb.com/assets/images/global/favicon.ico",
        layout="wide",
    )

    # Inject MongoDB dark theme
    st.markdown(MONGODB_THEME_CSS, unsafe_allow_html=True)

    # Live "DB is alive" overlay: ops ticker + surge/dispatch banners driven by
    # the SSE sidecar's Atlas change stream. Renders inside a 0-px component
    # iframe that injects a fixed overlay into the parent document; degrades to
    # an OFFLINE indicator if the sidecar is unreachable (spec REQ-E-020).
    try:
        import streamlit.components.v1 as _components

        _components.html(_render_live_overlay(_live_sse_url()), height=0)
    except Exception:
        pass  # overlay is additive — never block the dashboard (INV-001)

    # Header with MongoDB + Confluent logos
    st.markdown(
        """
        <div style="display:flex;align-items:center;gap:24px;margin-bottom:8px;">
            <div style="display:flex;align-items:center;gap:16px;">
                <svg width="220" height="55" viewBox="0 0 1102 278" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M82.3229 28.6444C71.5367 15.8469 62.2485 2.84945 60.351 0.149971C60.1512 -0.0499903 59.8515 -0.0499903 59.6518 0.149971C57.7542 2.84945 48.4661 15.8469 37.6798 28.6444C-54.9019 146.721 52.2613 226.406 52.2613 226.406L53.1601 227.006C53.959 239.303 55.9565 257 55.9565 257H59.9514H63.9463C63.9463 257 65.9438 239.403 66.7428 227.006L67.6416 226.306C67.7414 226.406 174.905 146.721 82.3229 28.6444ZM59.9514 224.706C59.9514 224.706 55.1576 220.607 53.8592 218.507V218.308L59.6518 89.7325C59.6518 89.3326 60.2511 89.3326 60.2511 89.7325L66.0436 218.308V218.507C64.7453 220.607 59.9514 224.706 59.9514 224.706Z" fill="#00ED64"/>
                    <path d="M260.501 197.588L215.845 89.2991L215.745 89H181.001V96.279H186.608C188.31 96.279 189.912 96.9771 191.114 98.1736C192.315 99.3702 192.916 100.966 192.916 102.661L191.915 211.647C191.915 215.037 189.112 217.829 185.707 217.929L180 218.029V225.208H213.843V218.029L210.338 217.929C206.934 217.829 204.13 215.037 204.13 211.647V108.943L252.792 225.208C253.492 226.903 255.094 228 256.897 228C258.699 228 260.301 226.903 261.002 225.208L308.562 111.535L309.263 211.647C309.263 215.137 306.459 217.929 302.955 218.029H299.35V225.208H339V218.029H333.593C330.189 218.029 327.385 215.137 327.285 211.747L326.985 102.76C326.985 99.2704 329.788 96.4785 333.193 96.3788L339 96.279V89H305.157L260.501 197.588Z" fill="#00ED64"/>
                    <path d="M571.869 216.136C570.764 215.04 570.162 213.546 570.162 211.754V158.369C570.162 148.21 567.151 140.242 561.127 134.565C555.205 128.888 546.973 126 536.734 126C522.378 126 511.035 131.777 503.104 143.131C503.004 143.33 502.703 143.43 502.402 143.43C502.1 143.43 501.9 143.23 501.9 142.932L498.185 128.689H491.961L476 137.753V142.732H480.116C482.023 142.732 483.629 143.23 484.734 144.226C485.838 145.222 486.44 146.716 486.44 148.808V211.654C486.44 213.447 485.838 214.941 484.734 216.036C483.629 217.132 482.124 217.729 480.317 217.729H476.301V225H513.042V217.729H509.027C507.22 217.729 505.714 217.132 504.61 216.036C503.506 214.941 502.903 213.447 502.903 211.654V170.022C502.903 164.743 504.108 159.465 506.317 154.286C508.625 149.206 512.038 144.924 516.556 141.637C521.073 138.35 526.494 136.757 532.718 136.757C539.745 136.757 545.066 138.948 548.378 143.33C551.691 147.712 553.398 153.389 553.398 160.162V211.554C553.398 213.347 552.795 214.841 551.691 215.937C550.587 217.032 549.081 217.63 547.274 217.63H543.259V224.9H580V217.63H575.985C574.479 217.829 573.073 217.231 571.869 216.136Z" fill="#00ED64"/>
                    <path d="M907.546 97.212C897.39 91.8041 886.039 89 873.792 89H826V96.3107H830.68C832.472 96.3107 834.065 97.0117 835.658 98.6141C837.152 100.116 837.948 101.819 837.948 103.621V211.379C837.948 213.181 837.152 214.884 835.658 216.386C834.165 217.888 832.472 218.689 830.68 218.689H826V226H873.792C886.039 226 897.39 223.196 907.546 217.788C917.701 212.38 925.966 204.368 931.94 194.154C937.914 183.939 941 171.621 941 157.6C941 143.58 937.914 131.362 931.94 121.047C925.866 110.632 917.701 102.62 907.546 97.212ZM921.784 157.4C921.784 170.219 919.494 181.034 915.013 189.747C910.533 198.46 904.558 204.969 897.19 209.175C889.823 213.382 881.658 215.485 872.896 215.485H863.238C861.446 215.485 859.853 214.784 858.26 213.181C856.766 211.679 855.97 209.977 855.97 208.174V106.526C855.97 104.723 856.667 103.121 858.26 101.518C859.753 100.016 861.446 99.2149 863.238 99.2149H872.896C881.658 99.2149 889.823 101.318 897.19 105.524C904.558 109.73 910.533 116.24 915.013 124.953C919.494 133.665 921.784 144.581 921.784 157.4Z" fill="#00ED64"/>
                    <path d="M1053.97 164.711C1049.55 159.603 1041.02 155.297 1030.99 152.993C1044.84 146.083 1051.96 136.369 1051.96 123.851C1051.96 117.041 1050.16 110.932 1046.54 105.724C1042.93 100.517 1037.81 96.3106 1031.29 93.4064C1024.76 90.5022 1017.13 89 1008.5 89H954.402V96.3107H958.718C960.524 96.3107 962.13 97.0117 963.736 98.614C965.242 100.116 966.045 101.819 966.045 103.621V211.379C966.045 213.181 965.242 214.884 963.736 216.386C962.231 217.888 960.524 218.689 958.718 218.689H954V226H1012.72C1021.65 226 1029.98 224.498 1037.51 221.493C1045.04 218.489 1051.06 214.083 1055.38 208.274C1059.79 202.466 1062 195.355 1062 187.143C1061.9 178.33 1059.29 170.819 1053.97 164.711ZM986.621 213.281C985.115 211.779 984.312 210.077 984.312 208.274V159.904H1012.22C1022.05 159.904 1029.58 162.407 1034.8 167.414C1040.02 172.422 1042.63 178.931 1042.63 186.943C1042.63 191.75 1041.42 196.457 1039.22 200.763C1036.91 205.17 1033.49 208.675 1028.88 211.379C1024.36 214.083 1018.74 215.485 1012.22 215.485H991.639C989.833 215.585 988.227 214.784 986.621 213.281ZM984.413 149.588V106.626C984.413 104.823 985.115 103.221 986.721 101.618C988.227 100.116 989.933 99.315 991.74 99.315H1004.99C1014.52 99.315 1021.55 101.719 1025.97 106.325C1030.38 111.032 1032.59 117.041 1032.59 124.452C1032.59 132.063 1030.48 138.172 1026.37 142.779C1022.25 147.285 1016.03 149.588 1007.8 149.588H984.413Z" fill="#00ED64"/>
                    <path d="M431.999 132.387C424.329 128.196 415.763 126 406.5 126C397.237 126 388.571 128.096 381.001 132.387C373.331 136.579 367.255 142.667 362.773 150.352C358.291 158.037 356 167.02 356 177C356 186.98 358.291 195.963 362.773 203.648C367.255 211.333 373.331 217.421 381.001 221.613C388.671 225.804 397.237 228 406.5 228C415.763 228 424.429 225.904 431.999 221.613C439.669 217.421 445.745 211.333 450.227 203.648C454.709 195.963 457 186.98 457 177C457 167.02 454.709 158.037 450.227 150.352C445.745 142.667 439.669 136.679 431.999 132.387ZM439.37 177C439.37 189.276 436.382 199.256 430.405 206.442C424.529 213.628 416.461 217.321 406.5 217.321C396.54 217.321 388.471 213.628 382.595 206.442C376.618 199.256 373.63 189.276 373.63 177C373.63 164.724 376.618 154.744 382.595 147.558C388.471 140.372 396.54 136.679 406.5 136.679C416.461 136.679 424.529 140.372 430.405 147.558C436.382 154.843 439.37 164.724 439.37 177Z" fill="#00ED64"/>
                    <path d="M784.999 132.387C777.329 128.196 768.763 126 759.5 126C750.237 126 741.571 128.096 734.001 132.387C726.331 136.579 720.255 142.667 715.773 150.352C711.291 158.037 709 167.02 709 177C709 186.98 711.291 195.963 715.773 203.648C720.255 211.333 726.331 217.421 734.001 221.613C741.671 225.804 750.237 228 759.5 228C768.763 228 777.429 225.904 784.999 221.613C792.669 217.421 798.745 211.333 803.227 203.648C807.709 195.963 810 186.98 810 177C810 167.02 807.709 158.037 803.227 150.352C798.745 142.667 792.569 136.679 784.999 132.387ZM792.37 177C792.37 189.276 789.381 199.256 783.405 206.442C777.528 213.628 769.46 217.321 759.5 217.321C749.539 217.321 741.471 213.628 735.595 206.442C729.618 199.256 726.63 189.276 726.63 177C726.63 164.624 729.618 154.744 735.595 147.558C741.471 140.372 749.539 136.679 759.5 136.679C769.46 136.679 777.528 140.372 783.405 147.558C789.282 154.843 792.37 164.724 792.37 177Z" fill="#00ED64"/>
                    <path d="M642.64 126C634.614 126 627.292 127.704 620.671 131.113C614.05 134.522 608.834 139.135 605.122 145.05C601.411 150.865 599.505 157.383 599.505 164.301C599.505 170.517 600.909 176.232 603.818 181.346C606.627 186.259 610.439 190.369 615.254 193.778L600.909 213.23C599.103 215.636 598.903 218.844 600.207 221.451C601.611 224.158 604.219 225.763 607.229 225.763H611.342C607.329 228.47 604.119 231.678 601.912 235.488C599.304 239.8 598 244.311 598 248.923C598 257.546 601.812 264.665 609.335 269.979C616.759 275.293 627.191 278 640.332 278C649.461 278 658.188 276.496 666.113 273.588C674.138 270.681 680.658 266.369 685.473 260.755C690.389 255.14 692.897 248.322 692.897 240.501C692.897 232.28 689.887 226.464 682.865 220.85C676.847 216.137 667.417 213.631 655.68 213.631H615.555C615.455 213.631 615.354 213.53 615.354 213.53C615.354 213.53 615.254 213.33 615.354 213.23L625.787 199.193C628.596 200.496 631.204 201.298 633.511 201.799C635.918 202.301 638.627 202.501 641.636 202.501C650.063 202.501 657.687 200.797 664.307 197.388C670.928 193.979 676.245 189.367 680.057 183.451C683.868 177.636 685.774 171.119 685.774 164.201C685.774 156.781 682.163 143.245 672.332 136.327C672.332 136.227 672.433 136.227 672.433 136.227L694 138.633V128.707H659.492C654.075 126.902 648.458 126 642.64 126ZM654.677 188.665C650.865 190.67 646.752 191.773 642.64 191.773C635.919 191.773 630 189.367 624.984 184.654C619.969 179.942 617.461 173.024 617.461 164.201C617.461 155.377 619.969 148.459 624.984 143.747C630 139.034 635.919 136.628 642.64 136.628C646.853 136.628 650.865 137.631 654.677 139.736C658.489 141.741 661.599 144.85 664.107 148.96C666.514 153.071 667.818 158.185 667.818 164.201C667.818 170.317 666.614 175.43 664.107 179.441C661.699 183.551 658.489 186.66 654.677 188.665ZM627.492 225.662H654.677C662.201 225.662 667.016 227.166 670.226 230.375C673.436 233.583 675.041 237.894 675.041 242.908C675.041 250.227 672.132 256.243 666.314 260.755C660.495 265.267 652.671 267.573 643.041 267.573C634.614 267.573 627.592 265.668 622.476 262.058C617.36 258.449 614.752 252.934 614.752 245.916C614.752 241.504 615.956 237.393 618.364 233.784C620.771 230.174 623.68 227.567 627.492 225.662Z" fill="#00ED64"/>
                    <path d="M1082.35 224.327C1080.37 223.244 1078.88 221.669 1077.69 219.799C1076.6 217.831 1076 215.764 1076 213.5C1076 211.236 1076.6 209.071 1077.69 207.201C1078.78 205.232 1080.37 203.756 1082.35 202.673C1084.34 201.591 1086.52 201 1089 201C1091.48 201 1093.66 201.591 1095.65 202.673C1097.63 203.756 1099.12 205.331 1100.31 207.201C1101.4 209.169 1102 211.236 1102 213.5C1102 215.764 1101.4 217.929 1100.31 219.799C1099.22 221.768 1097.63 223.244 1095.65 224.327C1093.66 225.409 1091.48 226 1089 226C1086.62 226 1084.34 225.409 1082.35 224.327ZM1094.56 222.85C1096.24 221.965 1097.44 220.587 1098.43 219.012C1099.32 217.339 1099.82 215.468 1099.82 213.402C1099.82 211.335 1099.32 209.465 1098.43 207.791C1097.53 206.118 1096.24 204.839 1094.56 203.953C1092.87 203.067 1091.08 202.575 1089 202.575C1086.92 202.575 1085.13 203.067 1083.44 203.953C1081.76 204.839 1080.56 206.217 1079.57 207.791C1078.68 209.465 1078.18 211.335 1078.18 213.402C1078.18 215.468 1078.68 217.339 1079.57 219.012C1080.47 220.685 1081.76 221.965 1083.44 222.85C1085.13 223.736 1086.92 224.228 1089 224.228C1091.08 224.228 1092.97 223.736 1094.56 222.85ZM1083.64 219.406V218.52L1083.84 218.421H1084.44C1084.63 218.421 1084.83 218.323 1084.93 218.224C1085.13 218.028 1085.13 217.929 1085.13 217.732V208.579C1085.13 208.382 1085.03 208.185 1084.93 208.087C1084.73 207.89 1084.63 207.89 1084.44 207.89H1083.84L1083.64 207.791V206.906L1083.84 206.807H1089C1090.49 206.807 1091.58 207.102 1092.47 207.791C1093.37 208.48 1093.76 209.366 1093.76 210.547C1093.76 211.433 1093.47 212.319 1092.77 212.909C1092.08 213.598 1091.28 213.992 1090.29 214.091L1091.48 214.484L1093.76 218.126C1093.96 218.421 1094.16 218.52 1094.46 218.52H1095.05L1095.15 218.618V219.504L1095.05 219.602H1091.98L1091.78 219.504L1088.6 214.189H1087.81V217.732C1087.81 217.929 1087.91 218.126 1088.01 218.224C1088.21 218.421 1088.31 218.421 1088.5 218.421H1089.1L1089.3 218.52V219.406L1089.1 219.504H1083.84L1083.64 219.406ZM1088.7 213.008C1089.5 213.008 1090.19 212.811 1090.59 212.319C1090.98 211.925 1091.28 211.236 1091.28 210.449C1091.28 209.661 1091.08 209.071 1090.69 208.579C1090.29 208.087 1089.69 207.89 1089 207.89H1088.6C1088.4 207.89 1088.21 207.988 1088.11 208.087C1087.91 208.283 1087.91 208.382 1087.91 208.579V213.008H1088.7Z" fill="#00ED64"/>
                </svg>
                <span style="color:rgba(255,255,255,0.2);font-size:1.5rem;font-weight:200;">+</span>
                <svg width="220" height="66" viewBox="112 89 1340 290" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M636.82,179.64c-4.98-4.98-10.81-8.85-17.31-11.51-6.4-2.63-13.15-3.95-20.07-3.95-10.75,0-21.1,3.06-29.99,9.05-8.81,5.92-15.6,14.24-19.64,24.07-4.05,9.83-5.07,20.53-2.98,30.97,2.09,10.42,7.16,19.9,14.68,27.39,7.51,7.49,16.99,12.54,27.39,14.57,3.41.66,6.85.99,10.27.99,7.03,0,14.01-1.41,20.57-4.17,9.77-4.1,18.02-10.97,23.88-19.83,5.84-8.87,8.91-19.17,8.85-29.81,0-7.05-1.37-13.93-4.05-20.45-2.7-6.52-6.58-12.34-11.57-17.32h-.05ZM637.79,217.44c.06,5.16-.88,10.22-2.78,15.02-1.93,4.8-4.73,9.1-8.35,12.77-3.62,3.69-7.87,6.55-12.61,8.55-4.76,1.99-9.77,3.01-14.93,3.01-7.65,0-15.04-2.25-21.41-6.52-6.37-4.25-11.29-10.24-14.24-17.32-2.95-7.08-3.73-14.79-2.29-22.33,1.45-7.53,5.05-14.39,10.43-19.87,5.37-5.46,12.16-9.18,19.65-10.74,2.62-.55,5.25-.81,7.88-.81,4.91,0,9.78.94,14.4,2.8,7.1,2.86,13.14,7.71,17.46,14.04,4.33,6.32,6.67,13.73,6.78,21.39h-.01Z" fill="#FFFFFF"/>
                    <path d="M472.38,178.05c7.72-.41,15.37,1.92,21.56,6.57,6.19,4.65,10.57,11.36,12.34,18.92l.26,1.16h14.99l-.34-1.79c-2.21-11.25-8.36-21.36-17.35-28.44-8.99-7.1-20.23-10.72-31.56-10.22-14.19,0-27.52,5.54-37.56,15.62-10.04,10.06-15.55,23.46-15.55,37.68s5.53,27.63,15.55,37.68c10.03,10.06,23.37,15.62,37.5,15.62.66.03,1.3.03,1.96.03,10.75,0,21.23-3.64,29.69-10.36,8.97-7.11,15.13-17.25,17.32-28.52l.34-1.79h-14.99l-.28,1.14c-1.79,7.54-6.19,14.26-12.36,18.92-6.17,4.66-13.83,7.03-21.61,6.67-10.42-.12-20.18-4.28-27.5-11.71-7.33-7.43-11.36-17.29-11.36-27.72s4.03-20.3,11.36-27.74c7.32-7.43,17.08-11.6,27.57-11.71l.02-.02Z" fill="#FFFFFF"/>
                    <path d="M756.56,243.03l-61.84-77.09h-11.54v102.93h14.55v-76.08l61.49,76.08h11.87v-102.93h-14.53v77.09Z" fill="#FFFFFF"/>
                    <path d="M809.76,268.88h14.55v-41.7h50.68v-13.76h-50.68v-33.75h56.77v-13.73h-71.32v102.94Z" fill="#FFFFFF"/>
                    <path d="M932.74,165.94h-14.53v102.93h69.45v-13.71h-54.91v-89.21Z" fill="#FFFFFF"/>
                    <path d="M1083.38,224.6c0,18.94-12.17,32.16-29.58,32.16s-29.54-12.92-29.54-32.16v-58.67h-14.55v58.67c0,27.54,17.73,46.04,44.09,46.04s44.12-18.51,44.12-46.04v-58.67h-14.52v58.67h-.01Z" fill="#FFFFFF"/>
                    <path d="M1135.52,268.87h72.69v-13.71h-58.12v-30.5h52.3v-13.71h-52.3v-31.26h58.12v-13.73h-72.69v102.93Z" fill="#FFFFFF"/>
                    <path d="M1313.23,243l-61.78-77.09h-11.6v102.91h14.55v-76.06l61.49,76.06h11.85v-102.91h-14.51v77.09Z" fill="#FFFFFF"/>
                    <path d="M1355.49,165.94v13.73h36.66v89.2h14.57v-89.2h36.71v-13.73h-87.93Z" fill="#FFFFFF"/>
                    <path d="M292.11,214.19c-2.01-.08-4.02-.18-6.04-.28l-18.09-.5c-9-.26-18.62-.41-31.11-.5,0-9.3-.05-20.22-.36-31.22l-.49-18.05c-.1-2.02-.2-4.04-.28-6.05-.18-4.07-.36-8.06-.63-11.99l-.03-.5h-6.04l-.03.5c-.3,4.05-.48,8.16-.63,11.99-.08,2.02-.18,4.04-.28,6.05l-.56,18.06c-.1,4.1-.17,8.24-.21,12.36l-.72-1.67c-1.35-3.16-2.73-6.42-4.2-9.66l-7.32-16.44c-.63-1.31-1.23-2.63-1.86-3.94l-1.09-2.33c-1.28-2.78-3-6.42-4.74-10.04l-.21-.46-5.61,2.37.16.48c1.33,3.99,2.8,8.01,4.2,11.89l.07.2c.59,1.62,1.19,3.26,1.78,4.91l6.4,16.86c1.37,3.52,2.81,7.21,4.53,11.45-1.27-1.24-2.55-2.5-3.83-3.74-1.65-1.6-3.31-3.23-4.95-4.85l-13.05-12.42c-1.5-1.36-2.98-2.71-4.46-4.09-3.28-3.01-6.01-5.51-8.9-8.02l-.38-.33-4.28,4.28.33.38c2.22,2.58,4.51,5.08,7.04,7.86,1.68,1.84,3.36,3.69,5.04,5.57l12.38,13.1c3.29,3.47,5.97,6.27,8.48,8.78l-.95-.38c-3.46-1.39-6.91-2.78-10.39-4.14l-16.84-6.39-4.95-1.8c-3.92-1.42-7.97-2.89-11.98-4.25l-.48-.17-2.35,5.62.44.23c3.21,1.64,6.53,3.18,9.74,4.66l.53.25c1.43.66,2.86,1.34,4.3,2l18.12,8.16c3.28,1.49,6.55,2.89,9.63,4.22l1.66.71c-4.1.05-8.23.12-12.31.21l-17.99.5c-2.01.1-4.02.2-6.02.28-4.23.18-8.05.36-11.95.63l-.49.03v6.09l.49.03c4.02.3,8.11.46,11.95.63,2.01.08,4.02.18,6.04.28l17.97.5c8.77.26,17.71.3,26.37.35h4.72c.07,10.24.13,20.83.49,31.23l.56,18.05c.1,2.02.2,4.04.28,6.05.18,4.27.36,8.12.63,11.99l.03.5h5.61l.03-.5c.3-4,.46-7.96.63-11.99.08-2.02.18-4.04.28-6.05l.56-18.03c.15-4.19.21-8.47.28-12.74l.33.78c1.53,3.52,3.09,7.18,4.76,10.87l7.39,16.44c.86,1.8,1.71,3.62,2.55,5.44,1.81,3.9,3.46,7.41,5.2,10.87l.23.45,5.17-2.18-.16-.48c-1.1-3.31-2.29-6.65-3.42-9.86l-.77-2.2c-.59-1.65-1.19-3.32-1.76-5.01l-6.34-16.86c-1.61-4.3-3.13-8.21-4.59-11.86l1,.98c2.65,2.58,5.4,5.24,8.13,7.81l13.14,12.36c1.33,1.21,2.67,2.43,4,3.66,2.77,2.55,6.04,5.52,9.38,8.4l.38.31,3.97-3.97-.33-.38c-2.68-3.11-5.46-6.19-8.15-9.15l-.39-.45c-1.15-1.26-2.3-2.53-3.46-3.82l-12.31-13.17c-2.67-2.86-5.46-5.76-7.87-8.25l-.94-.98c.97.38,1.94.78,2.91,1.16,2.98,1.19,5.96,2.37,8.95,3.51l16.82,6.37c2.09.73,4.18,1.49,6.24,2.23,3.95,1.44,7.26,2.63,10.73,3.77l.46.15,2.17-5.18-.44-.23c-3.42-1.74-6.95-3.39-10.35-5l-.48-.23c-1.81-.83-3.62-1.69-5.43-2.56l-16.33-7.36c-3.52-1.6-7.24-3.24-11.59-5.11,4.2-.05,8.48-.12,12.67-.28l17.97-.56c1.93-.1,3.85-.18,5.78-.26h.25c4.05-.18,8.02-.36,11.95-.65l.49-.03v-5.64l-.49-.03c-4.07-.3-8.13-.48-11.95-.63l-.1.03Z" fill="#FFFFFF"/>
                    <path d="M316.14,133.37c-16.69-16.76-37.76-28.06-60.92-32.67-23.18-4.63-46.94-2.28-68.75,6.78-21.81,9.07-40.28,24.27-53.4,43.97-13.14,19.72-20.06,42.65-20.06,66.35.03,31.88,12.43,61.85,34.9,84.38,22.47,22.55,52.33,34.97,84.09,35.02,23.62,0,46.47-6.96,66.1-20.12,19.64-13.17,34.8-31.69,43.83-53.58,9.04-21.89,11.37-45.74,6.77-68.98-4.61-23.24-15.87-44.38-32.57-61.14l.02-.02ZM335.13,238.38c-4.07,20.53-14.02,39.22-28.77,54.03-14.76,14.81-33.38,24.8-53.84,28.88-20.48,4.09-41.48,2-60.75-6-19.27-8.01-35.6-21.46-47.19-38.86-11.59-17.4-17.73-37.68-17.73-58.63.03-28.17,10.98-54.65,30.83-74.57,19.85-19.92,46.25-30.9,74.32-30.93,20.87,0,41.07,6.15,58.42,17.78,17.35,11.63,30.75,28.01,38.73,47.34,7.98,19.34,10.06,40.43,5.99,60.96Z" fill="#FFFFFF"/>
                    <g fill="#FFFFFF" opacity="0.7">
                        <path d="M441.4,358.95c-3.04,0-4.28-1.86-4.59-4.33h-.26c-1.13,3.25-3.97,4.95-7.68,4.95-5.62,0-8.92-3.09-8.92-8.05s3.61-7.79,11.35-7.79h5.26v-2.63c0-3.77-2.06-5.83-6.29-5.83-3.2,0-5.31,1.55-6.76,3.97l-2.48-2.32c1.44-2.84,4.64-5.21,9.44-5.21,6.4,0,10.21,3.35,10.21,8.97v14.65h3.04v3.61h-2.32ZM436.55,351.21v-4.38h-5.47c-4.69,0-6.81,1.44-6.81,4.02v1.08c0,2.63,2.06,4.13,5.26,4.13,4.07,0,7.01-2.12,7.01-4.85Z"/>
                        <path d="M449.86,358.95v-26.61h4.13v4.33h.21c1.29-2.99,3.56-4.95,7.53-4.95,5.47,0,8.92,3.71,8.92,10.16v17.07h-4.13v-16.35c0-4.74-2.06-7.17-6.03-7.17-3.3,0-6.5,1.65-6.5,5.05v18.47h-4.13Z"/>
                        <path d="M488.49,358.95v-3.61h5.05v-28.78h-5.05v-3.61h14.44v3.61h-5.05v28.78h5.05v3.61h-14.44Z"/>
                        <path d="M511.08,322.95h14.8c5.93,0,9.59,3.66,9.59,9.28s-3.46,7.27-5.83,7.63v.31c2.58.15,7.07,2.37,7.07,8.36s-3.97,10.42-9.28,10.42h-16.35v-36ZM515.42,338.52h9.95c3.4,0,5.47-1.81,5.47-5v-1.75c0-3.2-2.06-5-5.47-5h-9.95v11.76ZM515.42,355.13h10.68c3.71,0,5.98-1.96,5.98-5.57v-1.75c0-3.61-2.27-5.57-5.98-5.57h-10.68v12.9Z"/>
                        <path d="M545.02,322.95h5.78l10.32,19.39h.26l10.37-19.39h5.57v36h-4.23v-30.33h-.26l-3.04,6.03-8.61,15.68-8.61-15.68-3.04-6.03h-.26v30.33h-4.23v-36Z"/>
                        <path d="M595.73,341.15c0-12.02,5.42-18.83,14.49-18.83,5.98,0,10.11,2.89,12.38,7.89l-3.51,2.11c-1.44-3.71-4.44-6.14-8.87-6.14-6.19,0-9.85,4.9-9.85,12.28v5.36c0,7.38,3.66,11.86,9.85,11.86,4.59,0,7.74-2.58,9.18-6.5l3.45,2.17c-2.27,5.05-6.65,8.2-12.64,8.2-9.08,0-14.49-6.4-14.49-18.41Z"/>
                        <path d="M627.45,345.64c0-8.46,4.9-13.93,12.02-13.93s12.02,5.47,12.02,13.93-4.9,13.93-12.02,13.93-12.02-5.47-12.02-13.93ZM647.05,347.55v-3.82c0-5.62-3.15-8.35-7.58-8.35s-7.58,2.73-7.58,8.35v3.82c0,5.62,3.15,8.36,7.58,8.36s7.58-2.73,7.58-8.36Z"/>
                        <path d="M658.55,358.95v-26.61h4.13v4.33h.21c1.19-2.73,3.04-4.95,7.17-4.95,3.51,0,6.71,1.6,8.15,5.42h.1c.98-2.89,3.56-5.42,8.1-5.42,5.42,0,8.66,3.71,8.66,10.16v17.07h-4.13v-16.35c0-4.69-1.8-7.17-5.83-7.17-3.25,0-6.24,1.65-6.24,5.05v18.47h-4.13v-16.35c0-4.74-1.81-7.17-5.73-7.17-3.25,0-6.34,1.65-6.34,5.05v18.47h-4.13Z"/>
                        <path d="M703.84,332.33h4.13v4.33h.21c1.39-3.35,4.13-4.95,7.79-4.95,6.65,0,10.83,5.42,10.83,13.93s-4.18,13.93-10.83,13.93c-3.66,0-6.19-1.65-7.79-4.95h-.21v14.65h-4.13v-36.93ZM722.35,347.91v-4.54c0-4.74-2.89-7.94-7.53-7.94-3.76,0-6.86,2.17-6.86,5.11v9.9c0,3.46,3.09,5.42,6.86,5.42,4.64,0,7.53-3.2,7.53-7.94Z"/>
                        <path d="M753.35,358.95c-3.04,0-4.28-1.86-4.59-4.33h-.26c-1.14,3.25-3.97,4.95-7.69,4.95-5.62,0-8.92-3.09-8.92-8.05s3.61-7.79,11.35-7.79h5.26v-2.63c0-3.77-2.06-5.83-6.29-5.83-3.2,0-5.31,1.55-6.76,3.97l-2.48-2.32c1.44-2.84,4.64-5.21,9.44-5.21,6.4,0,10.21,3.35,10.21,8.97v14.65h3.04v3.61h-2.32ZM748.51,351.21v-4.38h-5.47c-4.69,0-6.81,1.44-6.81,4.02v1.08c0,2.63,2.06,4.13,5.26,4.13,4.07,0,7.01-2.12,7.01-4.85Z"/>
                        <path d="M761.81,358.95v-26.61h4.13v4.33h.21c1.29-2.99,3.56-4.95,7.53-4.95,5.47,0,8.92,3.71,8.92,10.16v17.07h-4.13v-16.35c0-4.74-2.06-7.17-6.03-7.17-3.3,0-6.5,1.65-6.5,5.05v18.47h-4.13Z"/>
                        <path d="M807.36,332.33h4.07l-11.91,32.8c-1.19,3.2-2.27,4.13-6.19,4.13h-2.12v-3.61h4.18l2.01-5.67-9.95-27.65h4.13l6.4,18.1,1.24,4.33h.26l1.44-4.33,6.45-18.1Z"/>
                    </g>
                </svg>
            </div>
        </div>
        <div style="margin-top:-4px;margin-bottom:8px;">
            <h1 style="font-family:'MongoDB Value Serif',Georgia,serif;font-size:2.2rem;
                margin:0;padding:0;line-height:1.1;color:#F0F4F8;">
                Streaming <span style="background:linear-gradient(135deg,#00ED64,#7CF5A5,#0078FF);
                -webkit-background-clip:text;-webkit-text-fill-color:transparent;
                background-clip:text;">Agents</span>
            </h1>
            <p style="font-size:0.75rem;color:#C8D5DE;margin:4px 0 0 0;letter-spacing:0.5px;">
                Real-time anomaly detection &amp; autonomous fleet dispatch
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Resolve and connect
    uri = _resolve_mongodb_uri()

    # Allow sidebar manual override
    if uri is None and HAS_STREAMLIT:
        uri = st.sidebar.text_input("MongoDB URI", placeholder="mongodb+srv://...")
        if not uri:
            uri = None

    client = _connect_mongodb(uri) if uri else None

    # Sidebar + filters
    filters = _render_sidebar(client)
    zones = filters.get("zones", [])
    cutoff = filters.get("cutoff")

    if client is None:
        st.warning(
            "Not connected to MongoDB Atlas. "
            "Provide credentials via `.env`, `terraform.tfvars`, "
            "or the sidebar input."
        )
        # Still render architecture diagram with zero counts
        _render_architecture_diagram(
            {"zone_traffic": 0, "anomalies": 0, "dispatches": 0, "knowledge_base": 0}
        )
        return

    # When auto_refresh is on, each panel reruns at the configured cadence
    # via @st.fragment(run_every=...). The map gets a faster cadence so the
    # boat animation stays smooth even when the user has set a slow data
    # refresh. When auto_refresh is off, fragments still rerun on user
    # interaction but never on a timer.
    auto_refresh = bool(filters.get("auto_refresh", True))
    refresh_s = int(filters.get("refresh_interval", 15)) if auto_refresh else None

    # The live dispatch map moved to the Mission Control HUD (served by the
    # SSE sidecar), where it animates continuously in the browser instead of
    # re-mounting on every Streamlit rerun. This page is the analytics
    # deep-dive; point presenters at the hero screen.
    st.markdown(
        f"""
        <a href="{html.escape(_live_sse_url())}" target="_blank" rel="noopener"
           style="display:flex;align-items:center;gap:12px;border:1px solid rgba(0,237,100,0.35);
                  background:rgba(0,237,100,0.06);border-radius:12px;padding:12px 18px;
                  margin:4px 0 12px 0;text-decoration:none;">
            <span style="font-size:1.1rem;">🗺️</span>
            <span style="color:#F0F4F8;font-weight:600;">Mission Control is the live webinar view</span>
            <span style="color:#C8D5DE;">— animated dispatch map, surge banners and the
                agent's reasoning, pushed in real time. Open {html.escape(_live_sse_url())}</span>
        </a>
        """,
        unsafe_allow_html=True,
    )

    @st.fragment(run_every=refresh_s)
    def _counts_and_charts_fragment():
        counts = _get_collection_counts(client, cutoff)
        _render_architecture_diagram(counts)
        _render_kpi_row(counts)
        st.divider()
        traffic_data = _fetch_zone_traffic(client, zones, cutoff)
        col1, col2 = st.columns(2)
        with col1:
            _render_zone_traffic(traffic_data)
        with col2:
            _render_zone_heatmap(traffic_data)

    @st.fragment(run_every=refresh_s)
    def _anomalies_fragment():
        anomaly_data = _fetch_anomalies(client, zones, cutoff)
        _render_anomalies(anomaly_data)

    @st.fragment(run_every=refresh_s)
    def _dispatch_kb_fragment():
        col3, col4 = st.columns(2)
        with col3:
            dispatch_data = _fetch_dispatches(client, cutoff)
            _render_dispatches(dispatch_data)
        with col4:
            kb_data = _fetch_knowledge_base(client)
            _render_knowledge_base(kb_data)

    _counts_and_charts_fragment()
    st.divider()
    _anomalies_fragment()
    st.divider()
    _dispatch_kb_fragment()


# ---------------------------------------------------------------------------
# Live "DB is alive" overlay (Path B) — no-build HTML/JS SSE client
# ---------------------------------------------------------------------------


def _live_sse_url() -> str:
    """Resolve the SSE sidecar base URL (env LIVE_SSE_URL, default 8502)."""
    return os.environ.get("LIVE_SSE_URL", "http://localhost:8502")


def _render_live_overlay(sse_url: str) -> str:
    """Return an HTML/JS island that streams live change events from the SSE
    sidecar and renders an ops ticker, surge/dispatch banners, and a
    LIVE/RECONNECTING/OFFLINE indicator.

    Embedded via st.components.v1.html, the script runs inside a sandbox
    iframe; it injects its DOM + styles into the PARENT document so the fixed
    ticker/banner span the whole app rather than a 0-px iframe. It opens the
    EventSource in the browser, so the dashboard renders fine even when the
    sidecar is down (the client flips to OFFLINE and keeps retrying). Styled
    with the MongoDB theme (REQ-E-014).
    """
    stream_url = f"{sse_url.rstrip('/')}/api/stream"
    css = """
  #live-ticker-wrap{position:fixed;left:0;right:0;bottom:0;z-index:2147483000;
    display:flex;align-items:center;gap:14px;padding:6px 14px;
    background:linear-gradient(90deg,#060A0F 0%,#0B1620 100%);
    border-top:1px solid rgba(0,237,100,0.35);
    font-size:12px;color:#C6D3DD;font-family:'JetBrains Mono',monospace;}
  #live-indicator{font-weight:700;letter-spacing:.5px;white-space:nowrap;}
  /* !important: the dashboard theme sets `p,span,label,... {color:var(--mdb-text-secondary)!important}`.
     The indicator is a <span>, so without !important that grey rule wins the
     cascade (it beats our higher-specificity rule purely on !important) and the
     LIVE/RECONNECTING/OFFLINE state colors never show. */
  /* When LIVE, the dot gently breathes so the overlay looks alive BETWEEN
     window closes (the pipeline is windowed; nothing lands in Mongo between
     windows, so without this the overlay looks frozen even when healthy).
     Driven purely by CSS + the existing SSE keepalive ping — no new data. */
  #live-indicator.live{color:#00ED64 !important;animation:livepulse 2s ease-in-out infinite;}
  #live-indicator.reconnecting{color:#fbbf24 !important;}
  #live-indicator.offline{color:#f43f5e !important;}
  #live-hint{color:#5a6b78;font-style:italic;white-space:nowrap;}
  @keyframes livepulse{0%,100%{opacity:1;}50%{opacity:0.45;}}
  #live-ticker{flex:1;overflow:hidden;white-space:nowrap;height:18px;}
  #live-ticker .row{animation:opin .35s ease-out;color:#8FA3B0;}
  #live-ticker .row b{color:#00ED64;}
  #live-counters{display:flex;gap:12px;white-space:nowrap;}
  #live-counters .ct b{color:#00ED64;}
  #live-counters .ct.total b{color:#fff;}
  #live-banner{position:fixed;top:12%;left:0;right:0;z-index:2147483000;
    display:flex;justify-content:center;pointer-events:none;}
  #live-banner .b{padding:14px 26px;border-radius:10px;font-weight:800;
    font-size:20px;letter-spacing:.5px;animation:bannerin .3s ease-out;
    box-shadow:0 8px 40px rgba(0,0,0,.5);}
  #live-banner .b.surge{background:linear-gradient(90deg,#7f1d2e,#f43f5e);color:#fff;}
  #live-banner .b.dispatch{background:linear-gradient(90deg,#064e3b,#00ED64);color:#04120b;}
  @keyframes opin{from{opacity:0;transform:translateY(6px);}to{opacity:1;transform:none;}}
  @keyframes bannerin{from{opacity:0;transform:translateY(-10px) scale(.96);}to{opacity:1;transform:none;}}
"""
    shell = """
    <div id="live-banner"></div>
    <div id="live-ticker-wrap">
      <span id="live-indicator" class="offline">● OFFLINE</span>
      <div id="live-ticker"></div>
      <span id="live-counters">
        <span class="ct">dispatch <b id="ct-dispatch">0</b></span>
        <span class="ct">anomaly <b id="ct-anomaly">0</b></span>
        <span class="ct">traffic <b id="ct-traffic">0</b></span>
        <span class="ct total">total <b id="ct-total">0</b></span>
      </span>
    </div>
"""
    return (
        """
<script>
(function(){
  var STREAM = "%STREAM_URL%";
  // Mount into the PARENT document so position:fixed spans the whole app.
  var doc = window.parent && window.parent.document ? window.parent.document : document;
  var root = doc.getElementById('live-overlay-root');
  if(!root){
    var style = doc.createElement('style'); style.textContent = %CSS%;
    doc.head.appendChild(style);
    root = doc.createElement('div'); root.id='live-overlay-root';
    root.innerHTML = %SHELL%;
    doc.body.appendChild(root);
  }
  var counts = {dispatch:0, anomaly:0, traffic:0, total:0};
  function el(id){ return doc.getElementById(id); }
  function setState(cls, label){ var i=el('live-indicator'); if(i){i.className=cls; i.textContent='● '+label;} }
  function bump(id){ var e=el(id); if(e) e.textContent=counts[id.replace('ct-','')]; }
  function pushRow(op, coll, zone){
    var t=el('live-ticker'); if(!t) return;
    // Build with textContent (never innerHTML) — op/coll/zone originate from
    // Kafka/Mongo docs and must not be interpreted as HTML (DOM-XSS safe).
    var row=doc.createElement('div'); row.className='row';
    row.appendChild(doc.createTextNode('✓ '+op+' '));
    var b=doc.createElement('b'); b.textContent=coll; row.appendChild(b);
    row.appendChild(doc.createTextNode(' · '+(zone||'')));
    t.innerHTML=''; t.appendChild(row);
  }
  function showBanner(kind, text){
    var b=el('live-banner'); if(!b) return;
    b.innerHTML='';
    var d2=doc.createElement('div'); d2.className='b '+kind; d2.textContent=text;
    b.appendChild(d2);
    setTimeout(function(){ if(el('live-banner')) el('live-banner').innerHTML=''; }, 8000);
  }
  function zoneOf(d){ return (d && (d.zone || d.pickup_zone || d.surge_zone)) || ''; }
  var lastEventTs = 0;
  function showHint(){
    // Between window closes nothing lands in Mongo, so the ticker would sit
    // empty and look frozen. On the SSE keepalive ping, if no real change has
    // arrived recently, show a subtle "listening" hint so the overlay reads as
    // alive. A real `change` event overwrites it immediately.
    var t=el('live-ticker'); if(!t) return;
    if(Date.now() - lastEventTs < 8000) return;  // a real event is fresh; don't clobber
    t.innerHTML='';
    var h=doc.createElement('span'); h.id='live-hint';
    h.textContent='listening for surges…';
    t.appendChild(h);
  }
  function connect(){
    var es;
    try { es = new EventSource(STREAM); }
    catch(e){ setState('offline','OFFLINE'); return; }
    es.addEventListener('hello', function(){ setState('live','LIVE'); showHint(); });
    es.addEventListener('ping', function(){ setState('live','LIVE'); showHint(); });
    es.addEventListener('change', function(ev){
      setState('live','LIVE');
      var d; try{ d=JSON.parse(ev.data); }catch(e){ return; }
      lastEventTs = Date.now();
      var coll=d.collection||'', op=d.operationType||'insert', zone=zoneOf(d.doc);
      counts.total++;
      if(coll==='fleet.dispatch_log'){ counts.dispatch++; bump('ct-dispatch');
        showBanner('dispatch','🚤 AGENT DISPATCHING — '+(zone||'FLEET').toUpperCase()); }
      else if(coll==='analytics.zone_anomalies'){ counts.anomaly++; bump('ct-anomaly');
        showBanner('surge','⚠ SURGE DETECTED — '+(zone||'ZONE').toUpperCase()); }
      else if(coll==='analytics.zone_traffic'){ counts.traffic++; bump('ct-traffic'); }
      bump('ct-total');
      pushRow(op, coll, zone);
    });
    es.onerror = function(){
      setState('reconnecting','RECONNECTING');
      setTimeout(function(){ if(es.readyState===2){ setState('offline','OFFLINE'); connect(); } }, 4000);
    };
  }
  connect();
})();
</script>
""".replace("%STREAM_URL%", stream_url)
        .replace("%CSS%", json.dumps(css))
        .replace("%SHELL%", json.dumps(shell))
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _find_free_port(start: int, limit: int = 20) -> int:
    """Return `start` if it is free, else the next free TCP port above it.

    Streamlit exits with code 1 ("Port N is not available") when the
    requested port is occupied — e.g. a stale dashboard left running by a
    prior `uv run deploy`. The deploy-path launcher already falls back to
    the next free port; this gives the standalone `uv run dashboard` the
    same behavior instead of a hard failure.

    Scans `start .. start+limit`. Falls back to `start` if none are free
    (Streamlit will then surface its own error, preserving prior behavior).

    Detection is connect-based (matching deploy.py's _launch_dashboard):
    a port is "in use" if a TCP connect to 127.0.0.1:port succeeds (an
    active listener answers). A bind-based probe with SO_REUSEADDR was
    too permissive — it could bind 127.0.0.1:port alongside Streamlit's
    own bind and falsely report the port free.
    """
    import socket

    for candidate in range(start, start + limit + 1):
        try:
            with socket.create_connection(("127.0.0.1", candidate), timeout=0.5):
                continue  # something is listening → occupied
        except OSError:
            return candidate  # connection refused / no listener → free
    return start


def main() -> None:
    """CLI entry point for dashboard.

    If running inside Streamlit, runs the dashboard directly.
    Otherwise, spawns `streamlit run` as a subprocess.
    """
    if _is_running_in_streamlit():
        _run_dashboard()
        return

    parser = argparse.ArgumentParser(
        prog="dashboard",
        description="Launch the Atlas-Enhanced Agents real-time dashboard.",
    )
    parser.add_argument(
        "--port", type=int, default=8501, help="Streamlit server port (default: 8501)"
    )
    args = parser.parse_args()

    if not HAS_STREAMLIT:
        print("Error: 'streamlit' package is required.")
        print("Install with: uv add streamlit plotly pandas streamlit-autorefresh")
        sys.exit(1)

    # Fall back to the next free port if the requested one is occupied
    # (e.g. a stale dashboard from a prior deploy), instead of letting
    # Streamlit hard-exit with "Port N is not available".
    port = _find_free_port(args.port)
    if port != args.port:
        print(f"[info] Port {args.port} is in use — using {port} instead.")

    script_path = Path(__file__).resolve()
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(script_path),
        "--server.port",
        str(port),
        "--server.headless",
        "true",
    ]

    print(f"Launching Dashboard on port {port}...")
    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    except subprocess.CalledProcessError as e:
        print(f"Error: Streamlit exited with code {e.returncode}")
        sys.exit(e.returncode)


# Streamlit entry point — when `streamlit run` loads this file
if __name__ == "__main__" or _is_running_in_streamlit():
    if _is_running_in_streamlit():
        _run_dashboard()
    else:
        main()
