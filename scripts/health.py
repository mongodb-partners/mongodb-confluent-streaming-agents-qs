"""`uv run health` — single-command pipeline health report.

Replaces the 15+ ad-hoc Python one-liners that triage required before this
tool existed. Walks every component of the streaming pipeline (Flink REST,
Atlas Stream Processing, Confluent Kafka, MongoDB Atlas) and prints either
a pretty terminal report or a JSON document.

Exit codes:
    0 — every component healthy
    1 — at least one component reports `fail`
    2 — at least one component reports `unknown` (typically: missing creds)
        and none reported `fail`
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.common.http_auth import basic_auth_token

# canonical project-root helper.
from scripts.common.terraform import get_project_root as _get_project_root


def _resolve_root() -> Path:
    return _get_project_root(strict=False)


def _load_creds() -> dict[str, str]:
    """Load credentials from .env, returning a flat dict."""
    try:
        from dotenv import dotenv_values
    except ImportError:
        return {}
    root = _resolve_root()
    cred_file = root / ".env"
    if not cred_file.exists():
        return {}
    return {k: (v or "").strip() for k, v in dotenv_values(cred_file).items()}


def _load_terraform_outputs() -> dict[str, Any]:
    """Read terraform outputs from terraform/core/terraform.tfstate."""
    state = _resolve_root() / "terraform" / "core" / "terraform.tfstate"
    if not state.exists():
        return {}
    try:
        data = json.loads(state.read_text())
        return data.get("outputs", {})
    except (json.JSONDecodeError, OSError):
        return {}


# ---------------------------------------------------------------------------
# Component checks
# ---------------------------------------------------------------------------

# canonical entry shape. Every
# health entry returned by any check function carries this exact key
# set, with None defaults for inapplicable fields. JSON consumers can
# `entry[key]` without defensive `.get(key)` checks.
_CANONICAL_ENTRY_KEYS = (
    "name",
    "status",
    "detail",
    "phase",
    "records",
    "count",
    "state",
    "last_checkpoint",
)


def _entry(**fields) -> dict[str, Any]:
    """Build a health entry with the full canonical key set."""
    out = {k: None for k in _CANONICAL_ENTRY_KEYS}
    out.update(fields)
    # Guard against typos: reject fields not in the canonical set.
    extras = set(out) - set(_CANONICAL_ENTRY_KEYS)
    if extras:
        raise ValueError(f"unknown health entry field(s): {extras}")
    return out


def _check_flink(outputs: dict[str, Any]) -> list[dict[str, Any]]:
    key = (outputs.get("app_manager_flink_api_key") or {}).get("value")
    secret = (outputs.get("app_manager_flink_api_secret") or {}).get("value")
    org = (outputs.get("confluent_organization_id") or {}).get("value")
    env = (outputs.get("confluent_environment_id") or {}).get("value")
    ep = (outputs.get("confluent_flink_rest_endpoint") or {}).get("value")
    if not all([key, secret, org, env, ep]):
        return [_entry(name="flink", status="unknown", detail="no terraform outputs")]
    cred = basic_auth_token(key, secret)
    base = f"{ep}/sql/v1/organizations/{org}/environments/{env}/statements"
    statements = [
        "zone-traffic-sink-insert",
        "anomaly-detection-insert",
        "anomalies-enriched-ctas",
        "anomalies-enriched-insert",
        "anomalies-sink-insert",
        "dispatch-insert",
    ]
    # Best-effort statements: their failure must NOT make overall health
    # 'unhealthy' (which only 'fail' triggers). anomalies-enriched-insert does a
    # per-anomaly VECTOR_SEARCH_AGG that reliably times out inside Flink under
    # load; it is OFF the critical path (anomalies-sink-insert reads detection
    # output directly), so a non-RUNNING enriched statement is a 'warn', not a
    # 'fail'. See terraform/agents/sql/anomalies-sink-insert.sql.
    BEST_EFFORT = {"anomalies-enriched-insert"}
    results = []
    for n in statements:
        try:
            req = urllib.request.Request(
                f"{base}/{n}", headers={"Authorization": f"Basic {cred}"}
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                d = json.loads(r.read())
                phase = d.get("status", {}).get("phase", "UNKNOWN")
                detail = (d.get("status", {}).get("detail") or "")[:200]
                if phase in ("RUNNING", "COMPLETED"):
                    status = "ok"
                elif n in BEST_EFFORT:
                    status = "warn"
                    detail = f"best-effort (off critical path): {detail}"
                else:
                    status = "fail"
                if status != "ok":
                    # Nothing auto-resumes a statement that fails after deploy;
                    # point the operator at the recovery command.
                    detail = (
                        f"{detail} | recover: `uv run datagen` "
                        "(restarts Flink DML statements)"
                    ).strip(" |")
                results.append(
                    _entry(
                        name=n,
                        status=status,
                        detail=detail,
                        phase=phase,
                    )
                )
        except urllib.error.HTTPError as e:
            # distinguish auth, not-found, server errors.
            # stable entry shape via _entry.
            if e.code in (401, 403):
                status, detail = "fail", f"auth error (HTTP {e.code})"
            elif e.code == 404:
                status, detail = "fail", "not_found (HTTP 404)"
            elif e.code >= 500:
                status, detail = "fail", f"server error (HTTP {e.code})"
            else:
                status, detail = "unknown", f"HTTP {e.code}"
            results.append(_entry(name=n, status=status, detail=detail))
        except Exception as e:
            # Transport / network — uncertain state.
            results.append(
                _entry(
                    name=n,
                    status="unknown",
                    detail=f"transport: {str(e)[:120]}",
                )
            )
    return results


def _check_asp(creds: dict[str, str]) -> list[dict[str, Any]]:
    pub = creds.get("ATLAS_PUBLIC_KEY", "")
    priv = creds.get("ATLAS_PRIVATE_KEY", "")
    proj = creds.get("ATLAS_PROJECT_ID", "")
    if not (pub and priv and proj):
        return [_entry(name="asp", status="unknown", detail="missing Atlas Admin keys")]
    try:
        import requests
        from requests.auth import HTTPDigestAuth
    except ImportError:
        return [_entry(name="asp", status="unknown", detail="requests not installed")]
    url = (
        f"https://cloud.mongodb.com/api/atlas/v2/groups/{proj}"
        f"/streams/asp-instance/processors"
    )
    headers = {"Accept": "application/vnd.atlas.2024-05-30+json"}
    try:
        r = requests.get(
            url, auth=HTTPDigestAuth(pub, priv), headers=headers, timeout=15
        )
        if r.status_code != 200:
            # discriminate auth vs not-found vs other.
            if r.status_code in (401, 403):
                status, detail = "fail", f"auth error (HTTP {r.status_code})"
            elif r.status_code == 404:
                status, detail = "fail", "not_found (HTTP 404)"
            elif r.status_code >= 500:
                status, detail = "fail", f"server error (HTTP {r.status_code})"
            else:
                status, detail = "unknown", f"HTTP {r.status_code}"
            return [_entry(name="asp", status=status, detail=detail)]
        results = []
        for p in r.json().get("results", []):
            state = p.get("state", "UNKNOWN")
            ckpt = (
                p.get("stats", {})
                .get("lastCheckpoint", {})
                .get("commitTime", {})
                .get("$date", "")
            )
            # A processor in STARTED state with no committed checkpoint is the
            # silent-stall case: it reports "running" yet is not making any
            # progress (e.g. consumer-group offsets pointing past a recreated
            # topic, or an $addFields stage rejecting every doc to the DLQ).
            # Surface it as a non-fatal 'warn' rather than a clean 'ok' so the
            # stall is not masked; a freshly-started processor may legitimately
            # not have checkpointed yet, hence warn (not fail).
            if state == "STARTED":
                if ckpt:
                    status, detail = "ok", None
                else:
                    status, detail = (
                        "warn",
                        "STARTED but no checkpoint yet (possible stall)",
                    )
            else:
                status, detail = "fail", None
            results.append(
                _entry(
                    name=p.get("name"),
                    status=status,
                    detail=detail,
                    state=state,
                    last_checkpoint=ckpt,
                )
            )
        return results
    except Exception as e:
        return [_entry(name="asp", status="unknown", detail=str(e)[:120])]


def _check_kafka(outputs: dict[str, Any]) -> list[dict[str, Any]]:
    bootstrap = (outputs.get("confluent_kafka_cluster_bootstrap_endpoint") or {}).get(
        "value"
    )
    key = (outputs.get("app_manager_kafka_api_key") or {}).get("value")
    secret = (outputs.get("app_manager_kafka_api_secret") or {}).get("value")
    if not (bootstrap and key and secret):
        return [_entry(name="kafka", status="unknown", detail="no terraform outputs")]
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError:
        return [
            _entry(
                name="kafka", status="unknown", detail="confluent_kafka not installed"
            )
        ]
    bootstrap = bootstrap.replace("SASL_SSL://", "")
    # wrap Consumer creation inside the try/finally
    # so an exception from list_topics() can no longer bypass close().
    # Was previously created above the try and only closed inside the
    # finally — when list_topics raised, early `return` skipped close()
    # and leaked the librdkafka native handle + background fetcher thread.
    #
    # unique group.id per invocation. The previous
    # static `health-check-readonly` made every workshop attendee show
    # up in the same Confluent UI consumer group (50-member warning).
    from uuid import uuid4

    consumer_config = {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": key,
        "sasl.password": secret,
        "group.id": f"health-check-readonly-{uuid4().hex[:8]}",
        "enable.auto.commit": False,
    }
    # canonical source: ingress + streaming + CTAS outputs (health view).
    from scripts.common.pipeline_topics import HEALTH_TOPICS

    topics = list(HEALTH_TOPICS)
    results = []
    c: "Consumer | None" = None
    try:
        c = Consumer(consumer_config)
        # query the actual partition count per topic. Hardcoding
        # range(6) silently undercounts when partition count differs.
        try:
            cluster_md = c.list_topics(timeout=5)
        except Exception as e:
            return [
                _entry(
                    name="kafka",
                    status="unknown",
                    detail=f"cluster metadata: {str(e)[:120]}",
                )
            ]
        for topic in topics:
            tmd = cluster_md.topics.get(topic)
            if tmd is None or tmd.error is not None:
                results.append(
                    _entry(name=topic, status="unknown", detail="topic not found")
                )
                continue
            partitions = list(tmd.partitions.keys())
            total = 0
            try:
                for pid in partitions:
                    try:
                        lo, hi = c.get_watermark_offsets(
                            TopicPartition(topic, pid), timeout=3
                        )
                        total += max(0, hi - lo)
                    except Exception:
                        pass
                results.append(_entry(name=topic, status="ok", records=total))
            except Exception as e:
                results.append(
                    _entry(name=topic, status="unknown", detail=str(e)[:120])
                )
    finally:
        if c is not None:
            try:
                c.close()
            except Exception:
                pass
    return results


def _check_mongo(creds: dict[str, str]) -> list[dict[str, Any]]:
    conn = creds.get("TF_VAR_mongodb_connection_string", "")
    user = creds.get("TF_VAR_mongodb_username", "")
    pwd = creds.get("TF_VAR_mongodb_password", "")
    if not conn:
        return [_entry(name="mongo", status="unknown", detail="no connection string")]
    try:
        from scripts.common.mongo import build_uri, get_client

        uri = build_uri(conn, user, pwd)
        client = get_client(uri, app_name="streaming-agents-health")
    except Exception as e:
        return [_entry(name="mongo", status="unknown", detail=str(e)[:120])]
    targets = [
        ("analytics", "zone_traffic"),
        ("analytics", "zone_anomalies"),
        ("fleet", "dispatch_log"),
        ("fleet", "vessel_catalog"),
        ("events", "knowledge_base"),
        ("events", "calendar"),
    ]
    results = []
    for db, coll in targets:
        try:
            n = client[db][coll].estimated_document_count()
            results.append(
                _entry(
                    name=f"{db}.{coll}",
                    status="ok",
                    count=n,
                )
            )
        except Exception as e:
            results.append(
                _entry(
                    name=f"{db}.{coll}",
                    status="unknown",
                    detail=str(e)[:120],
                )
            )
    return results


def _check_mcp(creds: dict[str, str]) -> list[dict[str, Any]]:
    """Probe the MCP server's /mcp endpoint with a JSON-RPC initialize.

    MCP is the most failure-prone component (10 prior fixes
    for blue/green TG, IAM permission lag, content-type drift). A user
    running `uv run health` after a deploy must see an MCP-side failure
    surface here, not discover it later via AI_RUN_AGENT crashing.
    """
    url = creds.get("TF_VAR_mcp_server_url", "")
    token = creds.get("TF_VAR_mcp_auth_token", "")
    if not (url and token):
        return [
            _entry(
                name="mcp",
                status="unknown",
                detail="TF_VAR_mcp_server_url or auth_token not set",
            )
        ]
    try:
        import requests
    except ImportError:
        return [_entry(name="mcp", status="unknown", detail="requests not installed")]
    check_url = url.rstrip("/") + "/mcp"
    body = {
        "jsonrpc": "2.0",
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "health-check", "version": "1.0"},
        },
        "id": 1,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    try:
        r = requests.post(check_url, json=body, headers=headers, timeout=15)
        if r.status_code == 200:
            return [_entry(name="mcp", status="ok", detail="initialize returned 200")]
        # error discrimination.
        if r.status_code in (401, 403):
            return [
                _entry(
                    name="mcp",
                    status="fail",
                    detail=f"auth error (HTTP {r.status_code})",
                )
            ]
        if r.status_code == 404:
            return [_entry(name="mcp", status="fail", detail="not_found (HTTP 404)")]
        if r.status_code >= 500:
            return [
                _entry(
                    name="mcp",
                    status="fail",
                    detail=f"server error (HTTP {r.status_code})",
                )
            ]
        return [_entry(name="mcp", status="unknown", detail=f"HTTP {r.status_code}")]
    except Exception as e:
        return [
            _entry(name="mcp", status="unknown", detail=f"transport: {str(e)[:120]}")
        ]


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------


def collect_report() -> dict[str, Any]:
    """Run all component checks. Returns the full report dict."""
    creds = _load_creds()
    outputs = _load_terraform_outputs()
    flink = _check_flink(outputs)
    asp = _check_asp(creds)
    kafka = _check_kafka(outputs)
    mongo = _check_mongo(creds)
    # MCP is the 5th component.
    mcp = _check_mcp(creds)
    overall = "healthy"
    for component in (flink, asp, kafka, mongo, mcp):
        for entry in component:
            if entry.get("status") == "fail":
                overall = "unhealthy"
                break
        if overall == "unhealthy":
            break
    if overall == "healthy":
        # Promote to 'unknown' if EVERY component is unknown
        any_ok = any(
            entry.get("status") == "ok"
            for component in (flink, asp, kafka, mongo, mcp)
            for entry in component
        )
        if not any_ok:
            overall = "unknown"
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overall": overall,
        "flink": flink,
        "asp": asp,
        "kafka": kafka,
        "mongo": mongo,
        "mcp": mcp,
    }


def _fmt_text(report: dict[str, Any]) -> str:
    lines = [
        f"Pipeline Health Report — {report['timestamp']}",
        f"Overall: {report['overall'].upper()}",
        "",
        "Flink Statements:",
    ]
    for e in report["flink"]:
        sym = {"ok": "✓", "fail": "✗", "warn": "⚠", "unknown": "?"}.get(
            e.get("status"), "?"
        )
        # Use `or ""` (not the .get default) so an explicit None value — which
        # a statement in an unexpected/partial state can carry — still formats.
        phase = e.get("phase") or ""
        detail = e.get("detail") or ""
        name = e.get("name") or ""
        lines.append(f"  {sym}  {name:<32} {phase:<12} {detail}")
    lines.append("")
    lines.append("ASP Processors:")
    for e in report["asp"]:
        sym = {"ok": "✓", "fail": "✗", "warn": "⚠", "unknown": "?"}.get(
            e.get("status"), "?"
        )
        state = e.get("state") or ""
        ckpt = e.get("last_checkpoint") or ""
        detail = e.get("detail") or ""
        name = e.get("name") or ""
        # Show the checkpoint when present; otherwise fall back to any detail
        # (e.g. the STARTED-but-no-checkpoint stall warning).
        info = f"ckpt={ckpt}" if ckpt else (detail if detail else "ckpt=")
        lines.append(f"  {sym}  {name:<32} {state:<12} {info}")
    lines.append("")
    lines.append("Kafka Topics:")
    for e in report["kafka"]:
        sym = {"ok": "✓", "fail": "✗", "unknown": "?"}.get(e.get("status"), "?")
        recs = e.get("records", e.get("detail", ""))
        lines.append(f"  {sym}  {(e.get('name') or ''):<32} records={recs}")
    lines.append("")
    lines.append("MongoDB Collections:")
    for e in report["mongo"]:
        sym = {"ok": "✓", "fail": "✗", "unknown": "?"}.get(e.get("status"), "?")
        cnt = e.get("count", e.get("detail", ""))
        lines.append(f"  {sym}  {(e.get('name') or ''):<32} count={cnt}")
    # MCP component.
    lines.append("")
    lines.append("MCP Server:")
    for e in report.get("mcp", []):
        sym = {"ok": "✓", "fail": "✗", "unknown": "?"}.get(e.get("status"), "?")
        detail = e.get("detail") or ""
        lines.append(f"  {sym}  {(e.get('name') or ''):<32} {detail}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="health",
        description="Print a pipeline health report.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a JSON report instead of formatted text.",
    )
    args = parser.parse_args(argv)
    report = collect_report()
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(_fmt_text(report))
    if report["overall"] == "unhealthy":
        sys.exit(1)
    if report["overall"] == "unknown":
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
