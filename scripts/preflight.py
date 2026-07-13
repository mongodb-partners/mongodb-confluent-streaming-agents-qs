"""Phase-aware preflight checks for `uv run deploy`.

CLI:
    uv run preflight                         # all checks
    uv run preflight --phase asp_setup       # only checks for one phase
    uv run preflight --json                  # machine-readable output
    uv run preflight --skip-network          # offline mode (skip network=True)
    uv run preflight --list-checks           # list registered checks and exit

Library:
    from scripts.preflight import run_preflight
    passed, warned, failed = run_preflight(phase="asp_setup")
"""

from __future__ import annotations
from scripts.common.http_auth import basic_auth_token

import argparse
import json
import sys
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal

from scripts.common import cli_output


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

Status = Literal["pass", "warn", "fail", "skip"]


@dataclass(frozen=True)
class CheckResult:
    status: Status
    message: str
    remediation: str | None = None


@dataclass(frozen=True)
class Check:
    name: str
    phases: tuple              # () = always-run; otherwise WORK_PHASES values
    severity: Literal["fail", "warn", "info"]
    network: bool
    # precise callable type so contributors wiring a
    # new check get a static-analysis error if they use the wrong shape.
    run: Callable[[dict], "CheckResult"]


# Hard timeout for any single check
CHECK_TIMEOUT_SECONDS: int = 10


# ---------------------------------------------------------------------------
# Registry — populated lazily so individual check implementations can live
# elsewhere if desired. For now we declare the registry here as an empty list;
# Task 8 fills it with real check implementations.
# ---------------------------------------------------------------------------

# Atlas Admin API version constants — must match scripts/asp_setup.py:90/91
# Hardcoding any other date in a check function is a bug.
_ATLAS_API_VERSION_DEFAULT = "application/vnd.atlas.2023-02-01+json"


def _is_workshop_pre_atlas_terraform(env: dict) -> bool:
    """Shared skip predicate for checks that depend on
    artifacts produced by the atlas_terraform phase.

    Returns True when:
      - TF_VAR_create_atlas_cluster=true (workshop / fresh deploy path), AND
      - DEPLOY_PHASE is unset or empty (atlas_terraform hasn't run yet)

    Checks that depend on `TF_VAR_mongodb_connection_string` or
    `ATLAS_CLUSTER_NAME` existing as a real cluster MUST use this
    predicate to return 'skip' instead of 'fail' in the workshop
    fresh-deploy case. Otherwise the deploy-startup preflight aborts
    with exit 1 BEFORE the atlas_terraform phase can produce those
    artifacts — a workshop-breaking regression.

    NOT a per-check decision: every fail-severity check that reads a
    terraform/atlas-produced value at deploy-startup must honor this
    predicate. Adding a new check that reads `TF_VAR_mongodb_*` or
    similar without consulting this helper is a bug.
    """
    create_cluster = (env.get("TF_VAR_create_atlas_cluster") or "").strip().lower() == "true"
    deploy_phase = (env.get("DEPLOY_PHASE") or "").strip()
    return create_cluster and not deploy_phase


# ---------------------------------------------------------------------------
# Check implementations
# ---------------------------------------------------------------------------

def _check_atlas_admin_auth(env: dict) -> CheckResult:
    """Probe Atlas Admin API auth via GET /api/atlas/v2/groups/{project_id}."""
    pub  = env.get("ATLAS_PUBLIC_KEY", "")
    priv = env.get("ATLAS_PRIVATE_KEY", "")
    proj = env.get("ATLAS_PROJECT_ID", "")
    if not (pub and priv and proj):
        return CheckResult(
            "fail", "Atlas admin keys missing",
            remediation="set ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY, ATLAS_PROJECT_ID in .env",
        )
    try:
        import requests
        from requests.auth import HTTPDigestAuth
    except ImportError:
        return CheckResult("warn", "requests library unavailable, cannot probe Atlas")
    url = f"https://cloud.mongodb.com/api/atlas/v2/groups/{proj}"
    try:
        # Use API_VERSION_DEFAULT (vnd.atlas.2023-02-01+json) — matches
        # scripts/asp_setup.py:90.
        resp = requests.get(
            url, auth=HTTPDigestAuth(pub, priv),
            headers={"Accept": _ATLAS_API_VERSION_DEFAULT},
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        return CheckResult(
            "fail", f"network error: {e}",
            remediation="check Atlas API status / firewall",
        )
    if resp.status_code == 200:
        return CheckResult("pass", f"project {proj} reachable")
    if resp.status_code == 401:
        return CheckResult(
            "fail", "Atlas admin keys invalid (401)",
            remediation="rotate keys at https://cloud.mongodb.com/v2#/preferences/publicApi",
        )
    if resp.status_code == 404:
        return CheckResult(
            "fail", f"project {proj} not found (404)",
            remediation="verify ATLAS_PROJECT_ID is correct",
        )
    return CheckResult(
        "fail", f"unexpected status {resp.status_code}",
        remediation=(resp.text[:200] if resp.text else None),
    )


def check_atlas_cluster_exists(env: dict) -> CheckResult:
    """Verify `ATLAS_CLUSTER_NAME` exists in the configured Atlas project.

    Catches the most common asp_setup failure mode: `.env` points at a
    cluster name that does not exist in the project (typo, deleted
    cluster, wrong project). Without this preflight, the deploy reaches
    `ensure_connections` and fails late with three near-identical 400
    errors from `atlas_cluster` + `events_dlq` + `fleet_dlq` (all
    three reference the same `clusterName`).

    Severity matrix:
      - create_atlas_cluster=true AND DEPLOY_PHASE unset
                                                 → skip (cluster will be
                                                   provisioned by
                                                   atlas_terraform phase
                                                   AFTER this preflight
                                                   runs at deploy
                                                   startup; checking
                                                   would 404 and abort
                                                   the deploy)
      - cluster present (HTTP 200)               → pass
      - cluster absent (HTTP 404), list ok       → fail; remediation
                                                   enumerates project's
                                                   actual cluster names
      - cluster absent (HTTP 404), list empty    → fail; remediation
                                                   points at
                                                   TF_VAR_create_atlas_cluster
      - cluster absent, enumeration call failed  → fail; remediation is
                                                   generic (we
                                                   distinguish "no
                                                   clusters" from
                                                   "couldn't list")
      - HTTP 401                                 → fail (defense in
                                                   depth; primary signal
                                                   is atlas_admin_auth)
      - admin keys missing                       → warn (covered by
                                                   atlas_admin_auth)
      - cluster name unset                       → fail (config error;
                                                   not transient)
      - network error / non-(200/401/404)        → warn (treated as
                                                   transient; deploy
                                                   will retry inside
                                                   AtlasAPI)

    NAMING: This function intentionally has NO leading underscore
    (unlike sibling `_check_*` helpers). It is part of
    `scripts.preflight`'s public import surface — `scripts/asp_setup.py`
    imports it as a fail-fast guard before any ASP resource is created.
    Both call sites share this implementation so messages stay
    consistent across `uv run preflight --phase asp_setup` and
    `uv run deploy`.
    """
    # workshop fresh-deploy skip — see
    # `_is_workshop_pre_atlas_terraform` docstring for rationale.
    if _is_workshop_pre_atlas_terraform(env):
        return CheckResult(
            "skip",
            "cluster will be provisioned in atlas_terraform phase; check deferred",
        )

    pub  = env.get("ATLAS_PUBLIC_KEY", "")
    priv = env.get("ATLAS_PRIVATE_KEY", "")
    proj = env.get("ATLAS_PROJECT_ID", "")
    name = env.get("ATLAS_CLUSTER_NAME", "")

    if not (pub and priv and proj):
        return CheckResult(
            "warn",
            "Atlas admin keys missing (covered by atlas_admin_auth)",
        )
    if not name:
        return CheckResult(
            "fail",
            "ATLAS_CLUSTER_NAME not set",
            remediation=(
                "set ATLAS_CLUSTER_NAME in .env to match an existing "
                "Atlas cluster, OR set TF_VAR_create_atlas_cluster=true "
                "to provision one."
            ),
        )

    try:
        import requests
        from requests.auth import HTTPDigestAuth
    except ImportError:
        return CheckResult("warn", "requests library unavailable, cannot probe Atlas")

    base = f"https://cloud.mongodb.com/api/atlas/v2/groups/{proj}"
    headers = {"Accept": _ATLAS_API_VERSION_DEFAULT}
    auth = HTTPDigestAuth(pub, priv)

    # Optimistic specific-cluster GET — single HTTP call on the pass path
    # ( guards against accidentally enumerating the
    # cluster list when the configured cluster exists).
    try:
        resp = requests.get(
            f"{base}/clusters/{name}",
            auth=auth, headers=headers, timeout=10,
        )
    except requests.exceptions.RequestException as e:
        return CheckResult(
            "warn",
            f"network error probing cluster: {e}",
            remediation="check Atlas API reachability / firewall",
        )

    if resp.status_code == 200:
        return CheckResult("pass", f"cluster {name!r} exists in project {proj}")
    if resp.status_code == 401:
        return CheckResult(
            "fail",
            "Atlas admin keys invalid (401)",
            remediation="rotate keys at https://cloud.mongodb.com/v2#/preferences/publicApi",
        )
    if resp.status_code != 404:
        return CheckResult(
            "warn",
            f"unexpected status {resp.status_code} probing cluster",
            remediation=(resp.text[:200] if getattr(resp, "text", "") else None),
        )

    # 404 path. Enumerate available clusters to produce an actionable
    # remediation. Distinguish "list returned empty"
    # from "list call itself failed" — the former points at
    # TF_VAR_create_atlas_cluster, the latter must NOT silently report
    # "no clusters" when the truth is unknown.
    available: list[str] | None = None  # None ≡ enumeration failed
    try:
        list_resp = requests.get(
            f"{base}/clusters",
            auth=auth, headers=headers, timeout=10,
        )
        if list_resp.status_code == 200:
            available = [
                c.get("name", "?")
                for c in (list_resp.json().get("results") or [])
            ]
        # Non-200 (e.g. 401 from a key revoked between calls) leaves
        # available=None; remediation will be generic.
    except Exception:
        # Network / parse error on enumeration; available stays None.
        available = None

    if available:
        return CheckResult(
            "fail",
            f"cluster {name!r} not found in project {proj}",
            remediation=(
                f"available clusters: {', '.join(available)}. "
                "Update ATLAS_CLUSTER_NAME in .env (and "
                "TF_VAR_mongodb_connection_string to match), OR set "
                "TF_VAR_create_atlas_cluster=true to provision a fresh "
                "M10 via Terraform."
            ),
        )
    if available == []:
        return CheckResult(
            "fail",
            f"cluster {name!r} not found and project {proj} has no clusters",
            remediation=(
                "create the cluster via the Atlas UI, OR set "
                "TF_VAR_create_atlas_cluster=true in .env and re-run "
                "`uv run deploy` to provision an M10 via Terraform."
            ),
        )
    # available is None ≡ couldn't enumerate
    return CheckResult(
        "fail",
        f"cluster {name!r} not found in project {proj} (could not list "
        "remaining clusters)",
        remediation=(
            "verify ATLAS_CLUSTER_NAME and ATLAS_PROJECT_ID, then re-run. "
            "If the cluster genuinely does not exist, set "
            "TF_VAR_create_atlas_cluster=true to provision one."
        ),
    )


# Backwards-compat shim: a follow-up commit renamed the function
# to drop the leading underscore. The old
# private name is kept as an alias so external callers (monkeypatchers
# in tests, third-party scripts) don't break. Remove after a release.
_check_atlas_cluster_exists = check_atlas_cluster_exists


def _check_mongodb_uri_format(env: dict) -> CheckResult:
    """Validate the connection string parses as a mongodb URI.

    Uses pymongo's `parse_uri(..., validate=True, warn=False, normalize=False)`
    to check SYNTAX only — DNS resolution for mongodb+srv:// happens at
    runtime in `_check_mongodb_reachable`, not here. A bare host without
    scheme is considered invalid.

    DNS errors are suppressed (treated as `pass` for the
    syntax check) ONLY when the `mongodb_reachable` check will also run
    later in the same preflight pass to catch the actual reachability.
    When `skip_network=True` removes that follow-up check, a DNS error
    is reported as `warn` so a typo'd SRV hostname doesn't silently
    pass and the operator notices before the deploy fails minutes in.

    Workshop fresh-deploy skip. In the
    `TF_VAR_create_atlas_cluster=true` path, the URI is populated by
    `_persist_atlas_cluster_connection_string` AFTER the atlas_terraform
    phase runs — which is AFTER `run_preflight` is called at deploy
    startup. Returning `fail` here would abort the deploy before the
    URI could exist. See `_is_workshop_pre_atlas_terraform` and
    .007.
    """
    if _is_workshop_pre_atlas_terraform(env):
        return CheckResult(
            "skip",
            "URI will be populated by atlas_terraform phase; check deferred",
        )
    uri = env.get("TF_VAR_mongodb_connection_string", "")
    if not uri:
        return CheckResult(
            "fail", "TF_VAR_mongodb_connection_string not set",
            remediation="run `uv run deploy` interactive flow to capture",
        )
    if not (uri.startswith("mongodb://") or uri.startswith("mongodb+srv://")):
        return CheckResult(
            "fail", "URI must start with mongodb:// or mongodb+srv://",
            remediation="example: mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/",
        )
    try:
        from pymongo.uri_parser import parse_uri
    except ImportError:
        return CheckResult("warn", "pymongo not installed, skipping deep parse")
    try:
        parse_uri(uri)
    except Exception as e:
        msg = str(e)
        is_dns_error = (
            "DNS" in msg or "SRV" in msg or "_mongodb._tcp" in msg
        )
        if is_dns_error:
            # When the reachability check is also running, defer; when
            # it's skipped, surface as warn so the operator notices.
            skip_network_active = env.get("_preflight_skip_network", False)
            if skip_network_active:
                return CheckResult(
                    "warn",
                    f"URI may be reachable but SRV DNS lookup failed: {msg[:200]}",
                    remediation=(
                        "Re-run preflight without --skip-network to verify, "
                        "or fix the SRV hostname in .env."
                    ),
                )
            return CheckResult(
                "pass", "URI syntax valid (DNS resolution deferred to reachability check)",
            )
        return CheckResult(
            "fail", f"invalid URI: {e}",
            remediation="must be mongodb+srv://user:pass@host/  or mongodb://...",
        )
    return CheckResult("pass", "URI parses cleanly")


def _check_mongodb_reachable(env: dict) -> CheckResult:
    """Run db.adminCommand('ping') against the configured connection string."""
    uri = env.get("TF_VAR_mongodb_connection_string", "")
    if not uri:
        return CheckResult(
            "warn", "no connection string set, skipping reachability check",
        )
    try:
        from scripts.common.mongo import build_uri, get_client
    except ImportError:
        return CheckResult("warn", "mongo helper unavailable, skipping")
    try:
        full_uri = build_uri(
            uri,
            env.get("TF_VAR_mongodb_username", ""),
            env.get("TF_VAR_mongodb_password", ""),
        )
        client = get_client(full_uri, app_name="streaming-agents-preflight")
        client.admin.command("ping")
    except Exception as e:
        return CheckResult(
            "warn", f"ping failed: {type(e).__name__}: {e}",
            remediation="check Atlas IP allowlist + cluster availability",
        )
    return CheckResult("pass", "ping succeeded")


def _check_flink_rest_reachable(env: dict) -> CheckResult:
    """Probe the Flink REST endpoint.

    404 is NOT accepted as pass. The Flink REST root
    serves an OpenAPI response (200) or auth-required (401/403). A
    404 means the user typo'd `CONFLUENT_FLINK_REST_ENDPOINT` to an
    unrelated HTTP server (corporate proxy 404, static-site host),
    which previously slipped through as "reachable".
    """
    endpoint = env.get("CONFLUENT_FLINK_REST_ENDPOINT", "")
    if not endpoint:
        return CheckResult(
            "warn", "CONFLUENT_FLINK_REST_ENDPOINT not set (terraform hasn't run yet?)",
        )
    try:
        import requests
    except ImportError:
        return CheckResult("warn", "requests library unavailable")
    url = endpoint.rstrip("/")
    try:
        resp = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        return CheckResult(
            "fail", f"connection error: {e}",
            remediation="check CONFLUENT_FLINK_REST_ENDPOINT and network reachability",
        )
    # 200 / 401 / 403 indicate a real Flink REST endpoint. 404 means
    # the URL is wrong (typo'd to an unrelated server).
    if resp.status_code in (200, 401, 403):
        return CheckResult("pass", f"endpoint reachable (HTTP {resp.status_code})")
    if resp.status_code == 404:
        return CheckResult(
            "fail", "endpoint returned 404 — not a Flink REST API",
            remediation="verify CONFLUENT_FLINK_REST_ENDPOINT in .env "
                        "(expected https://flink.<region>.<provider>.confluent.cloud)",
        )
    return CheckResult(
        "warn", f"unexpected status {resp.status_code}",
        remediation="endpoint may still be reachable; check Confluent status",
    )


def _check_kafka_rest_reachable(env: dict) -> CheckResult:
    """Probe the Kafka REST v3 endpoint.

    prefer a path-aware probe (`/kafka/v3/clusters/{id}`)
    when `CONFLUENT_KAFKA_CLUSTER_ID` is known. 404 on the v3 path is
    a real failure (wrong endpoint OR wrong cluster id). Fall back to
    the path-less probe when the cluster id is unknown, but in that
    fallback 404 is reported as `fail` (no longer accepted as pass).
    """
    endpoint = env.get("CONFLUENT_KAFKA_REST_ENDPOINT", "")
    if not endpoint:
        return CheckResult(
            "warn", "CONFLUENT_KAFKA_REST_ENDPOINT not set (terraform hasn't run yet?)",
        )
    try:
        import requests
    except ImportError:
        return CheckResult("warn", "requests library unavailable")
    cluster_id = env.get("CONFLUENT_KAFKA_CLUSTER_ID", "")
    base = endpoint.rstrip("/")
    url = (
        f"{base}/kafka/v3/clusters/{cluster_id}"
        if cluster_id else base
    )
    try:
        resp = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        return CheckResult(
            "fail", f"connection error: {e}",
            remediation="check CONFLUENT_KAFKA_REST_ENDPOINT and network reachability",
        )
    if resp.status_code in (200, 401, 403):
        return CheckResult("pass", f"endpoint reachable (HTTP {resp.status_code})")
    if resp.status_code == 404:
        return CheckResult(
            "fail",
            "endpoint returned 404 — not a Kafka REST v3 API "
            f"(cluster_id={cluster_id or 'unset'})",
            remediation="verify CONFLUENT_KAFKA_REST_ENDPOINT (expected "
                        "https://<id>.<region>.<provider>.confluent.cloud:443) "
                        "and CONFLUENT_KAFKA_CLUSTER_ID",
        )
    return CheckResult("warn", f"unexpected status {resp.status_code}")


def _check_docker_daemon(env: dict) -> CheckResult:
    """`docker version` exits 0 if daemon is reachable."""
    import subprocess
    try:
        result = subprocess.run(
            ["docker", "version"],
            capture_output=True, text=True, timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        return CheckResult(
            "fail", f"docker CLI not available: {e}",
            remediation="install Docker Desktop or `brew install docker`",
        )
    if result.returncode == 0:
        return CheckResult("pass", "docker daemon reachable")
    err = (getattr(result, "stderr", "") or "").strip()
    return CheckResult(
        "fail", f"docker daemon unreachable: {err[:120]}",
        remediation="start Docker Desktop / dockerd",
    )


def _check_voyage_api_key(env: dict) -> CheckResult:
    """The event knowledge base is embedded at seed time in Python
    (asp_setup.populate_knowledge_base) using this key. A blank key does not
    stop the deploy — KB population is skipped with a warning — but the
    Vector Search evidence in Mission Control's reasoning panel stays empty,
    which defeats the RAG half of the demo. Surface it before deploy."""
    key = (env.get("TF_VAR_voyage_api_key") or "").strip()
    if not key:
        return CheckResult(
            "warn",
            "TF_VAR_voyage_api_key is not set — knowledge-base seeding will "
            "be skipped and RAG evidence chunks will be empty",
            remediation=(
                "set TF_VAR_voyage_api_key in .env (Atlas project settings "
                "→ AI / Voyage API keys), then run `uv run asp-setup` to seed"
            ),
        )
    return CheckResult("pass", "voyage api key present")


def _check_aws_caller_identity(env: dict) -> CheckResult:
    """`aws sts get-caller-identity` succeeds if AWS creds are usable."""
    import subprocess
    try:
        result = subprocess.run(
            ["aws", "sts", "get-caller-identity"],
            capture_output=True, text=True, timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        return CheckResult(
            "fail", f"aws CLI not available: {e}",
            remediation="install awscli (`brew install awscli`)",
        )
    if result.returncode == 0:
        return CheckResult("pass", "AWS credentials valid")
    err = (getattr(result, "stderr", "") or "").strip()
    return CheckResult(
        "fail", f"aws sts failed: {err[:120]}",
        remediation="check AWS_ACCESS_KEY_ID/SECRET, profile, or `aws configure`",
    )


def _check_confluent_cloud_auth(env: dict) -> CheckResult:
    """Probe Confluent Cloud Cloud API auth before terraform runs.

     Without this, `uv run preflight --phase terraform`
    has no Cloud-side probe and gives false confidence.
    """
    key = env.get("TF_VAR_confluent_cloud_api_key", "")
    secret = env.get("TF_VAR_confluent_cloud_api_secret", "")
    if not (key and secret):
        return CheckResult(
            "warn",
            "Confluent Cloud API key/secret not set",
            remediation="set TF_VAR_confluent_cloud_api_key and TF_VAR_confluent_cloud_api_secret in .env",
        )
    cred = basic_auth_token(key, secret)
    req = urllib.request.Request(
        "https://api.confluent.cloud/iam/v2/service-accounts?page_size=1",
        headers={"Authorization": f"Basic {cred}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            if r.status == 200:
                return CheckResult("pass", "Confluent Cloud credentials valid")
            return CheckResult(
                "warn", f"unexpected HTTP {r.status}",
                remediation="check Confluent Cloud API permissions",
            )
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return CheckResult(
                "fail", f"auth rejected (HTTP {e.code})",
                remediation="regenerate Cloud API key with Organization Admin",
            )
        return CheckResult(
            "warn", f"HTTP {e.code}",
            remediation="check Confluent Cloud API connectivity",
        )
    except Exception as e:
        return CheckResult(
            "warn", f"connection error: {str(e)[:120]}",
            remediation="check network connectivity to api.confluent.cloud",
        )


# ---------------------------------------------------------------------------
# Registry — populated below the check definitions so each entry's `run`
# attribute is bound to a real callable.
# ---------------------------------------------------------------------------

CHECKS: list[Check] = [
    Check("atlas_admin_auth",     ("asp_setup", "atlas_terraform"), "fail", network=True,  run=_check_atlas_admin_auth),
    # Phase-tagged asp_setup ONLY: this check assumes the Atlas cluster
    # either already exists (BYO) or was provisioned by an earlier
    # atlas_terraform phase. Running it before then would be a false
    # negative.
    Check("atlas_cluster_exists", ("asp_setup",), "fail", network=True,  run=check_atlas_cluster_exists),
    Check("mongodb_uri_format",   ("mcp_server", "terraform", "flink_dml"), "fail", network=False, run=_check_mongodb_uri_format),
    Check("mongodb_reachable",    ("mcp_server", "flink_dml"), "warn", network=True,  run=_check_mongodb_reachable),
    Check("flink_rest_reachable", ("flink_dml",), "fail", network=True,  run=_check_flink_rest_reachable),
    Check("kafka_rest_reachable", ("publish_data", "flink_dml"), "fail", network=True, run=_check_kafka_rest_reachable),
    Check("docker_daemon",        ("mcp_server",), "fail", network=False, run=_check_docker_daemon),
    # warn-severity: deploy proceeds without the key, but the knowledge base
    # would be empty (see _check_voyage_api_key docstring). Named
    # "voyage_embeddings" (not *_api_key): the redaction layer treats
    # "<name> : <message>" as a secret key/value pair and masks the first
    # message word when the name matches a secret-key pattern.
    Check("voyage_embeddings",    ("asp_setup",), "warn", network=False, run=_check_voyage_api_key),
    # terraform phase needs auth probes for both
    # Confluent Cloud and AWS (Bedrock creds exercised at terraform time).
    Check("aws_caller_identity",  ("mcp_server", "terraform"), "fail", network=True,  run=_check_aws_caller_identity),
    Check("confluent_cloud_auth", ("terraform",), "fail", network=True,  run=_check_confluent_cloud_auth),
]


# ---------------------------------------------------------------------------
# Filtering and execution
# ---------------------------------------------------------------------------

def _filter_checks(
    checks: list[Check],
    phase: str | None,
    skip_network: bool,
) -> list[Check]:
    """Return checks whose `phases` tuple is empty or contains `phase`,
    further filtered by `skip_network`."""
    out: list[Check] = []
    for c in checks:
        if c.phases and phase is not None and phase not in c.phases:
            continue
        if skip_network and c.network:
            continue
        out.append(c)
    return out


def _run_with_timeout(check: Check, env: dict, timeout: int) -> CheckResult:
    """Run `check.run(env)` in a thread with a hard timeout.

    Daemon thread so a hung check can't keep the process alive — the result
    is recorded as fail with a timeout message.
    """
    box: dict = {"result": None, "error": None}

    def worker():
        try:
            box["result"] = check.run(env)
        except BaseException as e:  # noqa: BLE001 — we want any exception
            box["error"] = e

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        return CheckResult(
            "fail",
            f"timeout after {timeout}s",
            remediation=f"check {check.name} target reachability",
        )
    if box["error"] is not None:
        e = box["error"]
        return CheckResult(
            "fail",
            f"{type(e).__name__}: {e}",
            remediation=f"check {check.name} configuration",
        )
    if box["result"] is None:
        return CheckResult("fail", "check returned no result")
    return box["result"]


def _render_check(check: Check, result: CheckResult) -> None:
    sym = {
        "pass": "[ok]  ",
        "warn": "[warn]",
        "fail": "[FAIL]",
        "skip": "[skip]",
    }.get(result.status, "[?]   ")
    cli_output.kv(f"{sym} {check.name}", result.message)
    if result.status in ("warn", "fail") and result.remediation:
        cli_output.info(f"      → {result.remediation}")


# ---------------------------------------------------------------------------
# Public entry points (, 333, 334)
# ---------------------------------------------------------------------------

def run_preflight(
    phase: str | None = None,
    skip_network: bool = False,
    env: dict | None = None,
    checks: list[Check] | None = None,
) -> tuple[int, int, int]:
    """Run preflight checks; return (passed, warned, failed).

    `phase` filters which checks run; None runs every always-run check
    plus every phase-tagged check (caller can pass a specific phase to
    only run checks relevant to it).
    """
    if env is None:
        env = _load_env()
    # expose skip_network to checks so they can adjust
    # severity (e.g. mongodb_uri DNS-error suppression depends on
    # whether the reachability follow-up will run).
    env = {**env, "_preflight_skip_network": skip_network}
    if checks is None:
        checks = CHECKS

    relevant = _filter_checks(checks, phase=phase, skip_network=skip_network)

    cli_output.section("Preflight checks")
    passed = warned = failed = skipped = 0
    for check in relevant:
        result = _run_with_timeout(check, env, timeout=CHECK_TIMEOUT_SECONDS)
        _render_check(check, result)
        if result.status == "pass":
            passed += 1
        elif result.status == "warn":
            warned += 1
        elif result.status == "fail":
            failed += 1
        elif result.status == "skip":
            skipped += 1
    # Include skipped in the summary so a skipped check (e.g. the workshop
    # fresh-deploy cluster / URI checks) is visible rather than vanishing
    # from the tally. The return tuple stays (passed, warned, failed):
    # callers gate on `failed`, and a skip is intentionally non-fatal.
    summary = f"{passed} pass, {warned} warn, {failed} fail"
    if skipped:
        summary += f", {skipped} skip"
    cli_output.kv("Summary", summary)
    return passed, warned, failed


def run_preflight_with_results(
    phase: str | None = None,
    skip_network: bool = False,
    env: dict | None = None,
    checks: list[Check] | None = None,
) -> dict:
    """Variant that returns a structured dict suitable for --json output."""
    if env is None:
        env = _load_env()
    # mirror run_preflight's flag injection.
    env = {**env, "_preflight_skip_network": skip_network}
    if checks is None:
        checks = CHECKS
    relevant = _filter_checks(checks, phase=phase, skip_network=skip_network)
    items: list[dict] = []
    counts = {"pass": 0, "warn": 0, "fail": 0, "skip": 0}
    for check in relevant:
        result = _run_with_timeout(check, env, timeout=CHECK_TIMEOUT_SECONDS)
        items.append({
            "name":        check.name,
            "phases":      list(check.phases),
            "severity":    check.severity,
            "network":     check.network,
            "status":      result.status,
            "message":     result.message,
            "remediation": result.remediation,
        })
        counts[result.status] = counts.get(result.status, 0) + 1
    return {
        "checks":  items,
        "summary": counts,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_env() -> dict:
    """Read .env. Local copy of the deploy-side helper to avoid
    a circular import at module load time."""
    here = Path(__file__).resolve()
    for parent in (here, *here.parents):
        if (parent / "pyproject.toml").exists():
            env_path = parent / ".env"
            break
    else:
        return {}
    if not env_path.exists():
        return {}
    try:
        from dotenv import dotenv_values
        return {k: v for k, v in dotenv_values(env_path).items() if v}
    except ImportError:
        return {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="preflight",
        description="Run deploy-time preflight checks.",
    )
    parser.add_argument("--phase", default=None,
                        help="Run only checks tagged for this phase.")
    parser.add_argument("--skip-network", action="store_true",
                        help="Skip checks that hit the network.")
    parser.add_argument("--json", action="store_true",
                        help="Emit machine-readable JSON output.")
    parser.add_argument("--list-checks", action="store_true",
                        help="List registered checks and exit.")
    args = parser.parse_args(argv)

    # cli_output may not be initialized when called via `uv run preflight`.
    # Pass name="preflight" so the log file is `preflight-<UTC>.log`,
    # not the misleading `deploy-<UTC>.log`.
    if cli_output._S.log_fh is None:
        cli_output.init(quiet=False, debug=False, name="preflight")

    if args.list_checks:
        for c in CHECKS:
            phases = ",".join(c.phases) if c.phases else "(always)"
            print(f"  {c.name:30}  severity={c.severity:5}  network={c.network!s:5}  phases={phases}")
        sys.exit(0)

    if args.json:
        result = run_preflight_with_results(
            phase=args.phase, skip_network=args.skip_network,
        )
        print(json.dumps(result, indent=2))
        if result["summary"].get("fail", 0) > 0:
            sys.exit(1)
        sys.exit(0)

    passed, warned, failed = run_preflight(
        phase=args.phase, skip_network=args.skip_network,
    )
    if failed > 0:
        # emit a clear summary BEFORE sys.exit(1) so the
        # operator sees what to do next. Without this, the terminal /
        # log file ends abruptly at the per-check summary line and the
        # operator has no indication that the command exited non-zero
        # or how to recover.
        cli_output.error(
            f"Preflight: {failed} check(s) failed. "
            "Fix the issues above and re-run, or use --skip-preflight "
            "to override (not recommended)."
        )
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
