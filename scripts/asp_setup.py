#!/usr/bin/env python3
"""Atlas Stream Processing (ASP) Setup Script.

Provisions ASP workspace, connection registry, and stream processors
via the Atlas Admin API v2.

Usage:
    uv run asp-setup \\
        --atlas-public-key <key> \\
        --atlas-private-key <key> \\
        --project-id <id> \\
        --cluster-name <name> \\
        --confluent-bootstrap-server <server> \\
        --confluent-api-key <key> \\
        --confluent-api-secret <secret> \\
        --voyage-api-key <key>

The script reads .env for defaults where available.

Actions:
    1. Create/verify ASP stream processing instance (SP10)
    2. Register 5–6 connections (idempotent — skip if exists; the 6th is the
       optional Schema Registry connection when SR credentials are provided)
    3. Create 5 processors (idempotent — skip if exists, start if stopped)
    4. Seed events.calendar (upsert by event_name + zone)
"""

import argparse
import json
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
    from requests.auth import HTTPDigestAuth
except ImportError:
    print("Error: 'requests' package is required. Install with: uv pip install requests")
    sys.exit(1)

try:
    # MongoClient is part of the optional-dependency availability probe for
    # HAS_PYMONGO; the code itself uses get_client/build_uri. Kept (noqa) so
    # the import presence still gates the guard.
    from pymongo import MongoClient  # noqa: F401
    from pymongo.errors import ConnectionFailure, OperationFailure
    from scripts.common.mongo import build_uri, get_client
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False

# Fail-fast cluster-existence preflight. Imported lazily so
# the asp_setup module load doesn't crash when scripts.preflight has
# an import-time error (preflight pulls in cli_output which writes to
# logs/). The fallback is a no-op that lets the deploy proceed and
# surface the original (less actionable) 400 from ensure_connections —
# preserves the old behavior on broken installs.
try:
    from scripts.preflight import check_atlas_cluster_exists
    _CLUSTER_PREFLIGHT_AVAILABLE = True
except Exception:  # noqa: BLE001 — any failure must not block asp_setup
    _CLUSTER_PREFLIGHT_AVAILABLE = False
    def check_atlas_cluster_exists(env):  # type: ignore[no-redef]
        from types import SimpleNamespace
        return SimpleNamespace(status="warn", message="preflight unavailable", remediation=None)

# -- Constants ----------------------------------------------------------------
ATLAS_API_BASE = "https://cloud.mongodb.com/api/atlas/v2"
ASP_INSTANCE_NAME = "asp-instance"
ASP_TIER = "SP10"

# Voyage AI embeddings endpoint. Default points at MongoDB Atlas's hosted
# proxy; override via TF_VAR_voyage_api_endpoint in .env (e.g. to
# call api.voyageai.com directly).
VOYAGE_API_ENDPOINT_DEFAULT = "https://ai.mongodb.com/v1/embeddings"


# -- Slug helper ----------------------------------------------------
_DOCUMENT_ID_RE = re.compile(r"[^a-z0-9]+")


def _compute_document_id(event_name: str, zone: str) -> str:
    """Compute a URL-safe slug from event_name + zone.

    Behavior contract:
      - lowercase
      - ASCII-fold (e.g. "café" → "cafe")
      - any run of non-[a-z0-9] characters collapses to a single '-'
      - no leading/trailing dashes

    Computed in Python at seed time (not in the ASP pipeline) because
    neither MongoDB aggregation nor ASP support a regex-substitute
    operator. The seed input vocabulary is fixed, so this is deterministic.
    """
    raw = f"{event_name or ''}-{zone or ''}".lower()
    # Strip non-ASCII (accents → base letters)
    folded = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    slug = _DOCUMENT_ID_RE.sub("-", folded)
    return slug.strip("-")


# -- Atlas Admin API helpers --------------------------------------------------
class AtlasAPI:
    """Thin wrapper around the Atlas Admin API v2 with Digest auth.

    every request carries a (connect, read)
    timeout AND retries on 429/5xx/connection errors with exponential
    backoff.

    - 5xx retry restricted to idempotent methods (GET, DELETE).
      POST is retried ONLY on connection errors. A 5xx response to
      POST may arrive after Atlas already accepted the side effect;
      retry would create a duplicate.
    - retry budget extended to 4 attempts with (2, 8, 20)-second
      backoff so the final attempt can ride out a typical Atlas
      503 window.
    - 429 honors the `Retry-After` header if it's larger than
      the static backoff.

    Pass-4 refinements:
    - callers may pass `idempotent=True` to opt into 5xx retry
      for POSTs where the API surface is name-idempotent. Atlas's
      connection / processor create endpoints return 409 on duplicate
      name, which the caller already treats as success — so a retry
      after a transient 503 is safe.
    - Retry-After header now parses HTTP-date form via
      email.utils.parsedate_to_datetime (RFC 9110 §10.2.3). Capped
      at 60 seconds so a buggy server can't stall the deploy.
    """

    API_VERSION_DEFAULT = "application/vnd.atlas.2023-02-01+json"
    API_VERSION_PROCESSORS = "application/vnd.atlas.2024-05-30+json"

    # hard timeouts.
    TIMEOUT = (10, 60)  # (connect_seconds, read_seconds)

    # extended retry budget. (2, 8, 20)-second backoff between
    # attempts 1→2, 2→3, 3→4 = max 30s of waiting before giving up.
    MAX_ATTEMPTS = 4
    RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
    RETRY_BACKOFF_S = (2.0, 8.0, 20.0)

    # methods where retrying a 5xx response is safe (idempotent).
    # POST is NOT idempotent: a 5xx may arrive after Atlas accepted
    # the create; retry would yield 409 (which the caller treats as
    # "already exists, OK") but the underlying resource was created
    # with the prior attempt's payload.
    _IDEMPOTENT_METHODS = frozenset({"GET", "DELETE", "HEAD", "OPTIONS", "PUT"})

    def __init__(self, public_key: str, private_key: str, project_id: str):
        self.auth = HTTPDigestAuth(public_key, private_key)
        self.project_id = project_id

    def _url(self, path: str) -> str:
        return f"{ATLAS_API_BASE}/groups/{self.project_id}{path}"

    def _headers(self, api_version: str | None = None) -> dict:
        return {
            "Accept": api_version or self.API_VERSION_DEFAULT,
            "Content-Type": "application/json",
        }

    def _parse_retry_after(self, value: str) -> float | None:
        """Parse a Retry-After header value (RFC 9110 §10.2.3).

        Accepts delta-seconds (integer) or HTTP-date. Returns seconds
        to wait, capped at 60s so a buggy server can't stall a deploy.
        Returns None on unparseable input.
        """
        if not value:
            return None
        # delta-seconds form
        try:
            return min(float(int(value)), 60.0)
        except (TypeError, ValueError):
            pass
        # HTTP-date form
        try:
            from email.utils import parsedate_to_datetime
            from datetime import datetime, timezone
            dt = parsedate_to_datetime(value)
            if dt is None:
                return None
            now = datetime.now(dt.tzinfo if dt.tzinfo else timezone.utc)
            delta = (dt - now).total_seconds()
            if delta <= 0:
                return None
            return min(delta, 60.0)
        except Exception:
            return None

    def _request_with_retry(
        self, method: str, path: str,
        idempotent: bool | None = None,
        **kwargs,
    ) -> requests.Response:
        """Issue HTTP request with timeout + retry on transient failures.

        Args:
            idempotent: When None (default), idempotency is inferred from
                HTTP method (GET/DELETE/PUT/HEAD/OPTIONS = idempotent).
                Pass True to opt into 5xx retry for POSTs on endpoints
                whose API surface is name-idempotent (e.g. Atlas
                connection create returns 409 on duplicate). H-5.
        """
        kwargs.setdefault("timeout", self.TIMEOUT)
        url = self._url(path)
        last_exc: Exception | None = None
        if idempotent is None:
            idempotent = method.upper() in self._IDEMPOTENT_METHODS
        for attempt in range(self.MAX_ATTEMPTS):
            try:
                resp = requests.request(method, url, auth=self.auth, **kwargs)
                # Retry on documented transient statuses.
                if attempt < self.MAX_ATTEMPTS - 1 and resp.status_code in self.RETRY_STATUSES:
                    # don't retry 5xx on non-idempotent methods.
                    if resp.status_code in (500, 502, 503, 504) and not idempotent:
                        return resp
                    # honor Retry-After on 429.
                    sleep_s = self.RETRY_BACKOFF_S[attempt]
                    if resp.status_code == 429:
                        ra = resp.headers.get("Retry-After") if resp.headers else None
                        parsed = self._parse_retry_after(ra) if ra else None
                        if parsed is not None:
                            sleep_s = max(sleep_s, parsed)
                    time.sleep(sleep_s)
                    continue
                return resp
            except (requests.ConnectionError, requests.Timeout) as exc:
                # Connection errors are safe to retry regardless of
                # idempotency — by definition the request didn't reach
                # the server (or the response was lost in transit).
                last_exc = exc
                if attempt < self.MAX_ATTEMPTS - 1:
                    time.sleep(self.RETRY_BACKOFF_S[attempt])
                    continue
                raise
        # Final-attempt transport error already raised above; this is
        # the unreachable fallback for static analyzers.
        if last_exc:
            raise last_exc
        raise RuntimeError("AtlasAPI._request_with_retry: unreachable")

    def get(self, path: str, api_version: str | None = None) -> requests.Response:
        return self._request_with_retry(
            "GET", path, headers=self._headers(api_version),
        )

    def post(
        self, path: str, body: dict,
        api_version: str | None = None,
        idempotent: bool = False,
    ) -> requests.Response:
        """POST with optional 5xx retry opt-in (H-5).

        Pass `idempotent=True` for endpoints where Atlas treats duplicate
        names as 409 (connection create, processor create) — the caller
        already handles 409 as success, so a retry after a transient
        503 is safe.
        """
        return self._request_with_retry(
            "POST", path,
            headers=self._headers(api_version),
            json=body,
            idempotent=idempotent,
        )

    def delete(self, path: str, api_version: str | None = None) -> requests.Response:
        return self._request_with_retry(
            "DELETE", path, headers=self._headers(api_version),
        )


# -- ASP Instance -------------------------------------------------------------
def ensure_asp_instance(api: AtlasAPI, cluster_name: str) -> dict:
    """Create or verify the ASP stream processing instance."""
    print(f"\n{'='*60}")
    print("Step 1: ASP Stream Processing Instance")
    print(f"{'='*60}")

    # Check if instance already exists
    resp = api.get("/streams")
    resp.raise_for_status()
    instances = resp.json().get("results", [])

    for inst in instances:
        if inst.get("name") == ASP_INSTANCE_NAME:
            print(f"  ✓ Instance '{ASP_INSTANCE_NAME}' already exists (id={inst.get('_id', inst.get('id', 'N/A'))})")
            return inst

    # Create new instance
    print(f"  Creating instance '{ASP_INSTANCE_NAME}' (tier={ASP_TIER})...")
    body = {
        "name": ASP_INSTANCE_NAME,
        "dataProcessRegion": {
            "cloudProvider": "AWS",
            "region": "VIRGINIA_USA",
        },
        "streamConfig": {
            "tier": ASP_TIER,
        },
    }
    # instance create is name-idempotent (409 on duplicate is
    # handled below). Allow 5xx retry.
    resp = api.post("/streams", body, idempotent=True)
    if resp.status_code == 409:
        print(f"  ✓ Instance '{ASP_INSTANCE_NAME}' already exists (409 conflict)")
        resp2 = api.get("/streams")
        resp2.raise_for_status()
        for inst in resp2.json().get("results", []):
            if inst.get("name") == ASP_INSTANCE_NAME:
                return inst
    if resp.status_code >= 400:
        print(f"  ✗ API error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    instance = resp.json()
    print(f"  ✓ Created instance '{ASP_INSTANCE_NAME}' (id={instance.get('id', 'pending')})")

    # Wait for instance to become ACTIVE (has hostnames)
    print("  Waiting for instance to become ACTIVE...")
    for _ in range(60):
        time.sleep(10)
        resp = api.get(f"/streams/{ASP_INSTANCE_NAME}")
        resp.raise_for_status()
        inst_data = resp.json()
        hostnames = inst_data.get("hostnames", [])
        state = inst_data.get("stateName", "")
        if hostnames or state == "ACTIVE":
            print(f"  ✓ Instance is ACTIVE (hostnames: {hostnames[0] if hostnames else 'N/A'})")
            return inst_data
        print(f"    ... state: {state or 'provisioning'}")

    # raise rather than sys.exit(1). The caller
    # (run_asp_setup) wraps in try/except and returns a bool; sys.exit
    # bypasses that contract and prevents deploy.py phase-resume from
    # recording the failure.
    raise RuntimeError(
        f"ASP instance '{ASP_INSTANCE_NAME}' did not reach ACTIVE state "
        "within 10 minutes. Re-run the deploy (it will resume from "
        "asp_setup) once the Atlas control plane is healthy."
    )


# -- Connection Registry ------------------------------------------------------
def ensure_connections(
    api: AtlasAPI,
    cluster_name: str,
    bootstrap_server: str,
    confluent_api_key: str,
    confluent_api_secret: str,
    voyage_api_key: str,
    schema_registry_url: str = "",
    schema_registry_key: str = "",
    schema_registry_secret: str = "",
    voyage_api_endpoint: str = VOYAGE_API_ENDPOINT_DEFAULT,
) -> None:
    """Register all connections in the ASP connection registry (idempotent).

    Registers 5 base connections plus an optional Schema Registry connection
    (6 total) when schema registry credentials are provided.  The Schema
    Registry connection is required for Pipelines 4 & 5 which consume
    Avro-serialized Kafka topics produced by Flink.
    """
    print(f"\n{'='*60}")
    print("Step 2: Connection Registry")
    print(f"{'='*60}")

    # Get existing connections
    resp = api.get(f"/streams/{ASP_INSTANCE_NAME}/connections")
    resp.raise_for_status()
    existing = {c["name"] for c in resp.json().get("results", [])}

    # Strip SASL_SSL:// prefix if present (Terraform outputs include it, ASP API rejects it)
    clean_bootstrap = bootstrap_server
    if clean_bootstrap.startswith("SASL_SSL://"):
        clean_bootstrap = clean_bootstrap[len("SASL_SSL://"):]

    connections = [
        {
            "name": "kafka_confluent",
            "type": "Kafka",
            "bootstrapServers": clean_bootstrap,
            "authentication": {
                "mechanism": "PLAIN",
                "username": confluent_api_key,
                "password": confluent_api_secret,
            },
            "security": {
                "protocol": "SASL_SSL",
            },
        },
        {
            "name": "atlas_cluster",
            "type": "Cluster",
            "clusterName": cluster_name,
            "dbRoleToExecute": {
                "role": "readWriteAnyDatabase",
                "type": "BUILT_IN",
            },
        },
        {
            "name": "voyage_ai",
            "type": "Https",
            "url": voyage_api_endpoint,
            "headers": {
                "Authorization": f"Bearer {voyage_api_key}",
                "Content-Type": "application/json",
            },
        },
        {
            "name": "events_dlq",
            "type": "Cluster",
            "clusterName": cluster_name,
            "dbRoleToExecute": {
                "role": "readWriteAnyDatabase",
                "type": "BUILT_IN",
            },
        },
        {
            "name": "fleet_dlq",
            "type": "Cluster",
            "clusterName": cluster_name,
            "dbRoleToExecute": {
                "role": "readWriteAnyDatabase",
                "type": "BUILT_IN",
            },
        },
    ]

    # Add Schema Registry connection if credentials are provided
    if schema_registry_url and schema_registry_key and schema_registry_secret:
        connections.append({
            "name": "confluent_schema_registry",
            "type": "SchemaRegistry",
            "provider": "CONFLUENT",
            "schemaRegistryUrls": [schema_registry_url],
            "schemaRegistryAuthentication": {
                "type": "USER_INFO",
                "username": schema_registry_key,
                "password": schema_registry_secret,
            },
        })

    # If any connections need updating, stop active processors first.
    # Connections can't be deleted while processors that use them are STARTED.
    # Processors will be restarted by ensure_processors() which runs later.
    connections_to_update = {c["name"] for c in connections if c["name"] in existing}
    if connections_to_update:
        proc_api_ver = AtlasAPI.API_VERSION_PROCESSORS
        proc_resp = api.get(
            f"/streams/{ASP_INSTANCE_NAME}/processors",
            api_version=proc_api_ver,
        )
        if proc_resp.ok:
            import time as _time
            # stop EVERY non-terminal processor,
            # not just STARTED ones. A processor left STOPPING by a prior
            # interrupted run still HOLDS its connections, so a later
            # DELETE connection 403s with
            # STREAM_CONNECTION_HAS_STREAM_PROCESSORS. {STOPPED, FAILED}
            # are terminal (release connections); everything else
            # (STARTED, STARTING, STOPPING, INIT...) must reach terminal
            # before we delete any connection.
            _TERMINAL = {"STOPPED", "FAILED"}
            to_wait: list[str] = []
            for proc in proc_resp.json().get("results", []):
                pname = proc["name"]
                state = proc.get("state", "")
                if state in _TERMINAL:
                    continue
                if state == "STOPPING":
                    # Already stopping (likely from a prior interrupted
                    # run) — do NOT re-send :stop (would lock-conflict);
                    # just wait for it to finish.
                    print(f"  Waiting for STOPPING processor '{pname}'...")
                else:
                    print(f"  Stopping processor '{pname}' for connection update...")
                    # Tolerate transient errors / lock conflicts; the
                    # poll below is the source of truth for STOPPED.
                    try:
                        api.post(
                            f"/streams/{ASP_INSTANCE_NAME}/processor/{pname}:stop",
                            {},
                            api_version=proc_api_ver,
                        )
                    except Exception as _exc:
                        print(f"    [warn] :stop {pname} raised {type(_exc).__name__} "
                              f"(will still poll for STOPPED)")
                to_wait.append(pname)

            # Poll ALL non-terminal processors to {STOPPED, FAILED}
            # together (up to 60s) before proceeding to connection
            # deletes. refuse to delete connections
            # while any processor still holds them.
            if to_wait:
                poll_deadline = _time.monotonic() + 60
                pending = set(to_wait)
                while pending and _time.monotonic() < poll_deadline:
                    _time.sleep(3)
                    for pname in list(pending):
                        try:
                            status_resp = api.get(
                                f"/streams/{ASP_INSTANCE_NAME}/processor/{pname}",
                                api_version=proc_api_ver,
                            )
                            if status_resp.ok:
                                cur_state = status_resp.json().get("state", "")
                                if cur_state in _TERMINAL:
                                    pending.discard(pname)
                                    print(f"  ✓ Stopped '{pname}'")
                        except Exception:
                            pass
                if pending:
                    raise RuntimeError(
                        f"Processor(s) {sorted(pending)} did not reach "
                        f"STOPPED within 60s — refusing to delete their "
                        f"connections (would leave stale credentials). "
                        f"Re-run `uv run deploy` (resumes from asp_setup); "
                        f"Atlas is often transiently slow here."
                    )

    # track failures and raise at the end. Previously the
    # function silently `continue`d on any failure, leaving the OLD
    # connection in place with stale credentials and returning None to
    # the caller (which couldn't detect partial failure).
    failures: list[str] = []
    import time as _time
    for conn in connections:
        name = conn["name"]
        if name in existing:
            # Delete and recreate so credentials are always current
            # (e.g. deploying to a different Confluent account)
            print(f"  Updating connection '{name}' (delete + recreate)...")
            del_resp = api.delete(f"/streams/{ASP_INSTANCE_NAME}/connections/{name}")
            if not del_resp.ok and del_resp.status_code != 404:
                msg = f"delete connection '{name}': {del_resp.status_code} {del_resp.text[:200]}"
                print(f"  ✗ {msg}")
                failures.append(msg)
                continue
            # ASP connection DELETE is async. Poll
            # GET until 404 (max 30s) before POSTing the new one.
            # Without this, the immediate POST may 409 because the
            # old connection is still draining — and the 409 would
            # incorrectly look like "already exists, OK", leaving
            # processors running against stale credentials.
            #
            # use raw requests.get with a short timeout
            # instead of api.get(). The retry-with-backoff layer
            # (MAX_ATTEMPTS=4, backoff 2/8/20s) would otherwise eat
            # the entire 30s poll budget on the FIRST probe if Atlas
            # is rate-limited.
            #
            # timeout is now `min(3s, remaining)` so
            # we never block past the poll deadline. Previously a
            # `(5,5)` timeout could push effective budget to ~42s.
            probe_url = api._url(f"/streams/{ASP_INSTANCE_NAME}/connections/{name}")
            probe_headers = api._headers()
            poll_deadline = _time.monotonic() + 30
            gone = False
            while _time.monotonic() < poll_deadline:
                remaining = poll_deadline - _time.monotonic()
                if remaining <= 0:
                    break
                t = max(1.0, min(3.0, remaining))
                try:
                    probe = requests.get(
                        probe_url, auth=api.auth,
                        headers=probe_headers, timeout=(t, t),
                    )
                    if probe.status_code == 404:
                        gone = True
                        break
                except requests.RequestException:
                    pass  # blip — keep polling
                _time.sleep(min(2, max(0, poll_deadline - _time.monotonic())))
            if not gone:
                msg = (
                    f"connection '{name}': DELETE accepted but resource "
                    f"still visible after 30s — refusing to POST to avoid "
                    f"a stale-creds race."
                )
                print(f"  ✗ {msg}")
                failures.append(msg)
                continue
        else:
            print(f"  Creating connection '{name}' (type={conn['type']})...")

        # connection create is name-idempotent (409 on duplicate
        # handled below). Allow 5xx retry to ride out transient Atlas
        # 503s during the connection-rotation phase.
        # 409 here after a successful DELETE means the connection
        # is still draining; treat as failure rather than skip-success.
        was_delete_recreate = name in existing
        resp = api.post(
            f"/streams/{ASP_INSTANCE_NAME}/connections", conn,
            idempotent=True,
        )
        if resp.status_code == 409:
            if was_delete_recreate:
                msg = (
                    f"connection '{name}': 409 after DELETE — the old "
                    f"connection is still draining. Wait 30s and re-run."
                )
                print(f"  ✗ {msg}")
                failures.append(msg)
                continue
            print(f"  ✓ Connection '{name}' already exists (409 conflict)")
            continue
        if not resp.ok:
            msg = f"create connection '{name}': {resp.status_code} {resp.text[:200]}"
            print(f"  ✗ {msg}")
            failures.append(msg)
            continue
        # use "Recreated" verb on the delete-recreate
        # path so logs accurately reflect that credentials were rotated.
        verb = "Recreated" if was_delete_recreate else "Created"
        print(f"  ✓ {verb} connection '{name}'")

    if failures:
        raise RuntimeError(
            "ensure_connections failed for "
            f"{len(failures)} connection(s):\n  - "
            + "\n  - ".join(failures)
        )


# -- Kafka Topic Pre-creation -------------------------------------------------
REQUIRED_KAFKA_TOPICS = ["event_documents", "completed_actions", "zone_traffic_sink", "anomalies_sink"]


def ensure_kafka_topics(
    kafka_rest_endpoint: str,
    cluster_id: str,
    confluent_api_key: str,
    confluent_api_secret: str,
) -> None:
    """Pre-create Kafka topics required by ASP $emit stages (idempotent).

    Uses the Confluent Cloud Kafka REST API v3 (HTTP Basic auth) which
    authenticates immediately — unlike the SASL_SSL broker path that can
    take 2-5+ minutes for API key propagation after terraform creates it.
    """
    import base64
    import urllib.request
    import urllib.error

    print(f"\n{'='*60}")
    print("Step 2b: Kafka Topic Pre-creation")
    print(f"{'='*60}")

    if not kafka_rest_endpoint or not cluster_id:
        print("  ⚠ Kafka REST endpoint or cluster ID not available — skipping topic pre-creation.")
        print("    Ensure these topics exist before starting processors:")
        for t in REQUIRED_KAFKA_TOPICS:
            print(f"      - {t}")
        return

    cred = base64.b64encode(f"{confluent_api_key}:{confluent_api_secret}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {cred}",
    }

    # Wait for API key to be accepted (REST API returns 401 during propagation).
    # The REST API path can take 3-7 minutes to propagate independently of the
    # SASL_SSL broker path. Use longer waits to avoid exhausting retries too fast.
    list_url = f"{kafka_rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics"
    max_probe_attempts = 16
    for attempt in range(max_probe_attempts):
        probe_req = urllib.request.Request(list_url, headers=headers)
        try:
            urllib.request.urlopen(probe_req, timeout=15)
            break
        except urllib.error.HTTPError as e:
            if e.code == 401 and attempt < max_probe_attempts - 1:
                wait = 30
                print(f"  ⏳ Kafka REST auth not ready, retrying in {wait}s... (HTTP 401)")
                time.sleep(wait)
            elif e.code == 401:
                print("  ✗ Kafka REST API key not propagated after retries — skipping topic creation")
                print("    Topics will be created by the Flink DML step later.")
                return
            else:
                break
        except Exception:
            break

    for topic_name in REQUIRED_KAFKA_TOPICS:
        check_url = f"{kafka_rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics/{topic_name}"
        req = urllib.request.Request(check_url, headers=headers)
        try:
            urllib.request.urlopen(req, timeout=15)
            print(f"  ✓ Topic '{topic_name}' already exists — skipping")
            continue
        except urllib.error.HTTPError as e:
            if e.code != 404:
                print(f"  ⚠ Unexpected status checking topic '{topic_name}': HTTP {e.code}")
        except Exception:
            pass

        create_url = f"{kafka_rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics"
        body = json.dumps({
            "topic_name": topic_name,
            "partitions_count": 6,
        }).encode()
        create_req = urllib.request.Request(create_url, data=body, method="POST", headers=headers)
        try:
            urllib.request.urlopen(create_req, timeout=30)
            print(f"  ✓ Created topic '{topic_name}'")
        except urllib.error.HTTPError as e:
            resp_body = e.read().decode() if e.fp else ""
            if "TopicExistsException" in resp_body or e.code == 409:
                print(f"  ✓ Topic '{topic_name}' already exists (race)")
            else:
                print(f"  ✗ Failed to create topic '{topic_name}': HTTP {e.code}")
        except Exception as e:
            print(f"  ✗ Failed to create topic '{topic_name}': {e}")


# -- Pipeline Definitions -----------------------------------------------------

def _pipeline_event_knowledge_base() -> list:
    """Pipeline 1: events.calendar -> Voyage AI embed -> events.knowledge_base"""
    return [
        {
            "$source": {
                "connectionName": "atlas_cluster",
                "db": "events",
                "coll": "calendar",
                "config": {
                    "fullDocument": "updateLookup",
                    "fullDocumentOnly": True,
                },
            }
        },
        {
            "$validate": {
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["event_name", "zone", "description", "event_time_start"],
                        "properties": {
                            "event_name": {"bsonType": "string"},
                            "zone": {"bsonType": "string"},
                            "description": {"bsonType": "string"},
                            "event_time_start": {"bsonType": "date"},
                        },
                    }
                },
                "validationAction": "dlq",
            }
        },
        {
            # document_id is computed in Python at seed
            # time (see _compute_document_id) and stored on the calendar doc.
            # ASP doesn't support $regexReplace, and even standard MongoDB
            # aggregation has no substitution operator, so streaming-time
            # slug generation is structurally not possible. The pipeline
            # here just promotes `description` to `chunk` for the embedding.
            "$addFields": {
                "chunk": "$description",
            }
        },
        {
            "$https": {
                "connectionName": "voyage_ai",
                "method": "POST",
                "payload": [
                    {
                        "$replaceRoot": {
                            "newRoot": {
                                "model": "voyage-4",
                                "input": ["$description"],
                                "input_type": "document",
                            }
                        }
                    }
                ],
                "as": "voyage_response",
                "onError": "dlq",
            }
        },
        {
            "$addFields": {
                "embedding": {
                    "$getField": {
                        "field": "embedding",
                        "input": {"$arrayElemAt": ["$voyage_response.data", 0]},
                    }
                }
            }
        },
        {
            # guard against malformed Voyage responses
            # producing null embeddings. If `data` is missing or empty,
            # the extraction above yields null, which would silently
            # land as a null-embedding doc — vector search then misses
            # it forever. DLQ malformed docs so the operator can see
            # the rate of upstream failures.
            "$validate": {
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["embedding"],
                        "properties": {
                            "embedding": {"bsonType": "array", "minItems": 1},
                        },
                    }
                },
                "validationAction": "dlq",
            }
        },
        {
            # stamp embedding provenance so future model rotations
            # (e.g. voyage-4 -> voyage-5, 1024 -> 2048 dim) can incrementally
            # re-embed by querying {embedding_model: {$ne: "voyage-5"}}.
            "$addFields": {
                # $$NOW is REJECTED by ASP $addFields
                # ("Builtin variable '$$NOW' is not available") — it sent every
                # doc to the DLQ. Use the per-document stream timestamp.
                "embedded_at": "$_stream_meta.source.ts",
                "embedding_model": "voyage-4",
                "embedding_dim": 1024,
                "schema_version": 1,
            }
        },
        {
            "$project": {
                "document_id": 1,
                "chunk": 1,
                "embedding": 1,
                "event_name": 1,
                "event_time_start": 1,
                "event_time_end": 1,
                "venue": 1,
                "expected_attendance": 1,
                "zone": 1,
                "event_type": 1,
                "impact_level": 1,
                "embedded_at": 1,
                "embedding_model": 1,
                "embedding_dim": 1,
                "schema_version": 1,
            }
        },
        {
            "$merge": {
                "into": {
                    "connectionName": "atlas_cluster",
                    "db": "events",
                    "coll": "knowledge_base",
                },
                "on": "document_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]


def _pipeline_event_publication() -> list:
    """Pipeline 2: events.calendar -> Kafka event_documents topic."""
    return [
        {
            "$source": {
                "connectionName": "atlas_cluster",
                "db": "events",
                "coll": "calendar",
                "config": {
                    "fullDocument": "updateLookup",
                    "fullDocumentOnly": True,
                },
            }
        },
        {
            "$validate": {
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["event_name", "zone", "description"],
                        "properties": {
                            "event_name": {"bsonType": "string"},
                            "zone": {"bsonType": "string"},
                            "description": {"bsonType": "string"},
                        },
                    }
                },
                "validationAction": "dlq",
            }
        },
        {
            "$project": {
                "_id": 0,
                "event_name": 1,
                "event_type": 1,
                "venue": 1,
                "zone": 1,
                "event_time_start": 1,
                "event_time_end": 1,
                "expected_attendance": 1,
                "description": 1,
                "impact_level": 1,
                "created_at": 1,
                "updated_at": 1,
                "_kafka_key": {"$toString": "$_id"},
            }
        },
        {
            "$emit": {
                "connectionName": "kafka_confluent",
                "topic": "event_documents",
            }
        },
    ]


def _pipeline_dispatch_log() -> list:
    """Pipeline 3: Kafka completed_actions -> fleet.dispatch_log.

    Uses Schema Registry for Avro deserialization -- Flink writes Avro to
    the completed_actions Kafka topic via the Confluent Schema Registry.
    """
    return [
        {
            "$source": {
                "connectionName": "kafka_confluent",
                "topic": "completed_actions",
                "schemaRegistry": {
                    "connectionName": "confluent_schema_registry",
                },
            }
        },
        {
            "$validate": {
                "validator": {
                    "$jsonSchema": {
                        # window_time required for $merge on: contract.
                        "bsonType": "object",
                        "required": ["pickup_zone", "dispatch_summary", "window_time"],
                        "properties": {
                            "pickup_zone": {"bsonType": "string"},
                            "dispatch_summary": {"bsonType": "string"},
                            "window_time": {"bsonType": ["long", "int", "double", "date"]},
                            "dispatch_json": {"bsonType": ["string", "null"]},
                            "api_response": {"bsonType": ["string", "null"]},
                        },
                    }
                },
                "validationAction": "dlq",
            }
        },
        {
            # defensive $match guard. $validate already
            # ensures shape, but a second guard is cheap and catches any
            # null/missing window_time that slips through (e.g. Flink
            # producing tombstones during schema drift).
            "$match": {
                "pickup_zone": {"$type": "string"},
                "window_time": {"$type": ["long", "int", "double", "date"]},
            }
        },
        {
            "$addFields": {
                # $toDate so the BSON Date sort
                # direction of the unique compound index works correctly.
                # $$NOW is REJECTED by ASP $addFields
                # ("Builtin variable '$$NOW' is not available"). Use the
                # per-document stream timestamp instead.
                "window_time": {"$toDate": "$window_time"},
                "dispatched_at": "$_stream_meta.source.ts",
            }
        },
        {
            "$merge": {
                "into": {
                    "connectionName": "atlas_cluster",
                    "db": "fleet",
                    "coll": "dispatch_log",
                },
                "on": ["pickup_zone", "window_time"],
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]


def _pipeline_zone_traffic_ingestion() -> list:
    """Pipeline 4: Kafka zone_traffic_sink -> analytics.zone_traffic.

    Uses Schema Registry for Avro deserialization -- Flink writes Avro to
    these Kafka topics via the Confluent Schema Registry.

    window_start/window_end converted to BSON Date so the
    TTL index on window_start is effective.
    $match guard ensures merge keys are non-null and typed.
    """
    return [
        {
            "$source": {
                "connectionName": "kafka_confluent",
                "topic": "zone_traffic_sink",
                "schemaRegistry": {
                    "connectionName": "confluent_schema_registry",
                },
            }
        },
        {
            # $validate -> DLQ before $match so schema drift
            # is observable rather than silently dropped by $match.
            "$validate": {
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["zone", "window_start"],
                        "properties": {
                            "zone": {"bsonType": "string"},
                            "window_start": {"bsonType": ["long", "int", "double", "date"]},
                            "window_end": {"bsonType": ["long", "int", "double", "date"]},
                        },
                    }
                },
                "validationAction": "dlq",
            }
        },
        {
            "$match": {
                "zone": {"$type": "string"},
                "window_start": {"$type": ["long", "int", "double", "date"]},
            }
        },
        {
            "$addFields": {
                "window_start": {"$toDate": "$window_start"},
                "window_end": {"$toDate": "$window_end"},
                # $$NOW is REJECTED by ASP $addFields
                # ("Builtin variable '$$NOW' is not available") — it sent every
                # doc to the DLQ. Use the per-document stream timestamp.
                "ingested_at": "$_stream_meta.source.ts",
            }
        },
        {
            "$merge": {
                "into": {
                    "connectionName": "atlas_cluster",
                    "db": "analytics",
                    "coll": "zone_traffic",
                },
                "on": ["zone", "window_start"],
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]


def _pipeline_anomalies_ingestion() -> list:
    """Pipeline 5: Kafka anomalies_sink -> analytics.zone_anomalies.

    Uses Schema Registry for Avro deserialization -- Flink writes Avro to
    these Kafka topics via the Confluent Schema Registry.

    window_time converted to BSON Date.
    $match guard ensures merge keys are non-null and typed.
    """
    return [
        {
            "$source": {
                "connectionName": "kafka_confluent",
                "topic": "anomalies_sink",
                "schemaRegistry": {
                    "connectionName": "confluent_schema_registry",
                },
            }
        },
        {
            # $validate -> DLQ before $match.
            "$validate": {
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["pickup_zone", "window_time"],
                        "properties": {
                            "pickup_zone": {"bsonType": "string"},
                            "window_time": {"bsonType": ["long", "int", "double", "date"]},
                        },
                    }
                },
                "validationAction": "dlq",
            }
        },
        {
            "$match": {
                "pickup_zone": {"$type": "string"},
                "window_time": {"$type": ["long", "int", "double", "date"]},
            }
        },
        {
            "$addFields": {
                "window_time": {"$toDate": "$window_time"},
                # $$NOW is REJECTED by ASP $addFields
                # ("Builtin variable '$$NOW' is not available") — it sent every
                # doc to the DLQ. Use the per-document stream timestamp.
                "ingested_at": "$_stream_meta.source.ts",
            }
        },
        {
            "$merge": {
                "into": {
                    "connectionName": "atlas_cluster",
                    "db": "analytics",
                    "coll": "zone_anomalies",
                },
                "on": ["pickup_zone", "window_time"],
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]


# -- Stream Processors --------------------------------------------------------
def _start_processor_with_retry(api: "AtlasAPI", name: str, max_retries: int = 6) -> bool:
    """Start a processor, retrying on transient failures.

    Retries on:
    - SASL authentication errors (API key propagation delay)
    - "being provisioned" errors (processor not ready yet after creation)

    Returns True if the processor started successfully, False otherwise.
    """
    proc_api_ver = AtlasAPI.API_VERSION_PROCESSORS
    for attempt in range(max_retries):
        start_resp = api.post(
            f"/streams/{ASP_INSTANCE_NAME}/processor/{name}:start",
            {},
            api_version=proc_api_ver,
        )
        if start_resp.ok:
            print(f"    ✓ Started '{name}'")
            return True
        if start_resp.status_code == 409:
            print(f"    ✓ Processor '{name}' already running")
            return True

        error_text = start_resp.text
        is_auth_error = (
            "SASL authentication error" in error_text
            or "Authentication failed" in error_text
        )
        is_provisioning = "being provisioned" in error_text
        is_retryable = is_auth_error or is_provisioning

        if is_retryable and attempt < max_retries - 1:
            wait = min(15 * (attempt + 1), 45)
            if is_provisioning:
                print(f"    ⏳ Processor still provisioning, retrying in {wait}s...")
            else:
                print(f"    ⏳ Kafka auth not propagated, retrying start in {wait}s...")
            time.sleep(wait)
            continue

        print(f"    ✗ Failed to start: {start_resp.status_code} {error_text}")
        return False
    return False


def ensure_processors(api: AtlasAPI) -> bool:
    """Create and start the 5 ASP stream processors (idempotent).

    Returns True if any processor was newly created on this run (so the
    caller can decide whether to re-seed events.calendar to wake the
    change-stream-driven KB processor).
    """
    processors_created = False
    # parity with ensure_connections: accumulate processors that
    # never reached a started state across all retries and raise at the end,
    # so a non-starting processor (e.g. persistent SASL auth failure) is a
    # hard failure instead of a silently-swallowed "success".
    start_failures: list[str] = []
    print(f"\n{'='*60}")
    print("Step 3: Stream Processors")
    print(f"{'='*60}")

    proc_api_ver = AtlasAPI.API_VERSION_PROCESSORS

    # Get existing processors (LIST uses plural /processors)
    resp = api.get(f"/streams/{ASP_INSTANCE_NAME}/processors", api_version=proc_api_ver)
    if not resp.ok:
        print(f"  ✗ Failed to list processors: {resp.status_code} {resp.text}")
        resp.raise_for_status()
    existing = {}
    for p in resp.json().get("results", []):
        existing[p["name"]] = p

    processors = [
        {
            "name": "event_knowledge_base_population",
            "pipeline": _pipeline_event_knowledge_base(),
            "options": {
                "dlq": {
                    "connectionName": "events_dlq",
                    "db": "events",
                    "coll": "validation_dlq",
                },
            },
        },
        {
            "name": "event_publication_to_kafka",
            "pipeline": _pipeline_event_publication(),
            "options": {
                "dlq": {
                    "connectionName": "events_dlq",
                    "db": "events",
                    "coll": "validation_dlq",
                },
            },
        },
        {
            "name": "dispatch_log_ingestion",
            "pipeline": _pipeline_dispatch_log(),
            "options": {
                "dlq": {
                    "connectionName": "fleet_dlq",
                    "db": "fleet",
                    "coll": "validation_dlq",
                },
            },
        },
        {
            "name": "zone_traffic_ingestion",
            "pipeline": _pipeline_zone_traffic_ingestion(),
            "options": {
                "dlq": {
                    "connectionName": "events_dlq",
                    "db": "events",
                    "coll": "validation_dlq",
                },
            },
        },
        {
            "name": "anomalies_ingestion",
            "pipeline": _pipeline_anomalies_ingestion(),
            "options": {
                "dlq": {
                    "connectionName": "events_dlq",
                    "db": "events",
                    "coll": "validation_dlq",
                },
            },
        },
    ]

    for proc_def in processors:
        name = proc_def["name"]

        if name in existing:
            state = existing[name].get("state", "UNKNOWN")
            print(f"  ✓ Processor '{name}' already exists (state={state})")

            if state == "STARTED":
                continue

            # FAILED processors likely have stale connections — delete and recreate
            if state == "FAILED":
                print(f"    Deleting FAILED processor '{name}' for recreation...")
                stop_resp = api.post(
                    f"/streams/{ASP_INSTANCE_NAME}/processor/{name}:stop",
                    {},
                    api_version=proc_api_ver,
                )
                # Best-effort stop before delete; surface a non-fatal warning
                # if it failed (the delete below is the operation that matters
                # and is checked explicitly).
                if not (stop_resp.ok or stop_resp.status_code == 404):
                    print(f"    [warn] :stop on '{name}' returned {stop_resp.status_code} (continuing to delete)")
                del_resp = api.delete(
                    f"/streams/{ASP_INSTANCE_NAME}/processor/{name}",
                    api_version=proc_api_ver,
                )
                if del_resp.ok or del_resp.status_code == 404:
                    print(f"    ✓ Deleted '{name}'")
                else:
                    print(f"    ✗ Could not delete '{name}': {del_resp.status_code} {del_resp.text}")
                    continue
                # Fall through to create + start below
            elif state in ("STOPPED", "CREATED"):
                print(f"    Starting processor '{name}'...")
                if not _start_processor_with_retry(api, name):
                    start_failures.append(name)
                continue
            else:
                continue

        # CREATE uses singular /processor
        # processor create is name-idempotent (409 on duplicate
        # handled below). Allow 5xx retry.
        print(f"  Creating processor '{name}'...")
        resp = api.post(
            f"/streams/{ASP_INSTANCE_NAME}/processor",
            proc_def,
            api_version=proc_api_ver,
            idempotent=True,
        )
        if resp.status_code == 409:
            print(f"  ✓ Processor '{name}' already exists (409 conflict)")
        elif not resp.ok:
            print(f"  ✗ Failed to create processor '{name}': {resp.status_code} {resp.text}")
            continue
        else:
            print(f"  ✓ Created processor '{name}'")
            processors_created = True

        # Start the processor
        print(f"    Starting processor '{name}'...")
        if not _start_processor_with_retry(api, name):
            start_failures.append(name)

    if start_failures:
        raise RuntimeError(
            "ensure_processors failed to start "
            f"{len(start_failures)} processor(s) after all retries:\n  - "
            + "\n  - ".join(start_failures)
            + "\n  These processors are not consuming. Verify Kafka API key "
            "propagation and connection health, then re-run `uv run asp-setup`."
        )

    return processors_created


# -- Atlas Indexes ------------------------------------------------------------
def _ensure_kb_collection(client) -> None:
    """Create the events.knowledge_base collection idempotently.

    the vector_index POST requires the collection
    to exist. `create_collection` raises CollectionInvalid (pymongo) /
    "already exists" when the collection is present — swallow that so
    re-deploys are no-ops. Any other error is also swallowed (best-effort:
    if the collection truly can't be created, the vector-index POST will
    surface the real error).

    `client` is a mapping-like (client["events"]["knowledge_base"]) so
    this is unit-testable with a dict/MagicMock.
    """
    try:
        events_db = client["events"]
        events_db.create_collection("knowledge_base")
        print("  ✓ Created events.knowledge_base collection")
    except Exception as e:  # noqa: BLE001
        # CollectionInvalid / NamespaceExists / already-exists are the
        # expected idempotent path; anything else we also tolerate so
        # the broader index step proceeds and surfaces the real error.
        msg = str(e).lower()
        if "exist" in msg or "namespace" in msg or type(e).__name__ == "CollectionInvalid":
            return
        print(f"  ⚠ _ensure_kb_collection: {type(e).__name__}: {e}")


def _ensure_kb_collection_via_creds(connection_string: str, username: str, password: str) -> None:
    """Connect via pymongo and ensure events.knowledge_base exists.

    Thin wrapper around _ensure_kb_collection that owns the connection.
    Best-effort: connection failures are logged, not raised (the
    downstream vector-index POST will surface a real error if the
    collection genuinely doesn't exist).
    """
    if not HAS_PYMONGO:
        return
    try:
        uri = build_uri(connection_string, username, password)
        client = get_client(uri, app_name="streaming-agents-asp-kb-collection",
                            server_selection_timeout_ms=10000)
        client.admin.command("ping")
        _ensure_kb_collection(client)
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠ Could not pre-create events.knowledge_base: {e}")


def ensure_atlas_indexes(
    api: AtlasAPI,
    cluster_name: str,
    connection_string: str = "",
    username: str = "",
    password: str = "",
) -> None:
    """Create Atlas Vector Search index and collection indexes (idempotent).

    - Atlas Vector Search index 'vector_index' on events.knowledge_base
      (1024 dims, cosine, filter fields: zone, impact_level, event_type)
    - Compound/TTL indexes on analytics and fleet collections
    """
    print(f"\n{'='*60}")
    print("Step 5: Atlas Indexes")
    print(f"{'='*60}")

    # -- Atlas Vector Search Index (via Atlas Admin API) --
    search_index_def = {
        "name": "vector_index",
        "type": "vectorSearch",
        "definition": {
            "fields": [
                {
                    "type": "vector",
                    "path": "embedding",
                    "numDimensions": 1024,
                    "similarity": "cosine",
                },
                {
                    "type": "filter",
                    "path": "zone",
                },
                {
                    "type": "filter",
                    "path": "impact_level",
                },
                {
                    "type": "filter",
                    "path": "event_type",
                },
            ]
        },
    }

    # the vector_index POST below
    # requires the events.knowledge_base collection to ALREADY exist.
    # On a fresh cluster it does not (the pymongo create_index further
    # down is what implicitly creates it — but that runs AFTER this
    # block). The Atlas Admin API then returns 400
    # ATLAS_SEARCH_COLLECTION_NOT_FOUND and the index is never created,
    # so the RAG pipeline (anomalies-enriched-insert) FAILS forever.
    # Create the collection explicitly FIRST (idempotent).
    if connection_string and username and password:
        _ensure_kb_collection_via_creds(connection_string, username, password)

    # Check existing search indexes.
    # on a fresh cluster the collection doesn't
    # exist yet, so GET search/indexes returns 404. Previously the code
    # fell into the `else` branch and silently SKIPPED creation —
    # leaving RAG returning empty results forever. Treat 404 as
    # "no existing indexes, proceed to create."
    resp = api.get(
        f"/clusters/{cluster_name}/search/indexes/events/knowledge_base",
        api_version="application/vnd.atlas.2024-05-30+json",
    )
    existing_indexes = None
    if resp.ok:
        existing_indexes = resp.json()
    elif resp.status_code == 404:
        # No collection yet → no indexes. Fall through to create.
        existing_indexes = []
    else:
        print(f"  ⚠ Could not check existing search indexes: {resp.status_code} {resp.text}")

    if existing_indexes is not None:
        for idx in existing_indexes:
            if idx.get("name") == "vector_index":
                print("  ✓ Vector search index 'vector_index' already exists on events.knowledge_base")
                break
        else:
            # Create vector search index
            # search index create is name-idempotent. Allow 5xx retry.
            print("  Creating vector search index 'vector_index' on events.knowledge_base...")
            create_resp = api.post(
                f"/clusters/{cluster_name}/search/indexes",
                {
                    "collectionName": "knowledge_base",
                    "database": "events",
                    **search_index_def,
                },
                idempotent=True,
                api_version="application/vnd.atlas.2024-05-30+json",
            )
            if create_resp.ok or create_resp.status_code == 409:
                print("  ✓ Created vector search index 'vector_index'")
            else:
                print(f"  ⚠ Could not create vector search index: {create_resp.status_code} {create_resp.text}")

    # previously-defined Atlas Search indexes on
    # zone_anomalies and dispatch_log were removed because no caller
    # (dashboard, Flink SQL) issued $search against them. Existing
    # indexes on running clusters are not deleted here; teardown
    # drops them with the collection.

    # -- Collection indexes (via pymongo) --
    if not HAS_PYMONGO:
        print("  ⚠ pymongo not installed — skipping collection indexes")
        return

    if not (connection_string and username and password):
        print("  ⊘ No MongoDB credentials — skipping collection indexes")
        return

    uri = build_uri(connection_string, username, password)

    try:
        client = get_client(uri, app_name="streaming-agents-asp-indexes",
                            server_selection_timeout_ms=10000)
        client.admin.command("ping")
    except ConnectionFailure as e:
        print(f"  ⚠ Cannot connect to MongoDB for indexes: {e}")
        return

    # analytics.zone_traffic: unique compound index (required by ASP $merge on:) + TTL index
    zone_traffic = client["analytics"]["zone_traffic"]
    zone_traffic.create_index(
        [("zone", 1), ("window_start", -1)],
        name="zone_window_start_compound",
        unique=True,
    )
    # Re-deploy hygiene: legacy docs may have window_start as epoch-millis
    # Long. The TTL monitor only acts on BSON Date so those docs would
    # never expire. Purge legacy long-typed docs once on re-deploy so the
    # collection is uniformly Date going forward.
    try:
        legacy = zone_traffic.delete_many(
            {"window_start": {"$type": ["long", "int", "double"]}}
        )
        if legacy.deleted_count:
            print(f"  ✓ Purged {legacy.deleted_count} legacy epoch-millis rows "
                  f"from analytics.zone_traffic (TTL no-op rows)")
    except Exception as e:
        print(f"  ⚠ Could not purge legacy zone_traffic rows: {e}")
    zone_traffic.create_index(
        "window_start",
        name="window_start_ttl",
        expireAfterSeconds=7 * 24 * 3600,  # 7 days
    )
    print("  ✓ Created indexes on analytics.zone_traffic (unique compound + TTL)")

    # analytics.zone_anomalies: unique compound index (required by ASP $merge on:)
    zone_anomalies = client["analytics"]["zone_anomalies"]
    # Re-deploy hygiene: same epoch-millis purge as zone_traffic.
    try:
        legacy = zone_anomalies.delete_many(
            {"window_time": {"$type": ["long", "int", "double"]}}
        )
        if legacy.deleted_count:
            print(f"  ✓ Purged {legacy.deleted_count} legacy epoch-millis rows "
                  f"from analytics.zone_anomalies")
    except Exception as e:
        print(f"  ⚠ Could not purge legacy zone_anomalies rows: {e}")
    zone_anomalies.create_index(
        [("pickup_zone", 1), ("window_time", -1)],
        name="pickup_zone_window_time_compound",
        unique=True,
    )
    print("  ✓ Created indexes on analytics.zone_anomalies (unique compound)")

    # fleet.dispatch_log:
    # - UNIQUE compound on (pickup_zone, window_time) so the
    #     ASP $merge on:["pickup_zone","window_time"] enforces dedup.
    #   - dispatched_at desc index serves the dashboard "last 50 dispatches"
    #     query (sort by dispatched_at desc, no zone filter).
    dispatch_log = client["fleet"]["dispatch_log"]

    # Re-deploy hygiene: clusters that ran the older code carry the legacy
    # non-unique index pickup_zone_dispatched_at_compound. Drop it so the
    # collection is not paying write cost for an obsolete index.
    try:
        dispatch_log.drop_index("pickup_zone_dispatched_at_compound")
        print("  ✓ Dropped legacy index pickup_zone_dispatched_at_compound")
    except OperationFailure:
        pass  # Already absent — fresh cluster

    # Re-deploy hygiene: the previous $merge had no `on:` clause, so on
    # consumer reset every replay re-inserted rows under fresh ObjectIds.
    # Existing duplicates would block create_index(unique=True) with E11000.
    # Dedupe by (pickup_zone, window_time), keeping the most recent doc.
    _dedupe_dispatch_log(dispatch_log)

    dispatch_log.create_index(
        [("pickup_zone", 1), ("window_time", -1)],
        name="pickup_zone_window_time_unique",
        unique=True,
    )
    dispatch_log.create_index(
        [("dispatched_at", -1)],
        name="dispatched_at_desc",
    )
    print("  ✓ Created indexes on fleet.dispatch_log (unique + sort)")

    # fleet.vessel_catalog unique on vessel_id (the seeder uses
    # vessel_id as the upsert match key).
    vessel_catalog = client["fleet"]["vessel_catalog"]
    vessel_catalog.create_index(
        "vessel_id",
        name="vessel_id_unique",
        unique=True,
    )
    print("  ✓ Created unique index on fleet.vessel_catalog.vessel_id")

    # TTL on validation_dlq collections (30 days). ASP DLQ
    # writes include an `_ts` timestamp field by contract; we index it
    # with expireAfterSeconds.  If `_ts` is missing on a doc, the TTL
    # monitor leaves it in place (safe degradation).
    for db_name in ("events", "fleet"):
        dlq = client[db_name]["validation_dlq"]
        dlq.create_index(
            "_ts",
            name="dlq_ts_ttl",
            expireAfterSeconds=30 * 24 * 3600,
        )
    print("  ✓ Created 30-day TTL on events.validation_dlq, fleet.validation_dlq")

    # events.knowledge_base.document_id unique index used
    # to live in seed_events_calendar — that coupling meant any caller
    # invoking ensure_atlas_indexes WITHOUT the seeder (hypothetical
    # "indexes-only refresh") left the $merge on: document_id collection-
    # scanning. partial filter so legacy documents
    # (no document_id field) don't E11000 on null-null collision.
    # ASP $merge on:"document_id" REQUIRES a
    # NON-partial unique index. A partialFilterExpression unique index
    # does NOT satisfy MongoDB's $merge uniqueness validation —
    # event_knowledge_base_population FAILS with "Cannot find index to
    # verify that join fields will be unique". The partial filter
    # was added to tolerate legacy docs with no
    # document_id; instead we purge those (the seeders always set
    # document_id, so on a healthy cluster there are none) and install
    # a full unique index.
    kb_coll = client["events"]["knowledge_base"]
    try:
        purged = kb_coll.delete_many({"document_id": {"$exists": False}})
        if purged.deleted_count:
            print(f"  ✓ Purged {purged.deleted_count} knowledge_base docs "
                  f"missing document_id (would block full unique index)")
    except Exception as e:
        print(f"  ⚠ Could not purge null-document_id docs: {e}")
    # Drop a legacy PARTIAL index of the same name so the full unique
    # index below can be installed (create_index with different options
    # on an existing name raises).
    try:
        existing = kb_coll.index_information().get("document_id_unique")
        if existing is not None and existing.get("partialFilterExpression") is not None:
            kb_coll.drop_index("document_id_unique")
            print("  ✓ Dropped legacy PARTIAL document_id_unique index")
    except OperationFailure:
        pass
    kb_coll.create_index(
        "document_id", unique=True, name="document_id_unique",
    )
    print("  ✓ Ensured FULL unique index on events.knowledge_base.document_id")

    # native $jsonSchema validators applied via collMod with
    # validationLevel=moderate, validationAction=warn so existing writes
    # are not blocked during rollout.
    _apply_collection_validators(client)


def _dedupe_dispatch_log(coll) -> None:
    """Remove duplicate (pickup_zone, window_time) rows from fleet.dispatch_log.

    Old versions of this codebase had a $merge stage with no `on:` key,
    so MongoDB matched on _id and every Kafka replay re-inserted rows
    under fresh ObjectIds. When we now create a unique compound index on
    (pickup_zone, window_time), pre-existing duplicates would raise
    E11000. This helper deletes all but the most-recent row per key.
    """
    try:
        pipeline = [
            {"$match": {"pickup_zone": {"$exists": True}, "window_time": {"$exists": True}}},
            {"$sort": {"dispatched_at": -1}},
            {
                "$group": {
                    "_id": {"pickup_zone": "$pickup_zone", "window_time": "$window_time"},
                    "ids": {"$push": "$_id"},
                    "count": {"$sum": 1},
                }
            },
            {"$match": {"count": {"$gt": 1}}},
        ]
        duplicates = list(coll.aggregate(pipeline, allowDiskUse=True))
        if not duplicates:
            return
        ids_to_delete = []
        for group in duplicates:
            # Keep the first (most recent due to $sort -1), delete the rest
            ids_to_delete.extend(group["ids"][1:])
        if ids_to_delete:
            result = coll.delete_many({"_id": {"$in": ids_to_delete}})
            print(
                f"  ✓ Dedup'd {result.deleted_count} stale duplicate rows "
                f"in fleet.dispatch_log (across {len(duplicates)} keys)"
            )
    except Exception as e:
        # re-raise so we don't silently produce a
        # non-unique state. The subsequent create_index(unique=True)
        # would E11000 if duplicates remain; better to fail loudly here
        # with the actual dedup error so the operator can investigate.
        print(f"  ⚠ _dedupe_dispatch_log failed: {e}")
        raise


def _apply_collection_validators(client) -> None:
    """Apply native $jsonSchema validators.

    Uses validationAction='warn' so misshapen documents are logged but
    not rejected — safe to roll out without coordinated ASP changes.
    """
    validators = {
        ("events", "knowledge_base"): {
            "bsonType": "object",
            "required": ["document_id", "embedding", "chunk", "event_name", "zone"],
            "properties": {
                "document_id": {"bsonType": "string"},
                "embedding": {"bsonType": "array"},
                "chunk": {"bsonType": "string"},
                "event_name": {"bsonType": "string"},
                "zone": {"bsonType": "string"},
            },
        },
        ("events", "calendar"): {
            "bsonType": "object",
            "required": ["event_name", "zone", "description", "event_time_start"],
            "properties": {
                "event_name": {"bsonType": "string"},
                "zone": {"bsonType": "string"},
                "description": {"bsonType": "string"},
                "event_time_start": {"bsonType": "date"},
            },
        },
        ("fleet", "dispatch_log"): {
            # window_time is required for the $merge
            # on:["pickup_zone","window_time"] contract and the unique
            # compound index. Missing window_time would cause null-key
            # E11000 storms or silent rowloss.
            "bsonType": "object",
            "required": ["pickup_zone", "dispatch_summary", "window_time"],
            "properties": {
                "pickup_zone": {"bsonType": "string"},
                "dispatch_summary": {"bsonType": "string"},
                "window_time": {"bsonType": "date"},
            },
        },
    }
    for (db_name, coll_name), schema in validators.items():
        try:
            # Ensure collection exists; createCollection is a no-op if it does
            client[db_name].create_collection(coll_name)
        except Exception:
            pass  # CollectionInvalid means it already exists
        try:
            client[db_name].command({
                "collMod": coll_name,
                "validator": {"$jsonSchema": schema},
                "validationLevel": "moderate",
                "validationAction": "warn",
            })
            print(f"  ✓ Applied $jsonSchema validator to {db_name}.{coll_name}")
        except Exception as e:
            print(f"  ⚠ Could not apply validator to {db_name}.{coll_name}: {e}")


# -- Seed Data ----------------------------------------------------------------
SEED_EVENTS = [
    {
        "event_name": "Essence Music Festival",
        "zone": "CBD",
        "description": (
            "The Essence Music Festival is a massive annual music festival held "
            "in the Central Business District of New Orleans. Featuring top R&B, "
            "hip-hop, and jazz artists, the festival draws 25,000 attendees nightly "
            "to the Caesars Superdome and surrounding venues. Expect significant "
            "traffic congestion, surge pricing, and high ride demand in the CBD zone "
            "from late afternoon through midnight."
        ),
        "venue": "Caesars Superdome",
        "expected_attendance": 25000,
        "event_type": "music_festival",
        "impact_level": "high",
        "event_time_start_hour": 17,
        "event_time_start_min": 0,
        "event_time_end_hour": 23,
        "event_time_end_min": 30,
    },
    {
        "event_name": "French Quarter Festival",
        "zone": "French Quarter",
        "description": (
            "The French Quarter Festival is the largest free music festival in the "
            "South, spanning multiple stages throughout the historic French Quarter. "
            "With 15,000 attendees, expect heavy pedestrian traffic, road closures "
            "on Royal and Bourbon streets, and sustained ride demand from morning "
            "through evening in the French Quarter zone."
        ),
        "venue": "French Quarter (multiple stages)",
        "expected_attendance": 15000,
        "event_type": "music_festival",
        "impact_level": "high",
        "event_time_start_hour": 11,
        "event_time_start_min": 0,
        "event_time_end_hour": 21,
        "event_time_end_min": 0,
    },
    {
        "event_name": "Saints Game",
        "zone": "CBD",
        "description": (
            "New Orleans Saints NFL game at the Caesars Superdome in the CBD. "
            "With 73,000 fans, this creates the largest single-venue demand spike "
            "in the city. Pre-game traffic starts 2 hours before kickoff. Post-game "
            "exodus creates extreme ride demand within a 30-minute window. CBD and "
            "surrounding zones experience 3-5x normal ride volume."
        ),
        "venue": "Caesars Superdome",
        "expected_attendance": 73000,
        "event_type": "sporting_event",
        "impact_level": "critical",
        "event_time_start_hour": 19,
        "event_time_start_min": 0,
        "event_time_end_hour": 22,
        "event_time_end_min": 30,
    },
    {
        "event_name": "Mardi Gras Parade",
        "zone": "French Quarter",
        "description": (
            "Major Mardi Gras parade route through the French Quarter and along "
            "St. Charles Avenue. With 50,000 spectators lining the route, expect "
            "complete road closures along the parade path, heavy pedestrian traffic, "
            "and extremely high ride demand in all zones adjacent to the route. "
            "Surge pricing is virtually guaranteed from noon through late evening."
        ),
        "venue": "French Quarter & St. Charles Ave",
        "expected_attendance": 50000,
        "event_type": "parade",
        "impact_level": "critical",
        "event_time_start_hour": 12,
        "event_time_start_min": 0,
        "event_time_end_hour": 20,
        "event_time_end_min": 0,
    },
    {
        "event_name": "Jazz at Preservation Hall",
        "zone": "French Quarter",
        "description": (
            "Intimate jazz performance at the legendary Preservation Hall in the "
            "French Quarter. With only 500 attendees per show, this is a low-impact "
            "event that does not significantly affect zone-level ride demand. However, "
            "the surrounding Bourbon Street nightlife creates steady baseline demand "
            "in the French Quarter zone throughout the evening."
        ),
        "venue": "Preservation Hall",
        "expected_attendance": 500,
        "event_type": "concert",
        "impact_level": "low",
        "event_time_start_hour": 20,
        "event_time_start_min": 0,
        "event_time_end_hour": 23,
        "event_time_end_min": 0,
    },
    {
        "event_name": "Bayou Classic",
        "zone": "Uptown",
        "description": (
            "The Bayou Classic is a historic rivalry football game between Grambling "
            "State and Southern University, held at the Caesars Superdome but with "
            "most fan activities centered in the Uptown neighborhood. 30,000 attendees "
            "create high ride demand in the Uptown zone, with pre-game tailgating "
            "starting early afternoon and post-game celebrations continuing into "
            "the evening."
        ),
        "venue": "Uptown / Caesars Superdome",
        "expected_attendance": 30000,
        "event_type": "sporting_event",
        "impact_level": "high",
        "event_time_start_hour": 14,
        "event_time_start_min": 0,
        "event_time_end_hour": 18,
        "event_time_end_min": 0,
    },
    {
        "event_name": "Warehouse District Art Walk",
        "zone": "Warehouse District",
        "description": (
            "Monthly art gallery openings along Julia Street in the Warehouse "
            "District draw 8,000 visitors who walk between galleries, restaurants, "
            "and bars from early evening through late night. Combined with convention "
            "center events, the Warehouse District experiences sustained high ride "
            "demand throughout the evening as visitors move between venues and "
            "depart from the area's limited parking."
        ),
        "venue": "Julia Street Galleries & Convention Center",
        "expected_attendance": 8000,
        "event_type": "cultural",
        "impact_level": "high",
        "event_time_start_hour": 18,
        "event_time_start_min": 0,
        "event_time_end_hour": 23,
        "event_time_end_min": 0,
    },
    {
        "event_name": "Garden District Home & Garden Tour",
        "zone": "Garden District",
        "description": (
            "Annual tour of historic antebellum mansions in the Garden District "
            "attracts 12,000 visitors over a single day. The narrow streets and "
            "limited parking force most visitors to rely on ride-sharing. Peak "
            "demand occurs in late afternoon as tour groups finish and seek "
            "transportation to dinner destinations. The Garden District zone "
            "experiences 3x normal ride volume during event hours."
        ),
        "venue": "Garden District Historic Homes",
        "expected_attendance": 12000,
        "event_type": "cultural",
        "impact_level": "high",
        "event_time_start_hour": 10,
        "event_time_start_min": 0,
        "event_time_end_hour": 18,
        "event_time_end_min": 0,
    },
    {
        "event_name": "Frenchmen Street Live Music Festival",
        "zone": "Marigny",
        "description": (
            "The Frenchmen Street corridor in the Marigny hosts a multi-venue live "
            "music festival with 10,000 attendees flowing between clubs, bars, and "
            "outdoor stages. As the premier nightlife alternative to Bourbon Street, "
            "Marigny experiences extreme ride demand from evening through early morning. "
            "Limited street parking and narrow one-way streets make ride-sharing the "
            "primary transport mode for festival-goers."
        ),
        "venue": "Frenchmen Street Corridor",
        "expected_attendance": 10000,
        "event_type": "music_festival",
        "impact_level": "high",
        "event_time_start_hour": 19,
        "event_time_start_min": 0,
        "event_time_end_hour": 2,
        "event_time_end_min": 0,
    },
    {
        "event_name": "Bywater Biennale",
        "zone": "Bywater",
        "description": (
            "The Bywater Biennale is a large-scale art and performance festival "
            "across warehouses, parks, and studios in the Bywater neighborhood. "
            "With 7,000 attendees, the normally quiet residential area experiences "
            "significant ride demand spikes, especially in the evening as visitors "
            "attend performances and seek rides home. The Bywater's distance from "
            "downtown means longer ride times and higher surge pricing."
        ),
        "venue": "Bywater Arts District",
        "expected_attendance": 7000,
        "event_type": "cultural",
        "impact_level": "high",
        "event_time_start_hour": 16,
        "event_time_start_min": 0,
        "event_time_end_hour": 23,
        "event_time_end_min": 0,
    },
]


def seed_events_calendar(connection_string: str, username: str, password: str) -> None:
    """Seed events.calendar collection with 6 New Orleans events (upsert)."""
    print(f"\n{'='*60}")
    print("Step 4: Seed events.calendar")
    print(f"{'='*60}")

    if not HAS_PYMONGO:
        print("  ✗ pymongo not installed — skipping seed data.")
        print("    Install with: uv pip install pymongo")
        return

    uri = build_uri(connection_string, username, password)

    try:
        client = get_client(uri, app_name="streaming-agents-asp-seed-events",
                            server_selection_timeout_ms=10000)
        client.admin.command("ping")
    except ConnectionFailure as e:
        print(f"  ✗ Cannot connect to MongoDB: {e}")
        return

    db = client["events"]
    coll = db["calendar"]

    # events.knowledge_base.document_id unique index
    # moved to ensure_atlas_indexes. The seeder no longer creates
    # indexes for an OTHER collection.

    # support the (event_name, zone) upsert key with a unique
    # compound index. Without this, every upsert is a full collection scan
    # (fine at 10 docs, expensive if anyone extends the seed list).
    coll.create_index(
        [("event_name", 1), ("zone", 1)],
        unique=True, name="event_name_zone_unique",
    )

    now = datetime.now(timezone.utc)
    # Use today's date for event times
    base_date = now.replace(hour=0, minute=0, second=0, microsecond=0)

    upserted = 0
    for event in SEED_EVENTS:
        start = base_date.replace(
            hour=event["event_time_start_hour"],
            minute=event["event_time_start_min"],
        )
        end = base_date.replace(
            hour=event["event_time_end_hour"],
            minute=event["event_time_end_min"],
        )

        # $setOnInsert preserves created_at across reseeds, and
        # $currentDate updates updated_at.  Mutating updated_at on every
        # seeder run otherwise wakes the change-stream pipeline and forces
        # a Voyage AI re-embed of unchanged content.
        # document_id baked at seed time; ASP can't compute it.
        set_fields = {
            "event_name": event["event_name"],
            "zone": event["zone"],
            "description": event["description"],
            "event_time_start": start,
            "event_time_end": end,
            "venue": event["venue"],
            "expected_attendance": event["expected_attendance"],
            "event_type": event["event_type"],
            "impact_level": event["impact_level"],
            "document_id": _compute_document_id(event["event_name"], event["zone"]),
        }

        result = coll.update_one(
            {"event_name": event["event_name"], "zone": event["zone"]},
            {
                "$set": set_fields,
                "$setOnInsert": {"created_at": now},
                "$currentDate": {"updated_at": True},
            },
            upsert=True,
        )
        if result.upserted_id:
            print(f"  + Inserted: {event['event_name']} ({event['zone']})")
            upserted += 1
        else:
            print(f"  ~ Updated:  {event['event_name']} ({event['zone']})")

    print(f"\n  ✓ Seeded {len(SEED_EVENTS)} events ({upserted} new)")


def seed_vessel_catalog(connection_string: str, username: str, password: str) -> None:
    """Seed fleet.vessel_catalog collection with 31 vessels (upsert by vessel_id)."""
    print(f"\n{'='*60}")
    print("Step 4b: Seed fleet.vessel_catalog")
    print(f"{'='*60}")

    if not HAS_PYMONGO:
        print("  ✗ pymongo not installed — skipping vessel catalog seed.")
        return

    import json as _json
    from pathlib import Path

    data_file = Path(__file__).parent / "data" / "vessel_catalog.json"
    if not data_file.exists():
        print(f"  ✗ Seed file not found: {data_file}")
        return

    vessels = _json.loads(data_file.read_text())

    uri = build_uri(connection_string, username, password)

    try:
        client = get_client(uri, app_name="streaming-agents-asp-seed-vessels",
                            server_selection_timeout_ms=10000)
        client.admin.command("ping")
    except ConnectionFailure as e:
        print(f"  ✗ Cannot connect to MongoDB: {e}")
        return

    db = client["fleet"]
    coll = db["vessel_catalog"]

    # create the unique index BEFORE the upsert loop.
    # If the seed payload contains duplicate vessel_id entries (legacy
    # data or a future config bug), creating the index first surfaces
    # the conflict immediately rather than letting both rows insert
    # and then failing the index create at a separate code path with
    # E11000. Matches the seed_events_calendar pattern.
    coll.create_index("vessel_id", unique=True, name="vessel_id_unique")

    # preserve created_at if the seed payload carries one;
    # always stamp updated_at on each run.
    upserted = 0
    for vessel in vessels:
        set_fields = {k: v for k, v in vessel.items() if k != "created_at"}
        update = {
            "$set": set_fields,
            "$currentDate": {"updated_at": True},
        }
        if "created_at" in vessel:
            update["$setOnInsert"] = {"created_at": vessel["created_at"]}
        result = coll.update_one(
            {"vessel_id": vessel["vessel_id"]},
            update,
            upsert=True,
        )
        if result.upserted_id:
            upserted += 1

    print(f"  ✓ Seeded {len(vessels)} vessels ({upserted} new)")


# -- Programmatic API for deploy.py integration -------------------------------
def run_asp_setup(
    atlas_public_key: str,
    atlas_private_key: str,
    project_id: str,
    cluster_name: str,
    confluent_bootstrap_server: str,
    confluent_api_key: str,
    confluent_api_secret: str,
    voyage_api_key: str,
    mongodb_connection_string: str = "",
    mongodb_username: str = "",
    mongodb_password: str = "",
    skip_seed: bool = False,
    skip_processors: bool = False,
    schema_registry_url: str = "",
    schema_registry_key: str = "",
    schema_registry_secret: str = "",
    kafka_rest_endpoint: str = "",
    kafka_cluster_id: str = "",
    voyage_api_endpoint: str = VOYAGE_API_ENDPOINT_DEFAULT,
) -> bool:
    """Run ASP setup programmatically (called by deploy.py after terraform).

    Returns True on success, False on failure.
    """
    print("\nAtlas Stream Processing Setup")
    print("=" * 60)
    print(f"  Project ID:       {project_id}")
    print(f"  Cluster:          {cluster_name}")
    print(f"  Bootstrap Server: {confluent_bootstrap_server[:30]}...")
    print(f"  Voyage API Key:   {'*' * 8}{voyage_api_key[-4:]}")

    # ── Step 0: Preflight ────────────────────────────────────────────────
    # Verify ATLAS_CLUSTER_NAME exists in the configured Atlas project
    # BEFORE creating the ASP instance and connections. Without this
    # gate, three connection-creates (atlas_cluster, events_dlq,
    # fleet_dlq) all fail with the same 400 — the user gets the right
    # diagnosis only after seeing three near-identical errors mid-flow.
    cluster_check = check_atlas_cluster_exists({
        "ATLAS_PUBLIC_KEY":   atlas_public_key,
        "ATLAS_PRIVATE_KEY":  atlas_private_key,
        "ATLAS_PROJECT_ID":   project_id,
        "ATLAS_CLUSTER_NAME": cluster_name,
    })
    if cluster_check.status == "fail":
        print(f"\n  ✗ Atlas cluster preflight failed: {cluster_check.message}")
        if getattr(cluster_check, "remediation", None):
            print(f"    {cluster_check.remediation}")
        print(
            "\n  Refusing to create the ASP instance and connections — "
            "they would all fail downstream with the same root cause. "
            "Fix the cluster reference in .env and re-run "
            "`uv run deploy` (it will resume from asp_setup)."
        )
        return False
    if cluster_check.status == "warn":
        # Transient (network blip etc.) — log but proceed; AtlasAPI
        # internal retries will handle short-lived issues.
        print(f"\n  ⚠ Atlas cluster preflight warning: {cluster_check.message}")
        print("    Proceeding; AtlasAPI retries may still succeed.")

    api = AtlasAPI(atlas_public_key, atlas_private_key, project_id)

    try:
        # Step 1: ASP instance
        ensure_asp_instance(api, cluster_name)

        # Step 2: Connections
        ensure_connections(
            api,
            cluster_name=cluster_name,
            bootstrap_server=confluent_bootstrap_server,
            confluent_api_key=confluent_api_key,
            confluent_api_secret=confluent_api_secret,
            voyage_api_key=voyage_api_key,
            schema_registry_url=schema_registry_url,
            schema_registry_key=schema_registry_key,
            schema_registry_secret=schema_registry_secret,
            voyage_api_endpoint=voyage_api_endpoint,
        )

        # Step 2b: Kafka topics
        if not skip_processors:
            ensure_kafka_topics(
                kafka_rest_endpoint=kafka_rest_endpoint,
                cluster_id=kafka_cluster_id,
                confluent_api_key=confluent_api_key,
                confluent_api_secret=confluent_api_secret,
            )

        # Step 3: Seed data (before indexes, so collections exist)
        if not skip_seed and mongodb_connection_string and mongodb_username and mongodb_password:
            seed_events_calendar(mongodb_connection_string, mongodb_username, mongodb_password)
            seed_vessel_catalog(mongodb_connection_string, mongodb_username, mongodb_password)
        elif not skip_seed:
            print(f"\n{'='*60}")
            print("Step 3: Seed data")
            print(f"{'='*60}")
            print("  ⊘ Skipping — no MongoDB credentials provided")

        # Step 4: Atlas indexes (must run BEFORE processors -- $merge on: requires unique indexes)
        ensure_atlas_indexes(
            api,
            cluster_name=cluster_name,
            connection_string=mongodb_connection_string,
            username=mongodb_username,
            password=mongodb_password,
        )

        # Step 5: Processors (after indexes so $merge on: validation passes)
        processors_created = False
        if not skip_processors:
            processors_created = ensure_processors(api)

        # Step 6: Re-seed calendar to trigger change stream — ONLY when a
        # processor was newly created on this run. Re-seeding on every
        # asp-setup invocation mutates events.calendar.updated_at and
        # fires the change-stream re-embedding via Voyage AI for no
        # functional gain.
        if (processors_created
                and not skip_seed
                and not skip_processors
                and mongodb_connection_string
                and mongodb_username
                and mongodb_password):
            print(f"\n{'='*60}")
            print("Step 6: Re-seed calendar (trigger change stream)")
            print(f"{'='*60}")
            print("  (processors were newly created — waking the change stream)")
            time.sleep(5)
            seed_events_calendar(mongodb_connection_string, mongodb_username, mongodb_password)

        print(f"\n{'='*60}")
        print("  ✓ ASP setup complete!")
        print(f"{'='*60}\n")
        return True

    except Exception as e:
        print(f"\n  ✗ ASP setup failed: {e}")
        return False


# -- ASP Teardown (for destroy.py) --------------------------------------------
def run_asp_teardown(
    atlas_public_key: str,
    atlas_private_key: str,
    project_id: str,
) -> bool:
    """Tear down ASP resources: stop processors, delete instance.

    Returns True on success, False on failure.
    """
    print("\nAtlas Stream Processing Teardown")
    print("=" * 60)

    api = AtlasAPI(atlas_public_key, atlas_private_key, project_id)
    proc_api_ver = AtlasAPI.API_VERSION_PROCESSORS

    try:
        # Check if instance exists
        resp = api.get("/streams")
        if not resp.ok:
            print(f"  ⚠ Cannot list ASP instances: {resp.status_code}")
            return False

        instances = resp.json().get("results", [])
        instance_exists = any(i.get("name") == ASP_INSTANCE_NAME for i in instances)

        if not instance_exists:
            print(f"  ⊘ ASP instance '{ASP_INSTANCE_NAME}' not found — nothing to tear down")
            return True

        # Stop all processors
        resp = api.get(f"/streams/{ASP_INSTANCE_NAME}/processors", api_version=proc_api_ver)
        if resp.ok:
            processors = resp.json().get("results", [])
            for proc in processors:
                name = proc["name"]
                state = proc.get("state", "")
                if state in ("STARTED", "RUNNING"):
                    print(f"  Stopping processor '{name}'...")
                    stop_resp = api.post(
                        f"/streams/{ASP_INSTANCE_NAME}/processor/{name}:stop",
                        {},
                        api_version=proc_api_ver,
                    )
                    if stop_resp.ok or stop_resp.status_code == 409:
                        print(f"  ✓ Stopped '{name}'")
                    else:
                        print(f"  ⚠ Could not stop '{name}': {stop_resp.status_code}")

            # Drop all processors
            for proc in processors:
                name = proc["name"]
                print(f"  Deleting processor '{name}'...")
                del_resp = api.delete(
                    f"/streams/{ASP_INSTANCE_NAME}/processor/{name}",
                    api_version=proc_api_ver,
                )
                if del_resp.ok or del_resp.status_code == 404:
                    print(f"  ✓ Deleted '{name}'")
                else:
                    print(f"  ⚠ Could not delete '{name}': {del_resp.status_code}")

        # Delete instance
        print(f"\n  Deleting ASP instance '{ASP_INSTANCE_NAME}'...")
        del_resp = api.delete(f"/streams/{ASP_INSTANCE_NAME}")
        if del_resp.ok or del_resp.status_code == 404:
            print(f"  ✓ Deleted instance '{ASP_INSTANCE_NAME}'")
        else:
            print(f"  ⚠ Could not delete instance: {del_resp.status_code} {del_resp.text}")

        print(f"\n{'='*60}")
        print("  ✓ ASP teardown complete!")
        print(f"{'='*60}\n")
        return True

    except Exception as e:
        print(f"\n  ✗ ASP teardown failed: {e}")
        return False


# -- CLI ----------------------------------------------------------------------
def _load_env_defaults() -> dict:
    """Load defaults from .env if available."""
    here = Path(__file__).resolve().parent
    for p in [here, *here.parents]:
        env_file = p / ".env"
        if env_file.exists():
            try:
                from dotenv import dotenv_values
                return {k: v for k, v in dotenv_values(env_file).items() if v}
            except ImportError:
                return {}
    return {}


def main() -> None:
    """Main entry point."""
    env = _load_env_defaults()

    parser = argparse.ArgumentParser(
        prog="asp-setup",
        description="Provision Atlas Stream Processing resources",
    )
    parser.add_argument(
        "--atlas-public-key",
        default=env.get("ATLAS_PUBLIC_KEY", ""),
        help="Atlas Admin API public key",
    )
    parser.add_argument(
        "--atlas-private-key",
        default=env.get("ATLAS_PRIVATE_KEY", ""),
        help="Atlas Admin API private key",
    )
    parser.add_argument(
        "--project-id",
        default=env.get("ATLAS_PROJECT_ID", ""),
        help="Atlas project ID",
    )
    parser.add_argument(
        "--cluster-name",
        default=env.get("ATLAS_CLUSTER_NAME", "Cluster0"),
        help="Atlas cluster name (default: Cluster0)",
    )
    parser.add_argument(
        "--confluent-bootstrap-server",
        default=env.get("CONFLUENT_BOOTSTRAP_SERVER", ""),
        help="Confluent Cloud bootstrap server (host:port)",
    )
    parser.add_argument(
        "--confluent-api-key",
        default=env.get("CONFLUENT_KAFKA_API_KEY", ""),
        help="Confluent Cloud Kafka API key",
    )
    parser.add_argument(
        "--confluent-api-secret",
        default=env.get("CONFLUENT_KAFKA_API_SECRET", ""),
        help="Confluent Cloud Kafka API secret",
    )
    parser.add_argument(
        "--kafka-rest-endpoint",
        default=env.get("CONFLUENT_KAFKA_REST_ENDPOINT", ""),
        help="Confluent Kafka REST API endpoint (for topic pre-creation)",
    )
    parser.add_argument(
        "--kafka-cluster-id",
        default=env.get("CONFLUENT_KAFKA_CLUSTER_ID", ""),
        help="Confluent Kafka cluster ID (for topic pre-creation)",
    )
    parser.add_argument(
        "--voyage-api-key",
        default=env.get("TF_VAR_voyage_api_key", env.get("VOYAGE_API_KEY", "")),
        help="Voyage AI API key",
    )
    parser.add_argument(
        "--voyage-api-endpoint",
        default=env.get("TF_VAR_voyage_api_endpoint", VOYAGE_API_ENDPOINT_DEFAULT),
        help="Voyage AI embeddings endpoint URL "
             f"(default: {VOYAGE_API_ENDPOINT_DEFAULT})",
    )
    parser.add_argument(
        "--schema-registry-url",
        default=env.get("CONFLUENT_SCHEMA_REGISTRY_URL", ""),
        help="Confluent Schema Registry URL (required for Avro deserialization in Pipelines 4 & 5)",
    )
    parser.add_argument(
        "--schema-registry-key",
        default=env.get("CONFLUENT_SCHEMA_REGISTRY_KEY", ""),
        help="Confluent Schema Registry API key",
    )
    parser.add_argument(
        "--schema-registry-secret",
        default=env.get("CONFLUENT_SCHEMA_REGISTRY_SECRET", ""),
        help="Confluent Schema Registry API secret",
    )
    parser.add_argument(
        "--mongodb-connection-string",
        default=env.get("TF_VAR_mongodb_connection_string", ""),
        help="MongoDB connection string for seeding (optional -- uses Atlas API cluster if not set)",
    )
    parser.add_argument(
        "--mongodb-username",
        default=env.get("TF_VAR_mongodb_username", ""),
        help="MongoDB username for seeding",
    )
    parser.add_argument(
        "--mongodb-password",
        default=env.get("TF_VAR_mongodb_password", ""),
        help="MongoDB password for seeding",
    )
    parser.add_argument(
        "--skip-seed", action="store_true",
        help="Skip seeding events.calendar",
    )
    parser.add_argument(
        "--skip-processors", action="store_true",
        help="Skip creating/starting processors (connections only)",
    )
    parser.add_argument(
        "--seed-only", action="store_true",
        help="Only seed events.calendar (skip ASP provisioning entirely)",
    )

    args = parser.parse_args()

    # --seed-only: bypass ASP provisioning, just seed events.calendar
    if args.seed_only:
        mongo_conn = args.mongodb_connection_string
        mongo_user = args.mongodb_username
        mongo_pass = args.mongodb_password
        if not (mongo_conn and mongo_user and mongo_pass):
            print("Error: --seed-only requires --mongodb-connection-string, --mongodb-username, --mongodb-password")
            print("Set them via CLI flags or in .env")
            sys.exit(1)
        seed_events_calendar(mongo_conn, mongo_user, mongo_pass)
        seed_vessel_catalog(mongo_conn, mongo_user, mongo_pass)
        sys.exit(0)

    # Validate required arguments
    required = {
        "atlas-public-key": args.atlas_public_key,
        "atlas-private-key": args.atlas_private_key,
        "project-id": args.project_id,
        "confluent-bootstrap-server": args.confluent_bootstrap_server,
        "confluent-api-key": args.confluent_api_key,
        "confluent-api-secret": args.confluent_api_secret,
        "voyage-api-key": args.voyage_api_key,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"Error: Missing required arguments: {', '.join(f'--{m}' for m in missing)}")
        print("Set them via CLI flags or in .env")
        sys.exit(1)

    print("Atlas Stream Processing Setup")
    print("=" * 60)
    print(f"  Project ID:       {args.project_id}")
    print(f"  Cluster:          {args.cluster_name}")
    print(f"  Bootstrap Server: {args.confluent_bootstrap_server[:30]}...")
    print(f"  Voyage API Key:   {'*' * 8}{args.voyage_api_key[-4:]}")

    api = AtlasAPI(args.atlas_public_key, args.atlas_private_key, args.project_id)

    # Step 1: ASP instance
    ensure_asp_instance(api, args.cluster_name)

    # Step 2: Connections
    ensure_connections(
        api,
        cluster_name=args.cluster_name,
        bootstrap_server=args.confluent_bootstrap_server,
        confluent_api_key=args.confluent_api_key,
        confluent_api_secret=args.confluent_api_secret,
        voyage_api_key=args.voyage_api_key,
        schema_registry_url=args.schema_registry_url,
        schema_registry_key=args.schema_registry_key,
        schema_registry_secret=args.schema_registry_secret,
        voyage_api_endpoint=args.voyage_api_endpoint,
    )

    # Step 2b: Kafka topics (required by $emit before processors start)
    if not args.skip_processors:
        kafka_rest_endpoint = args.kafka_rest_endpoint
        kafka_cluster_id = args.kafka_cluster_id

        # Fall back to terraform state if not in .env.
        if not kafka_rest_endpoint or not kafka_cluster_id:
            try:
                from scripts.common.terraform import get_project_root
                from scripts.common.terraform_outputs import get_core_outputs
                _outputs = get_core_outputs(get_project_root(strict=False))
                if _outputs:
                    if not kafka_rest_endpoint:
                        kafka_rest_endpoint = _outputs.get("confluent_kafka_cluster_rest_endpoint", {}).get("value", "")
                    if not kafka_cluster_id:
                        kafka_cluster_id = _outputs.get("confluent_kafka_cluster_id", {}).get("value", "")
            except Exception:
                pass

        ensure_kafka_topics(
            kafka_rest_endpoint=kafka_rest_endpoint,
            cluster_id=kafka_cluster_id,
            confluent_api_key=args.confluent_api_key,
            confluent_api_secret=args.confluent_api_secret,
        )

    # Step 3: Seed data (before indexes, so collections exist)
    if not args.skip_seed:
        mongo_conn = args.mongodb_connection_string
        mongo_user = args.mongodb_username
        mongo_pass = args.mongodb_password
        if mongo_conn and mongo_user and mongo_pass:
            seed_events_calendar(mongo_conn, mongo_user, mongo_pass)
            seed_vessel_catalog(mongo_conn, mongo_user, mongo_pass)
        else:
            print(f"\n{'='*60}")
            print("Step 3: Seed data")
            print(f"{'='*60}")
            print("  ⊘ Skipping — no MongoDB credentials provided")
            print("    Use --mongodb-connection-string, --mongodb-username, --mongodb-password")
    else:
        print("\n  ⊘ Skipping seed data (--skip-seed)")

    # Step 4: Atlas indexes (must run BEFORE processors -- $merge on: requires unique indexes)
    ensure_atlas_indexes(
        api,
        cluster_name=args.cluster_name,
        connection_string=args.mongodb_connection_string,
        username=args.mongodb_username,
        password=args.mongodb_password,
    )

    # Step 5: Processors (after indexes so $merge on: validation passes)
    if not args.skip_processors:
        ensure_processors(api)
    else:
        print("\n  ⊘ Skipping processors (--skip-processors)")

    print(f"\n{'='*60}")
    print("  ✓ ASP setup complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
