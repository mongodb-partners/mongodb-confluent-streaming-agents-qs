"""Confluent Cloud Flink REST API client.

Encapsulates auth header construction, statement CRUD, polling,
retry/backoff, and FAILED-state detection.

Usage:
    client = FlinkRestClient.from_env(env)
    client.submit("zone-traffic-sink-insert", sql, expect_phase="RUNNING", timeout=120)
    client.delete_and_wait("anomaly-detection-insert")
    statements = client.list(prefix="boat_dispatch")
    client.drop_table("ride_requests", if_exists=True)
"""

from __future__ import annotations

import base64
import json
import socket
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Protocol

from scripts.common import cli_output


# ---------------------------------------------------------------------------
# Protocol — used by tests to verify mock parity
# ---------------------------------------------------------------------------

class FlinkRestProtocol(Protocol):
    def submit(self, name: str, sql: str, properties: dict | None = None,
               expect_phase: str = "RUNNING", timeout: int = 120) -> dict: ...
    def get(self, name: str) -> dict | None: ...
    def delete(self, name: str) -> None: ...
    def delete_and_wait(self, name: str, timeout: int = 60) -> None: ...
    def wait_for_deletion(self, name: str, timeout: int = 30,
                          raise_on_timeout: bool = False) -> bool: ...
    def list(self, prefix: str | None = None,
             phase: str | None = None) -> list[dict]: ...
    def drop_table(self, table_or_view: str, if_exists: bool = True) -> None: ...
    def wait_for_phase(self, name: str, phase: str, timeout: int) -> dict: ...
    def force_failed_recreate(self, name: str, sql: str,
                              properties: dict | None = None) -> dict: ...


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FlinkRestClient:
    rest_endpoint: str
    api_key: str
    api_secret: str
    org_id: str
    env_id: str
    compute_pool_id: str
    service_account_id: str
    catalog: str
    database: str

    # ── Construction ──────────────────────────────────────────────────────
    @classmethod
    def from_env(cls, env: dict) -> "FlinkRestClient":
        """Build from .env / terraform-output values.

        Required keys (all values must be non-empty strings):
            CONFLUENT_FLINK_REST_ENDPOINT
            CONFLUENT_FLINK_API_KEY
            CONFLUENT_FLINK_API_SECRET
            CONFLUENT_ORG_ID
            CONFLUENT_ENV_ID
            CONFLUENT_FLINK_COMPUTE_POOL_ID
            CONFLUENT_SERVICE_ACCOUNT_ID
            CONFLUENT_FLINK_CATALOG
            CONFLUENT_FLINK_DATABASE
        """
        required = {
            "rest_endpoint":       env.get("CONFLUENT_FLINK_REST_ENDPOINT", ""),
            "api_key":             env.get("CONFLUENT_FLINK_API_KEY", ""),
            "api_secret":          env.get("CONFLUENT_FLINK_API_SECRET", ""),
            "org_id":              env.get("CONFLUENT_ORG_ID", ""),
            "env_id":              env.get("CONFLUENT_ENV_ID", ""),
            "compute_pool_id":     env.get("CONFLUENT_FLINK_COMPUTE_POOL_ID", ""),
            "service_account_id":  env.get("CONFLUENT_SERVICE_ACCOUNT_ID", ""),
            "catalog":             env.get("CONFLUENT_FLINK_CATALOG", ""),
            "database":            env.get("CONFLUENT_FLINK_DATABASE", ""),
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"FlinkRestClient.from_env: missing keys {missing}")
        return cls(**required)

    # ── Internal: URL + auth ──────────────────────────────────────────────
    def _auth_header(self) -> str:
        token = base64.b64encode(f"{self.api_key}:{self.api_secret}".encode()).decode()
        return f"Basic {token}"

    def _statement_url(self, name: str = "") -> str:
        base = (
            f"{self.rest_endpoint}/sql/v1/organizations/{self.org_id}"
            f"/environments/{self.env_id}/statements"
        )
        return f"{base}/{name}" if name else base

    def _headers(self) -> dict:
        return {
            "Content-Type":  "application/json",
            "Authorization": self._auth_header(),
        }

    # ── Internal: HTTP ────────────────────────────────────────────────────
    def _get(self, url: str, timeout: int = 15) -> dict | None:
        req = urllib.request.Request(url, method="GET", headers=self._headers())
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    def _post(self, url: str, body: bytes, timeout: int = 30) -> dict:
        req = urllib.request.Request(url, data=body, method="POST", headers=self._headers())
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())

    def _delete(self, url: str, timeout: int = 15) -> None:
        req = urllib.request.Request(url, method="DELETE", headers=self._headers())
        try:
            urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return  # already gone
            raise

    def _post_with_retry(self, url: str, body: bytes, timeout: int = 30) -> dict:
        """POST with 3-attempt retry on 429/5xx + transient network errors;
        backoff schedule [3, 6, 12]s. Mirrors the existing _submit_statement
        behavior bit-for-bit.

        on HTTP 429, honor the Retry-After header
        when it's larger than the static backoff.

        Only network-class exceptions are retried — client-side bugs
        (ImportError, AttributeError, etc.) propagate immediately rather
        than wasting 21s of backoff before surfacing.
        """
        max_attempts = 3
        backoff = [3, 6, 12]
        for attempt in range(max_attempts):
            try:
                return self._post(url, body, timeout=timeout)
            except urllib.error.HTTPError as e:
                retriable = (e.code == 429) or (e.code >= 500)
                if retriable and attempt < max_attempts - 1:
                    sleep_s = backoff[attempt]
                    # parse Retry-After if 429.
                    if e.code == 429:
                        try:
                            ra = e.headers.get("Retry-After") if e.headers else None
                            if ra:
                                ra_s = int(ra)
                                if ra_s > sleep_s:
                                    sleep_s = ra_s
                        except (TypeError, ValueError):
                            pass  # malformed Retry-After — ignore
                    time.sleep(sleep_s)
                    continue
                raise
            except (urllib.error.URLError, OSError, socket.timeout):
                if attempt < max_attempts - 1:
                    time.sleep(backoff[attempt])
                    continue
                raise
        raise RuntimeError("_post_with_retry: unreachable")

    # ── Public: CRUD + polling ────────────────────────────────────────────
    def get(self, name: str) -> dict | None:
        return self._get(self._statement_url(name))

    def delete(self, name: str) -> None:
        cli_output.info(f"[flink] DELETE statement {name}")
        self._delete(self._statement_url(name))

    def delete_and_wait(self, name: str, timeout: int = 60) -> None:
        """DELETE then poll until 404 (or timeout).

        timeout raises `TimeoutError` rather than
        warn-and-continue. A silent timeout enables the create-while-
        deleting race: creating a new statement with the same name while
        the old one is still DELETING may silently fail or get lost.

        Default timeout is 60s because the DELETING phase can last
        10-30s under load.
        """
        try:
            self._delete(self._statement_url(name))
        except Exception as e:
            cli_output.warn(f"[flink] DELETE {name} raised: {e} (continuing to poll)")
        self.wait_for_deletion(name, timeout=timeout, raise_on_timeout=True)

    def wait_for_deletion(
        self, name: str, timeout: int = 30,
        raise_on_timeout: bool = False,
    ) -> bool:
        """Poll for a statement to disappear (404).

        consolidated from previously-duplicated 14-line
        poll loops in `deploy._wait_for_deletion` and
        `pipeline_reset._wait_for_statement_gone`. Returns True when
        the statement is gone, False on timeout (unless
        `raise_on_timeout=True`, in which case TimeoutError is raised
        — matches the `delete_and_wait` contract).
        """
        url = self._statement_url(name)
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(3)
            req = urllib.request.Request(url, method="GET", headers=self._headers())
            try:
                urllib.request.urlopen(req, timeout=10)
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return True
            except Exception:
                pass  # network blip — keep polling
        if raise_on_timeout:
            raise TimeoutError(
                f"Flink statement {name!r} still in DELETING phase after "
                f"{timeout}s. Refusing to proceed — a subsequent CREATE "
                f"would race the DELETE and may silently fail "
                f". Re-run after waiting, or delete "
                f"manually via the Confluent Cloud console."
            )
        return False

    def list(
        self,
        prefix: str | None = None,
        phase: str | None = None,
    ) -> list[dict]:
        """List statements; optionally filter by name prefix and/or phase."""
        data = self._get(self._statement_url())
        if not data:
            return []
        items = data.get("data", []) if isinstance(data, dict) else []
        out = []
        for item in items:
            name = item.get("name", "")
            ph = item.get("status", {}).get("phase", "")
            if prefix is not None and not name.startswith(prefix):
                continue
            if phase is not None and ph != phase:
                continue
            out.append(item)
        return out

    def wait_for_phase(self, name: str, phase: str, timeout: int) -> dict:
        """Poll get(name) until status.phase == `phase` or timeout. Returns
        the final response. Raises TimeoutError if the phase never reached.

        check phase BEFORE sleeping. DDL statements
        often complete instantly; the previous "sleep-then-check" pattern
        wasted up to 3s per statement (~21s across the 7 deploys-time
        statements).
        """
        deadline = time.time() + timeout
        last: dict | None = None
        first_iter = True
        while time.time() < deadline:
            if not first_iter:
                time.sleep(3)
            first_iter = False
            try:
                last = self.get(name)
            except Exception:
                continue
            if last is None:
                continue
            cur = last.get("status", {}).get("phase", "")
            if cur == phase:
                return last
            if cur == "FAILED":
                detail = last.get("status", {}).get("detail", "")
                raise RuntimeError(f"{name} reached FAILED: {detail}")
        raise TimeoutError(f"{name} did not reach phase {phase} within {timeout}s")

    def submit(
        self,
        name: str,
        sql: str,
        properties: dict | None = None,
        expect_phase: str = "RUNNING",
        timeout: int = 120,
    ) -> dict:
        """Create a statement (delete-and-recreate if FAILED), then poll for `expect_phase`.

        Retries the POST on 429/5xx (3 attempts, [3, 6, 12]s backoff).
        """
        cli_output.info(f"[flink] SUBMIT {name} (expect={expect_phase})")
        existing = self.get(name)
        if existing:
            cur = existing.get("status", {}).get("phase", "")
            if cur == expect_phase:
                cli_output.info(f"[flink] {name} already in {expect_phase}, skipping")
                return existing
            if cur in ("FAILED", "STOPPED", "DELETING", "COMPLETED"):
                cli_output.info(f"[flink] {name} in {cur}; deleting before recreate")
                self.delete_and_wait(name)

        body_dict = {
            "name": name,
            "spec": {
                "statement": sql,
                "properties": {
                    "sql.current-catalog":  self.catalog,
                    "sql.current-database": self.database,
                    **(properties or {}),
                },
                "compute_pool_id": self.compute_pool_id,
                "principal":       self.service_account_id,
            },
        }
        body = json.dumps(body_dict).encode()
        self._post_with_retry(self._statement_url(), body)
        return self.wait_for_phase(name, expect_phase, timeout=timeout)

    def drop_table(self, table_or_view: str, if_exists: bool = True) -> None:
        """Submit a synchronous DROP TABLE (or DROP VIEW) statement.

        Uses a unique throwaway name to avoid colliding with prior drops.

        Error policy: 404 (table already gone) is logged and swallowed —
        callers depend on idempotency. Auth (401/403) and 5xx errors are
        re-raised so a deploy-time misconfiguration doesn't surface only
        as a warning. Network errors propagate after the retry layer.
        """
        kw = "IF EXISTS " if if_exists else ""
        sql = f"DROP TABLE {kw}`{self.catalog}`.`{self.database}`.`{table_or_view}`;"
        # Throwaway statement name — timestamp-based to avoid collision
        drop_name = f"drop-{table_or_view.replace('_', '-')}-{int(time.time())}"
        body = json.dumps({
            "name": drop_name,
            "spec": {
                "statement": sql,
                "properties": {
                    "sql.current-catalog":  self.catalog,
                    "sql.current-database": self.database,
                },
                "compute_pool_id": self.compute_pool_id,
                "principal":       self.service_account_id,
            },
        }).encode()
        try:
            self._post(self._statement_url(), body)
            cli_output.info(f"[flink] DROP TABLE {kw}{table_or_view}")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                cli_output.info(f"[flink] DROP TABLE {table_or_view}: 404 (already gone)")
                return
            cli_output.error(
                f"[flink] drop_table({table_or_view}) HTTP {e.code}: {e.reason}"
            )
            raise

    def force_failed_recreate(
        self,
        name: str,
        sql: str,
        properties: dict | None = None,
    ) -> dict:
        """For a statement currently in FAILED state: delete and recreate.

        this method is intentionally narrow. If the
        statement is not in FAILED, callers should use ``submit()``
        directly — the old "fall through to submit()" behaviour made
        this method a synonym for submit() that could mask real bugs.

        Raises ValueError if the statement is missing or in any phase
        other than FAILED.
        """
        existing = self.get(name)
        cur_phase = (existing or {}).get("status", {}).get("phase", "")
        if cur_phase != "FAILED":
            raise ValueError(
                f"force_failed_recreate({name!r}): statement is in phase "
                f"{cur_phase!r}, not FAILED. Use submit() instead."
            )
        self.delete_and_wait(name)
        return self.submit(name, sql, properties=properties)
