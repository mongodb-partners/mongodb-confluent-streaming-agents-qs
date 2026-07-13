#!/usr/bin/env python3
"""Deploy script — single source of truth in .env.

Usage:
    uv run deploy                   # Auto-detect: full flow or quick-deploy
    uv run deploy --full            # Force full setup flow
    uv run deploy --plain           # Plain text mode (no rich/questionary UI)
    uv run deploy --edit            # Edit a saved variable (menu)
    uv run deploy --edit <key>      # Edit directly: cloud, confluent-keys,
                                    #                cloud-creds, atlas, email
"""

import argparse
from scripts.common.http_auth import basic_auth_token
import os
import secrets
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from scripts.common import cli_output

# ── Optional enhanced libraries (degrade gracefully if missing) ───────────────
try:
    from rich.console import Console
    from rich.panel import Panel

    _console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

try:
    import questionary

    HAS_QUESTIONARY = True
    try:
        import questionary.prompts.common as _qpc

        _qpc.INDICATOR_SELECTED = "[x]"
        _qpc.INDICATOR_UNSELECTED = "[ ]"
    except (ImportError, AttributeError):
        pass
    QSTYLE = questionary.Style(
        [
            ("qmark", "fg:#5f87ff bold"),
            ("question", "bold"),
            ("answer", "fg:#5f87ff bold"),
            ("pointer", "fg:#5f87ff bold"),
            ("highlighted", "fg:#ffffff bg:#005faf bold"),
            ("selected", "fg:#5f87ff bold"),
            ("instruction", "fg:#858585"),
            ("text", "bold"),
            ("disabled", "fg:#858585 italic"),
        ]
    )
except ImportError:
    HAS_QUESTIONARY = False
    QSTYLE = None

# ── Constants ─────────────────────────────────────────────────────────────────
VERSION = "1.0.0"

# ── Non-interactive mode ──────────────────────────────────────────
# Set to True by main() when --non-interactive / -y is passed. Consulted by
# every interactive surface (_select, _text, show_review_and_confirm,
# _check_bedrock_creds, _resume_prompt) so unattended runs never prompt and
# never block on stdin, regardless of whether stdout is a TTY.
_NON_INTERACTIVE: bool = False


def _is_non_interactive() -> bool:
    """Return True when --non-interactive / -y was passed."""
    return _NON_INTERACTIVE


# Credential keys that --non-interactive will hydrate from os.environ into
# .env before validating. Order matters only for the missing-creds error
# message.
#
# MCP URL/token + DEPLOY_PHASE state keys are NOT in
# this list. They are produced by THIS deploy (or cleared on destroy);
# silently materializing them from process env vars left over from an
# earlier deploy targeting a different MCP service caused
# `dispatch-insert` to bind to a dead URL (drift detection saw no
# change). Operators who want to reuse a specific MCP service must
# explicitly populate `.env` rather than rely on env-var shadows.
_NONINTERACTIVE_HYDRATE_KEYS: tuple = (
    # Confluent Cloud
    "TF_VAR_confluent_cloud_api_key",
    "TF_VAR_confluent_cloud_api_secret",
    # AWS Bedrock
    "TF_VAR_aws_bedrock_access_key",
    "TF_VAR_aws_bedrock_secret_key",
    "TF_VAR_aws_session_token",
    # MongoDB Atlas (database — BYO cluster path)
    "TF_VAR_mongodb_connection_string",
    "TF_VAR_mongodb_username",
    "TF_VAR_mongodb_password",
    # Voyage AI
    "TF_VAR_voyage_api_key",
    # Atlas Admin API
    "ATLAS_PUBLIC_KEY",
    "ATLAS_PRIVATE_KEY",
    "ATLAS_PROJECT_ID",
    "ATLAS_CLUSTER_NAME",
    # Optional inputs the operator may need to override
    "TF_VAR_owner_email",
    "TF_VAR_cloud_region",
    "TF_VAR_bedrock_model_id",
    "TF_VAR_voyage_api_endpoint",
    # Atlas cluster provisioning (when create_atlas_cluster=true)
    "TF_VAR_create_atlas_cluster",
    "TF_VAR_atlas_db_username",
    "TF_VAR_atlas_db_password",
    # NOTE: TF_VAR_mcp_server_url and TF_VAR_mcp_auth_token deliberately
    # omitted — H-NEW-3. They live in .env when deploy.py wrote them; a
    # CI runner's stale env var must NOT silently overwrite them.
)


def _hydrate_env_from_environment() -> int:
    """Copy known credential keys from os.environ into .env.

    Only writes keys that are present in os.environ AND not already set
    in .env (existing .env values win — operators can override a single
    credential via the file without an env-var shadow surprise).

    Returns the number of keys written. Safe to call multiple times.
    """
    existing = _load_env()
    pairs: dict = {}
    for key in _NONINTERACTIVE_HYDRATE_KEYS:
        if existing.get(key):
            continue
        val = os.environ.get(key)
        if val:
            pairs[key] = val
    if pairs:
        _save_env_many(pairs)
    return len(pairs)


def _missing_required_credentials(env: dict) -> list:
    """Return the ordered list of required credential keys missing from `env`.

    Mirrors _is_ready()'s checks but returns the failing keys instead of
    a bool — used by --non-interactive to produce an actionable error.


    when TF_VAR_create_atlas_cluster=true, the
    MongoDB connection string + username + password are PRODUCED by
    terraform/atlas apply (see _persist_atlas_cluster_connection_string)
    rather than supplied by the user. Requiring them up-front blocks the
    documented `uv run deploy -y` + create-cluster combination — the
    interactive flow handles this by letting `_is_ready=False` fall
    through to `run_full_flow`. Mirror that here.
    """
    missing: list = []
    creating_cluster = (env.get("TF_VAR_create_atlas_cluster") or "").lower() == "true"
    for k in _REQUIRED_KEYS + _AWS_KEYS + _VOYAGE_KEYS + _ATLAS_ADMIN_KEYS:
        if not env.get(k):
            missing.append(k)
    if creating_cluster:
        # Atlas DB user credentials are required to provision the cluster.
        for k in ("TF_VAR_atlas_db_username", "TF_VAR_atlas_db_password"):
            if not env.get(k):
                missing.append(k)
    else:
        # BYO cluster — operator must supply the connection details.
        for k in _MONGODB_KEYS:
            if not env.get(k):
                missing.append(k)
    return missing


# ── Phase order for real DEPLOY_PHASE resume ─────────────────────
# WORK_PHASES is the canonical iteration order for run_deployment. Each phase
# corresponds to one _save_env("DEPLOY_PHASE", ...) write site. `complete` is
# the terminal marker, NOT a runnable phase — it's stored separately.
WORK_PHASES: tuple = (
    "atlas_terraform",  # only runs when TF_VAR_create_atlas_cluster=true
    "mcp_server",
    "terraform",
    "credentials",
    "publish_data",
    "asp_setup",
    "flink_dml",
)
COMPLETE_MARKER: str = "complete"


def _phase_index(phase: str) -> int:
    """Return the position of `phase` in WORK_PHASES.

    Raises ValueError for unknown values (including "complete" — that's a
    terminal marker, not a runnable phase). Use _next_work_phase if you
    need a forgiving lookup.
    """
    if phase not in WORK_PHASES:
        raise ValueError(f"unknown work phase: {phase!r}")
    return WORK_PHASES.index(phase)


def _next_work_phase(current: str) -> str | None:
    """Return the next work phase after `current`.

    - If `current` is the last work phase OR `complete`, returns None.
    - If `current` is unknown (including ""), returns WORK_PHASES[0] so a
      caller suggesting a resume from an unrecognized state lands on the
      beginning rather than crashing.
    """
    if current == COMPLETE_MARKER:
        return None
    if current not in WORK_PHASES:
        return WORK_PHASES[0]
    idx = WORK_PHASES.index(current)
    if idx + 1 >= len(WORK_PHASES):
        return None
    return WORK_PHASES[idx + 1]


def _should_run_phase(phase: str, env: dict, args) -> bool:
    """Phase guard with precedence: --force > --from-phase > DEPLOY_PHASE.

    `phase` MUST be a member of WORK_PHASES.
    """
    if getattr(args, "force", False):
        return True
    from_phase = getattr(args, "from_phase", None)
    if from_phase:
        return _phase_index(phase) >= _phase_index(from_phase)
    last = (env or {}).get("DEPLOY_PHASE", "")
    if last == COMPLETE_MARKER:
        # Default behavior on a complete deploy: every phase is skipped.
        # Use --force or --from-phase to override.
        return False
    if last in WORK_PHASES:
        return _phase_index(phase) > _phase_index(last)
    return True


def _resume_prompt(env: dict, args) -> str | None:
    """Interactive resume prompt.

    Inspects DEPLOY_PHASE in env. If an in-progress / complete deploy is
    detected AND no --force / --from-phase was supplied, ask the user how
    to proceed. Mutates `args.from_phase` or `args.force` based on the
    response. Returns a string sentinel describing the choice, or None
    when no prompt was needed.

    Returns:
        - None: no prompt (no DEPLOY_PHASE, or args already overrides)
        - "summary": user chose to view summary (caller should print and exit)
        - "force":   user chose to re-deploy from scratch (args.force set True)
        - "resume":  user chose to resume (args.from_phase set)
        - "finalize": last work phase was reached; user chose to finalize
        - "cancel":  user cancelled (caller should sys.exit)
    """
    if getattr(args, "force", False) or getattr(args, "from_phase", None):
        return None
    last = (env or {}).get("DEPLOY_PHASE", "")
    if not last:
        return None

    # --non-interactive auto-resolves the resume prompt without
    # asking. For an in-progress deploy that means "resume from next phase"
    # (matches the non-TTY default). For a complete deploy that means
    # "show summary and exit" — re-running a complete deploy unattended
    # without --force should be a no-op, not a silent re-deploy.
    if _NON_INTERACTIVE:
        if last == COMPLETE_MARKER:
            return "summary"
        nxt = _next_work_phase(last)
        if nxt is None:
            return "finalize"
        args.from_phase = nxt
        return "resume"

    if last == COMPLETE_MARKER:
        opt_summary = "Show deployment summary"
        opt_force = "Re-deploy from scratch (--force)"
        opt_cancel = "Cancel"
        choice = _select(
            "An existing complete deploy was detected. What would you like to do?",
            [opt_summary, opt_force, opt_cancel],
            default=opt_summary,
        )
        if choice == opt_summary:
            return "summary"
        if choice == opt_force:
            args.force = True
            return "force"
        return "cancel"

    if last in WORK_PHASES:
        nxt = _next_work_phase(last)
        if nxt is None:
            # Last work phase reached but never finalized
            opt_finalize = "Finalize: write DEPLOY_PHASE=complete + summary"
            opt_force = "Re-deploy from scratch (--force)"
            opt_cancel = "Cancel"
            choice = _select(
                f"Found in-progress deploy at last work phase: {last}",
                [opt_finalize, opt_force, opt_cancel],
                default=opt_finalize,
            )
            if choice == opt_finalize:
                # Set from_phase to a sentinel — actually we just want all
                # work phases skipped (they're done) and let run_deployment
                # write COMPLETE_MARKER. Easiest: leave args alone; the
                # phase guards will skip every WORK_PHASE because last is
                # already at the end, and run_deployment will write complete.
                return "finalize"
            if choice == opt_force:
                args.force = True
                return "force"
            return "cancel"

        opt_resume = f"Resume from --from-phase {nxt}"
        opt_restart = "Restart from beginning (--force)"
        opt_cancel = "Cancel"
        choice = _select(
            f"Found in-progress deploy at phase: {last}",
            [opt_resume, opt_restart, opt_cancel],
            default=opt_resume,
        )
        if choice == opt_resume:
            args.from_phase = nxt
            return "resume"
        if choice == opt_restart:
            args.force = True
            return "force"
        return "cancel"

    # Unknown DEPLOY_PHASE value — let the deploy proceed normally
    return None


EDIT_KEYS = {
    "confluent-keys": "Confluent API Keys",
    "cloud-creds": "AWS Bedrock Credentials",
    "atlas": "MongoDB Atlas Credentials",
    "atlas-admin": "Atlas Admin API Keys",
    "voyage": "Voyage AI API Key",
    "email": "Email (for tagging)",
}

# Keys that must be present in .env for quick-deploy
_REQUIRED_KEYS = [
    "TF_VAR_confluent_cloud_api_key",
    "TF_VAR_confluent_cloud_api_secret",
]

# AWS Bedrock credential keys
_AWS_KEYS = ["TF_VAR_aws_bedrock_access_key", "TF_VAR_aws_bedrock_secret_key"]

# MongoDB Atlas credential keys
_MONGODB_KEYS = [
    "TF_VAR_mongodb_connection_string",
    "TF_VAR_mongodb_username",
    "TF_VAR_mongodb_password",
]

# Voyage AI key
_VOYAGE_KEYS = ["TF_VAR_voyage_api_key"]

# Atlas Admin API keys
_ATLAS_ADMIN_KEYS = ["ATLAS_PUBLIC_KEY", "ATLAS_PRIVATE_KEY", "ATLAS_PROJECT_ID"]


# ── Project root ──────────────────────────────────────────────────────────────
def _project_root() -> Path:
    here = Path(__file__).resolve().parent
    for p in [here, *here.parents]:
        if (p / "pyproject.toml").exists():
            return p
    return here


# ── .env helpers ──────────────────────────────────────────────────
def _env_path() -> Path:
    return _project_root() / ".env"


def _load_env() -> dict:
    """Load all values from .env."""
    p = _env_path()
    if not p.exists():
        return {}
    from dotenv import dotenv_values

    return {k: v for k, v in dotenv_values(p).items() if v}


def _save_env(key: str, value: str) -> None:
    """Write a single key to .env ( thin wrapper over
    atomic _save_env_many)."""
    _save_env_many({key: value})


def _save_env_many(pairs: dict) -> None:
    """Atomic, structure-preserving write to credentials.env.

    delegates to scripts.common.env_file.atomic_write_env
    so deploy.py and mcp_deploy.py share a single canonical writer.
    No race between concurrent callers (the previous parallel-deploy +
    background MCP-build thread could clobber each other's writes when
    each had its own copy of the read-modify-write logic).
    """
    from scripts.common.env_file import atomic_write_env

    atomic_write_env(_env_path(), pairs)


def _is_ready(env: dict) -> bool:
    """Check if all required keys exist for quick-deploy."""
    for k in _REQUIRED_KEYS:
        if not env.get(k):
            return False
    # Must have AWS Bedrock credentials
    for k in _AWS_KEYS:
        if not env.get(k):
            return False
    # Must have MongoDB Atlas credentials
    for k in _MONGODB_KEYS:
        if not env.get(k):
            return False
    # Must have Voyage AI key
    for k in _VOYAGE_KEYS:
        if not env.get(k):
            return False
    # Must have Atlas Admin API keys
    for k in _ATLAS_ADMIN_KEYS:
        if not env.get(k):
            return False
    return True


# ── Terminal hyperlinks ───────────────────────────────────────────────────────
def _smart_link(url: str, text: str) -> str:
    if (
        sys.stdout.isatty()
        and not os.environ.get("NO_COLOR")
        and os.environ.get("TERM") != "dumb"
    ):
        return f"\033]8;;{url}\033\\{text}\033]8;;\033\\"
    return f"{text}: {url}"


# ── Credential display helpers ────────────────────────────────────────────────
def _trunc(value: str, n: int = 12) -> str:
    if not value:
        return "not set"
    return value[:n] + "..." if len(value) > n else value


def _mask(value: str) -> str:
    if not value:
        return "not set"
    if len(value) <= 4:
        return "****" + value
    return "********" + value[-4:]


def _get_cloud_cred_info(env: dict) -> tuple[str, str]:
    """Get the AWS Bedrock credential label and masked value."""
    return "AWS Bedrock", _mask(env.get("TF_VAR_aws_bedrock_access_key"))


# ── Display ───────────────────────────────────────────────────────────────────
def _banner() -> None:
    if HAS_RICH:
        _console.print(
            Panel.fit(
                f"  Streaming Agents Quickstart  ·  Deploy Script  ·  v{VERSION}  ",
                border_style="bright_blue",
            )
        )
    else:
        content = f"  Streaming Agents Quickstart  ·  Deploy Script  ·  v{VERSION}  "
        w = len(content)
        print("+" + "=" * w + "+")
        print(f"|{content}|")
        print("+" + "=" * w + "+")
    print()


def _section(title: str) -> None:
    width = 54
    inner = f" {title} "
    remaining = max(0, width - len(inner))
    left = remaining // 2
    right = remaining - left
    print(f"\n  {'=' * left}{inner}{'=' * right}")


def _show_summary(env: dict) -> None:
    ck = _mask(env.get("TF_VAR_confluent_cloud_api_key"))
    cl_label, cl_value = _get_cloud_cred_info(env)
    mcp_url = env.get("TF_VAR_mcp_server_url") or "(auto-deployed)"
    voyage = _mask(env.get("TF_VAR_voyage_api_key"))
    mongo_conn = _mask(env.get("TF_VAR_mongodb_connection_string"))
    mongo_user = env.get("TF_VAR_mongodb_username") or "not set"
    atlas_pub = _mask(env.get("ATLAS_PUBLIC_KEY"))
    atlas_proj = env.get("ATLAS_PROJECT_ID") or "not set"
    atlas_cluster = env.get("ATLAS_CLUSTER_NAME") or "not set"
    email = env.get("TF_VAR_owner_email") or "not set"

    print(f"  Confluent Key:      {ck}")
    print(f"  {cl_label}:        {cl_value}")
    print(f"  MCP Server:         {mcp_url}")
    print(f"  Atlas Connection:   {mongo_conn}")
    print(f"  Atlas Username:     {mongo_user}")
    print(f"  Atlas Admin Key:    {atlas_pub}")
    print(f"  Atlas Project ID:   {atlas_proj}")
    print(f"  Atlas Cluster:      {atlas_cluster}")
    print(f"  Voyage AI API Key:  {voyage}")
    print(f"  Email:              {email}")

    lr = env.get("DEPLOY_LAST_RUN", "")
    if lr:
        try:
            dt = datetime.fromisoformat(lr)
            h = str(int(dt.strftime("%I")))
            print(
                f"\n  Last run: {dt.strftime('%b %d, %Y at ')}{h}{dt.strftime(':%M %p')}"
            )
        except (ValueError, AttributeError):
            pass


# ── Phase 0: Pre-flight ───────────────────────────────────────────────────────
def _preflight(env: dict) -> bool:
    from scripts.common.login_checks import check_confluent_login

    print("  Checking prerequisites...")
    results, failures = {}, []

    for cmd, label, fix in [
        (
            ["confluent", "version"],
            "confluent CLI installed",
            "confluent CLI not found -- install from https://docs.confluent.io/confluent-cli/",
        ),
        (
            ["terraform", "version"],
            "terraform installed",
            "terraform not found -- install from https://developer.hashicorp.com/terraform/install",
        ),
    ]:
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            ok = r.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            ok = False
        results[label] = ok
        if not ok:
            failures.append(fix)

    logged_in = check_confluent_login()
    if not logged_in:
        org_id = env.get("CONFLUENT_CLOUD_ORGANIZATION_ID", "")
        # `confluent login` opens a browser SSO flow.
        # Without a TTY guard, CI / EC2 runs hang for the full 60 s timeout
        # waiting for input that will never come. Only attempt auto-login
        # when we actually have an interactive terminal.
        if org_id and sys.stdin.isatty():
            print("  [info] Not logged in — attempting auto-login with saved org ID...")
            login_cmd = ["confluent", "login", "--organization", org_id, "--save"]
            login_result = subprocess.run(login_cmd, timeout=60)
            logged_in = login_result.returncode == 0
        elif org_id:
            print("  [info] Not logged in (non-TTY) — skipping auto-login.")
            print(
                "         Run `confluent login --organization "
                f"{org_id} --save` interactively, then re-run deploy."
            )
        if not logged_in:
            failures.append("Not logged in -- run: confluent login")
    results["Logged into Confluent Cloud"] = logged_in

    for label, ok in results.items():
        print(f"  {'[ok]' if ok else '[FAIL]'} {label}")

    if failures:
        print()
        for msg in failures:
            print(f"  [FAIL]  {msg}")
        return False

    # ── Advisory credential checks (all soft — user can override) ─────────────
    access_key = env.get("TF_VAR_aws_bedrock_access_key")
    secret_key = env.get("TF_VAR_aws_bedrock_secret_key")
    if access_key and secret_key:
        if not _check_bedrock_creds(env, access_key, secret_key):
            return False

    # Advisory checks for MongoDB Atlas, Voyage AI, and Atlas Admin keys
    missing = []
    if not env.get("TF_VAR_voyage_api_key"):
        missing.append("Voyage AI API key (TF_VAR_voyage_api_key)")
    if not env.get("TF_VAR_mongodb_connection_string"):
        missing.append(
            "MongoDB Atlas connection string (TF_VAR_mongodb_connection_string)"
        )
    if not env.get("TF_VAR_mongodb_username"):
        missing.append("MongoDB Atlas username (TF_VAR_mongodb_username)")
    if not env.get("TF_VAR_mongodb_password"):
        missing.append("MongoDB Atlas password (TF_VAR_mongodb_password)")
    if not env.get("ATLAS_PUBLIC_KEY"):
        missing.append("Atlas Admin API public key (ATLAS_PUBLIC_KEY)")
    if not env.get("ATLAS_PRIVATE_KEY"):
        missing.append("Atlas Admin API private key (ATLAS_PRIVATE_KEY)")
    if not env.get("ATLAS_PROJECT_ID"):
        missing.append("Atlas Project ID (ATLAS_PROJECT_ID)")
    if missing:
        print("  [warn]  Credentials not yet configured (will be prompted):")
        for m in missing:
            print(f"     - {m}")

    # Soft check: Docker + AWS CLI needed for MCP auto-deploy
    if not env.get("TF_VAR_mcp_server_url"):
        mcp_missing = []
        try:
            subprocess.run(
                ["docker", "info"], capture_output=True, text=True, timeout=10
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            mcp_missing.append("Docker (needed for MCP server auto-deploy)")
        try:
            r = subprocess.run(
                ["aws", "--version"], capture_output=True, text=True, timeout=5
            )
            if r.returncode != 0:
                mcp_missing.append("AWS CLI (needed for MCP server auto-deploy)")
        except FileNotFoundError:
            mcp_missing.append("AWS CLI (needed for MCP server auto-deploy)")
        if mcp_missing:
            print("\n  [warn]  MCP server will be auto-deployed but requires:")
            for m in mcp_missing:
                print(f"     - {m}")
            print("     Alternatively, set TF_VAR_mcp_server_url to skip auto-deploy.")

    return True


def _check_bedrock_creds(env: dict, access_key: str, secret_key: str) -> bool:
    """
    Advisory check for AWS Bedrock credentials.
    Returns True to continue, False to abort (user cancelled).
    """
    import logging
    from scripts.common.test_bedrock_credentials import test_bedrock_credentials

    region = env.get("TF_VAR_cloud_region", "us-east-1")
    _logger = logging.getLogger("preflight.bedrock")
    _logger.setLevel(logging.CRITICAL)  # suppress library noise

    print()
    print("  Checking AWS Bedrock model access...")

    sonnet_ok, sonnet_err = test_bedrock_credentials(
        access_key, secret_key, region, logger=_logger, max_retries=1
    )

    sonnet_label = f"Claude Sonnet 4.6 accessible ({region})"
    print(f"  {'[ok]' if sonnet_ok else '[FAIL]'} {sonnet_label}")

    if sonnet_ok:
        return True

    # Collect warnings per failure type
    warnings = []
    model_enable_needed = False

    for ok, err, model_name in [
        (sonnet_ok, sonnet_err, "Claude Sonnet 4.6"),
    ]:
        if ok:
            continue
        if err == "invalid_keys":
            warnings.append(
                "The AWS credentials were not recognized. They may be expired or incorrect.\n"
                "Generate fresh credentials in the AWS IAM console, then re-run deploy --edit cloud-creds"
            )
            break
        elif err == "model_not_enabled":
            model_enable_needed = True
            warnings.append(
                f"{model_name} is not enabled in your AWS account for region {region}."
            )
        elif err == "no_boto3":
            warnings.append("boto3 is not installed -- skipping live Bedrock check.")
            return True  # not a blocker, skip advisory

    if model_enable_needed:
        warnings.append(
            "To enable Claude models, visit the AWS Bedrock Model Catalog:\n"
            "  https://console.aws.amazon.com/bedrock/home#/model-catalog\n"
            "Select Claude Sonnet 4.6 -> open in Playground -> send a message.\n"
            "The access request form will appear automatically."
        )

    print()
    print("  +-----------------------------------------------------+")
    print("  |  WARNING: AWS Bedrock model access issue             |")
    print("  +-----------------------------------------------------+")
    for w in warnings:
        for line in w.splitlines():
            print(f"  {line}")
    print()
    print("  If you deploy without resolving this, the demo may fail")
    print("  when Flink attempts to call the LLM models.")
    print()

    OPT_CONTINUE = "I understand -- deploy anyway"
    OPT_ABORT = "Abort -- I'll fix my credentials first"
    # --non-interactive opts into "continue anyway" — the
    # warnings above are already printed, and the operator has explicitly
    # asked for an unattended run. Aborting silently would be surprising.
    if _NON_INTERACTIVE:
        cli_output.warn(
            "Non-interactive deploy: continuing despite Bedrock advisory. "
            "Re-run interactively (without --non-interactive) to abort and "
            "fix credentials first."
        )
        return True
    choice = _select("How would you like to proceed?", [OPT_CONTINUE, OPT_ABORT])

    if choice == OPT_ABORT:
        print("\n  Deployment aborted.")
        return False
    return True


# ── Interactive prompt helpers ────────────────────────────────────────────────
def _select(question: str, choices: list, default: str = None) -> str:
    effective_default = default if default is not None else choices[0]
    # --non-interactive short-circuits every prompt, regardless
    # of whether stdout is a TTY. Returns the explicit default (or first
    # choice) silently. This is what makes scripted deploys deterministic.
    if _NON_INTERACTIVE:
        return effective_default
    if HAS_QUESTIONARY and sys.stdout.isatty():
        result = questionary.select(
            question, choices=choices, default=default, style=QSTYLE
        ).ask()
        if result is None:
            print("\n  Aborted.")
            sys.exit(0)
        return result
    # non-TTY callers (CI, piped input, EC2 with no
    # stdin attached) must not crash with EOFError on bare input().
    # Return the default silently.
    if not sys.stdin.isatty():
        return effective_default
    print(f"\n  {question}\n")
    for i, c in enumerate(choices, 1):
        marker = "> " if c == effective_default else "  "
        print(f"    {marker}{i}) {c}")
    print(f"\n  [default: {effective_default}]  ", end="")
    while True:
        try:
            raw = input("Choice: ").strip()
        except EOFError:
            # Stdin closed mid-prompt — fall back to default.
            return effective_default
        if not raw:
            return effective_default
        try:
            idx = int(raw) - 1
            if 0 <= idx < len(choices):
                return choices[idx]
        except ValueError:
            pass
        print(f"  Invalid. Enter 1-{len(choices)}.")


def _text(prompt: str, default: str = None, secret: bool = False) -> str:
    # --non-interactive must never block on input. When a
    # default exists, use it silently. When no default is available the
    # required credential is missing — this should have been caught by
    # the up-front validation in main(); raise a clear error rather than
    # hanging on stdin.
    if _NON_INTERACTIVE:
        if default is not None:
            return default
        raise RuntimeError(
            f"Non-interactive deploy: would prompt for {prompt!r} but no "
            f"default is available. Set the corresponding TF_VAR_* / "
            f"ATLAS_* value in .env or as an environment variable before "
            f"re-running with --non-interactive."
        )
    if default is not None:
        preview = _mask(default) if secret else _trunc(default)
        disp = f"  {prompt} [previously: {preview}]: "
        value = input(disp).strip()
        return value if value else default
    disp = f"  {prompt}: "
    while True:
        value = input(disp).strip()
        if value:
            return value
        print("  This field is required.")


# ── Phase 3: Confluent API Keys ──────────────────────────────────────────────
def prompt_confluent_keys(env: dict) -> None:
    _section("Confluent Cloud API Keys")
    print()

    # Org ID — needed for non-interactive login
    saved_org = env.get("CONFLUENT_CLOUD_ORGANIZATION_ID")
    if not saved_org:
        print("  Organization ID is required for non-interactive login.")
        print("  Find it at: https://confluent.cloud/settings/org/general")
        print()
        org_id = _text("Confluent Cloud Organization ID")
        if org_id:
            _save_env("CONFLUENT_CLOUD_ORGANIZATION_ID", org_id)
        print()

    print("  We strongly recommend letting us generate these")
    print("  automatically to avoid configuration errors.")
    print()

    saved_key = env.get("TF_VAR_confluent_cloud_api_key")
    saved_secret = env.get("TF_VAR_confluent_cloud_api_secret")

    OPT_REUSE = "Reuse my saved keys"
    OPT_AUTO = "Generate automatically"
    OPT_MANUAL = "Enter manually"

    if saved_key and saved_secret:
        choices = [OPT_REUSE, OPT_AUTO, OPT_MANUAL]
    else:
        choices = [OPT_AUTO, OPT_MANUAL]

    choice = _select("Confluent Cloud API Keys:", choices=choices)

    if choice == OPT_REUSE:
        return

    if choice == OPT_AUTO:
        from scripts.common.credentials import generate_confluent_api_keys

        print("\n  Generating Confluent Cloud API keys...")
        api_key, api_secret = generate_confluent_api_keys()
        if not api_key or not api_secret:
            print("  [FAIL] Failed to generate Confluent API keys. Aborting.")
            sys.exit(1)
        _save_env_many(
            {
                "TF_VAR_confluent_cloud_api_key": api_key,
                "TF_VAR_confluent_cloud_api_secret": api_secret,
            }
        )
        print(f"  [ok] Generated key: {_trunc(api_key)}")
        return

    # Manual entry
    key = _text("API Key", default=saved_key)
    secret = _text("API Secret", default=saved_secret, secret=True)
    _save_env_many(
        {
            "TF_VAR_confluent_cloud_api_key": key,
            "TF_VAR_confluent_cloud_api_secret": secret,
        }
    )


# ── Phase 4: AWS Bedrock Credentials ─────────────────────────────────────────
def prompt_cloud_creds(env: dict) -> None:
    label = "AWS Bedrock"
    _section(f"{label} Credentials")
    print()
    print("  AWS access keys (or temporary STS credentials) for an account")
    print("  that has Bedrock model access enabled. The deploy script cannot")
    print("  generate these for you — they must come from your AWS account.")
    print()
    print("  Get them from:")
    print(
        f"    {_smart_link('https://console.aws.amazon.com/iam/home#/security_credentials', 'AWS IAM → Security credentials')}"
    )
    print(
        f"    {_smart_link('https://console.aws.amazon.com/bedrock/home#/modelaccess', 'AWS Bedrock → Model access')}  (enable Claude models)"
    )
    print()

    has_saved = bool(
        env.get("TF_VAR_aws_bedrock_access_key")
        and env.get("TF_VAR_aws_bedrock_secret_key")
    )

    OPT_REUSE = "Reuse my saved keys"
    OPT_MANUAL = "Enter keys"

    if has_saved:
        choice = _select(f"{label} Credentials:", choices=[OPT_REUSE, OPT_MANUAL])
        if choice == OPT_REUSE:
            return

    # Capture email for resource tagging (formerly only happened in the auto branch)
    if not env.get("TF_VAR_owner_email"):
        email = _prompt_email(env)
        _save_env("TF_VAR_owner_email", email)

    key = _text("AWS Access Key ID", default=env.get("TF_VAR_aws_bedrock_access_key"))
    secret = _text(
        "AWS Secret Access Key",
        default=env.get("TF_VAR_aws_bedrock_secret_key"),
        secret=True,
    )
    pairs = {
        "TF_VAR_aws_bedrock_access_key": key,
        "TF_VAR_aws_bedrock_secret_key": secret,
    }
    # Temporary STS credentials (ASIA*) need an accompanying session token
    if key.startswith("ASIA"):
        print()
        print(
            "  ASIA* access keys are temporary credentials and require a session token."
        )
        token = _text(
            "AWS Session Token",
            default=env.get("TF_VAR_aws_session_token"),
            secret=True,
        )
        pairs["TF_VAR_aws_session_token"] = token
    else:
        # when switching from ASIA* to AKIA*, the old
        # session token is no longer valid. Terraform would forward the
        # stale value to AWS APIs producing opaque "ExpiredToken" failures.
        # Clear it explicitly.
        if env.get("TF_VAR_aws_session_token"):
            pairs["TF_VAR_aws_session_token"] = ""
    _save_env_many(pairs)


def _prompt_email(env: dict) -> str:
    print()
    print("  To tag your cloud resources correctly, we need")
    print("  your email address.")
    print()
    return _text("Email", default=env.get("TF_VAR_owner_email"))


# ── Phase 5: MCP Server ──────────────────────────────────────────────────────
# The MongoDB MCP Server is deployed automatically to AWS ECS Express Mode
# during run_deployment() if TF_VAR_mcp_server_url is not already set.
# If a URL is already configured and healthy, deployment is skipped.
# No user input required -- auth token is auto-generated.


# ── Phase 6: MongoDB Atlas Credentials ───────────────────────────────────────
def _gen_atlas_password() -> str:
    """Generate a strong, URL-safe password for the Atlas database user.

    Uses secrets.token_urlsafe and filters to alphanumerics so the resulting
    mongodb+srv URI does not need percent-encoding.
    """
    raw = secrets.token_urlsafe(24)
    cleaned = "".join(c for c in raw if c.isalnum())
    # token_urlsafe(24) yields ~32 chars; after filtering we still have plenty.
    return cleaned[:24] if len(cleaned) >= 24 else cleaned + secrets.token_hex(8)


def prompt_mongodb_atlas(env: dict) -> None:
    _section("MongoDB Atlas Credentials")
    print()
    print("  This project requires a MongoDB Atlas M10+ cluster with")
    print("  Atlas Stream Processing (ASP) and Voyage AI enabled.")
    print()

    saved_conn = env.get("TF_VAR_mongodb_connection_string")
    saved_user = env.get("TF_VAR_mongodb_username")
    saved_pass = env.get("TF_VAR_mongodb_password")

    if saved_conn and saved_user and saved_pass:
        OPT_REUSE = "Reuse my saved credentials"
        OPT_ENTER = "Enter different credentials (or create a new cluster)"
        choice = _select("MongoDB Atlas Credentials:", choices=[OPT_REUSE, OPT_ENTER])
        if choice == OPT_REUSE:
            # Reuse implies BYO — make sure deploy doesn't try to provision.
            _save_env("TF_VAR_create_atlas_cluster", "false")
            return

    # Top-level: BYO existing cluster vs Terraform-managed new M10 cluster
    OPT_BYO = "Provide an existing cluster's connection string (BYO)"
    OPT_CREATE = "Create a new M10 cluster with Terraform"
    # Honor a pre-set TF_VAR_create_atlas_cluster as the default branch. This
    # is what makes the documented non-interactive fresh-cluster path work:
    # .env sets create_atlas_cluster=true (with ATLAS_* keys) but no Mongo URI,
    # so the default must be CREATE — otherwise --non-interactive falls into
    # the BYO branch and _text() raises on the missing connection string.
    _preset_create = (env.get("TF_VAR_create_atlas_cluster") or "").lower() == "true"
    _default_choice = OPT_CREATE if _preset_create else OPT_BYO
    choice = _select(
        "How would you like to provide your Atlas cluster?",
        choices=[OPT_BYO, OPT_CREATE],
        default=_default_choice,
    )

    if choice == OPT_BYO:
        # Existing flow — user supplies connection string + db creds.
        conn = _text("MongoDB Atlas Connection String (e.g. mongodb+srv://...)")
        user = _text("MongoDB Atlas Username")
        passwd = _text("MongoDB Atlas Password", secret=True)
        _save_env_many(
            {
                "TF_VAR_mongodb_connection_string": conn,
                "TF_VAR_mongodb_username": user,
                "TF_VAR_mongodb_password": passwd,
                "TF_VAR_create_atlas_cluster": "false",
            }
        )
        return

    # Create branch — Terraform will provision M10 in the existing project.
    print()
    print("  A 3-node M10 replica set will be created in your existing")
    print("  Atlas project using PAK auth. Termination protection is")
    print("  disabled so 'uv run destroy' can clean up.")
    print()
    print("  You'll need:")
    print("    - Atlas Admin API public + private keys (PAK)")
    print("    - Existing Atlas project ID")
    print()
    pub = _text("Atlas Public Key", default=env.get("ATLAS_PUBLIC_KEY"))
    priv = _text("Atlas Private Key", default=env.get("ATLAS_PRIVATE_KEY"), secret=True)
    proj = _text("Atlas Project ID", default=env.get("ATLAS_PROJECT_ID"))
    cluster_name = _text(
        "Cluster name (will be created)",
        default=env.get("ATLAS_CLUSTER_NAME") or "streaming-agents-cluster",
    )
    db_user = "streaming_agents_app"
    db_pass = _gen_atlas_password()
    _save_env_many(
        {
            "ATLAS_PUBLIC_KEY": pub,
            "ATLAS_PRIVATE_KEY": priv,
            "ATLAS_PROJECT_ID": proj,
            "ATLAS_CLUSTER_NAME": cluster_name,
            "TF_VAR_create_atlas_cluster": "true",
            "TF_VAR_atlas_db_username": db_user,
            "TF_VAR_atlas_db_password": db_pass,
            # Clear any stale BYO connection string so the post-apply step writes
            # the freshly-provisioned cluster's value.
            "TF_VAR_mongodb_connection_string": "",
            "TF_VAR_mongodb_username": db_user,
            "TF_VAR_mongodb_password": db_pass,
        }
    )
    print()
    print(f"  [ok] Cluster '{cluster_name}' will be created.")
    print(f"  [ok] DB user '{db_user}' password generated and saved.")


# ── Phase 7: Atlas Admin API Keys ────────────────────────────────────────────
def prompt_atlas_admin_keys(env: dict) -> None:
    _section("Atlas Admin API Keys")
    print()
    print("  This project requires Atlas Admin API keys to provision")
    print("  Atlas Stream Processing (ASP) resources.")
    print()
    print("  Create a programmatic API key in your Atlas project:")
    print("  Atlas -> Project Settings -> Access Manager -> API Keys")
    print("  Required role: Project Owner")
    print()

    saved_pub = env.get("ATLAS_PUBLIC_KEY")
    saved_priv = env.get("ATLAS_PRIVATE_KEY")
    saved_proj = env.get("ATLAS_PROJECT_ID")
    saved_cluster = env.get("ATLAS_CLUSTER_NAME")

    if saved_pub and saved_priv and saved_proj:
        OPT_REUSE = "Reuse my saved keys"
        OPT_ENTER = "Enter new keys"
        choice = _select("Atlas Admin API Keys:", choices=[OPT_REUSE, OPT_ENTER])
        if choice == OPT_REUSE:
            return

    pub = _text("Atlas Public Key", default=saved_pub)
    priv = _text("Atlas Private Key", default=saved_priv, secret=True)
    proj = _text("Atlas Project ID", default=saved_proj)
    cluster = _text("Atlas Cluster Name", default=saved_cluster or "Cluster0")
    _save_env_many(
        {
            "ATLAS_PUBLIC_KEY": pub,
            "ATLAS_PRIVATE_KEY": priv,
            "ATLAS_PROJECT_ID": proj,
            "ATLAS_CLUSTER_NAME": cluster,
        }
    )


# ── Phase 8: Voyage AI API Key ───────────────────────────────────────────────
def prompt_voyage_api_key(env: dict) -> None:
    _section("Voyage AI Integration")
    print()
    print("  A Voyage AI API key is required for embedding")
    print("  generation via MongoDB's Atlas-hosted endpoint.")
    print()
    print("  Get your key from MongoDB Atlas project settings")
    print("  (Integrations -> Voyage AI).")
    print()

    saved = env.get("TF_VAR_voyage_api_key")

    if saved:
        OPT_REUSE = "Reuse my saved key"
        OPT_ENTER = "Enter new key"
        choice = _select("Voyage AI API Key:", choices=[OPT_REUSE, OPT_ENTER])
        if choice == OPT_REUSE:
            return

    key = _text("Voyage AI API Key", secret=True)
    _save_env("TF_VAR_voyage_api_key", key)


# ── Phase 9: Review & Confirm ────────────────────────────────────────────────
def show_review_and_confirm(env: dict) -> bool:
    _section("Deployment Summary")
    print()
    ck = _mask(env.get("TF_VAR_confluent_cloud_api_key"))
    cl_label, cl_value = _get_cloud_cred_info(env)
    mcp_url = env.get("TF_VAR_mcp_server_url") or "(auto-deployed)"
    voyage = _mask(env.get("TF_VAR_voyage_api_key"))
    mongo_conn = _mask(env.get("TF_VAR_mongodb_connection_string"))
    mongo_user = env.get("TF_VAR_mongodb_username") or "not set"
    atlas_pub = _mask(env.get("ATLAS_PUBLIC_KEY"))
    atlas_proj = env.get("ATLAS_PROJECT_ID") or "not set"
    atlas_cluster = env.get("ATLAS_CLUSTER_NAME") or "not set"

    print(f"  Confluent API Key:     {ck}")
    print(f"  {cl_label + ' Keys:':23}{cl_value}")
    print(f"  MCP Server:            {mcp_url}")
    print(f"  Atlas Connection:      {mongo_conn}")
    print(f"  Atlas Username:        {mongo_user}")
    print(f"  Atlas Admin Key:       {atlas_pub}")
    print(f"  Atlas Project ID:      {atlas_proj}")
    print(f"  Atlas Cluster:         {atlas_cluster}")
    print(f"  Voyage AI API Key:     {voyage}")
    print()
    # --non-interactive auto-confirms. _select would already
    # return the first choice silently, but make this explicit so the
    # surface is greppable and the log emits a clear marker.
    if _NON_INTERACTIVE:
        cli_output.info("Non-interactive deploy: proceeding without confirmation.")
        return True
    choice = _select("Proceed with deployment?", ["Yes, deploy", "No, cancel"])
    if choice == "Yes, deploy":
        return True
    print("\n  Deployment aborted.")
    return False


# ── Mode 1: Full Flow ─────────────────────────────────────────────────────────
def run_full_flow(env: dict) -> dict:
    if any(env.get(k) for k in _REQUIRED_KEYS):
        print()
        print("  " + "-" * 51)
        print("  Saved credentials found in .env.")
        print("  Your previous answers are shown as defaults.")
        print("  Press ENTER to accept any default, or type to change.")
        print("  " + "-" * 51)

    prompt_confluent_keys(env)
    env = _load_env()

    prompt_cloud_creds(env)
    env = _load_env()

    prompt_mongodb_atlas(env)
    env = _load_env()

    prompt_atlas_admin_keys(env)
    env = _load_env()

    prompt_voyage_api_key(env)
    env = _load_env()

    if not show_review_and_confirm(env):
        sys.exit(0)
    return env


# ── Mode 2: Quick-Deploy ──────────────────────────────────────────────────────
def run_quick_deploy(env: dict) -> dict:
    _section("Saved Configuration Found")
    print()
    _show_summary(env)
    print()

    OPT_DEPLOY = "Deploy with saved settings"
    OPT_EDIT = "Edit a setting"
    OPT_FULL = "Run full setup"
    OPT_QUIT = "Quit"

    choice = _select(
        "What would you like to do?",
        choices=[OPT_DEPLOY, OPT_EDIT, OPT_FULL, OPT_QUIT],
    )

    if choice == OPT_DEPLOY:
        if not show_review_and_confirm(env):
            sys.exit(0)
        return env
    elif choice == OPT_EDIT:
        run_edit_menu(env)
        return run_quick_deploy(_load_env())
    elif choice == OPT_FULL:
        return run_full_flow(env)
    else:
        print("\n  Goodbye.")
        sys.exit(0)


# ── Mode 3: Edit ──────────────────────────────────────────────────────────────
def _edit_key(key: str, env: dict) -> None:
    if key == "confluent-keys":
        prompt_confluent_keys(env)
    elif key == "cloud-creds":
        prompt_cloud_creds(env)
    elif key == "atlas":
        prompt_mongodb_atlas(env)
    elif key == "atlas-admin":
        prompt_atlas_admin_keys(env)
    elif key == "voyage":
        prompt_voyage_api_key(env)
    elif key == "email":
        email = _prompt_email(env)
        _save_env("TF_VAR_owner_email", email)


def run_edit_menu(env: dict) -> None:
    while True:
        env = _load_env()
        _section("Edit Configuration")
        print()

        ck = _mask(env.get("TF_VAR_confluent_cloud_api_key"))
        cl_label, cl_value = _get_cloud_cred_info(env)
        voyage = _mask(env.get("TF_VAR_voyage_api_key"))
        email = env.get("TF_VAR_owner_email") or "not set"

        atlas_conn = _mask(env.get("TF_VAR_mongodb_connection_string"))
        atlas_pub = _mask(env.get("ATLAS_PUBLIC_KEY"))

        FIELD_ITEMS = [
            ("confluent-keys", f"Confluent API Keys   {ck}"),
            ("cloud-creds", f"{cl_label} Keys       {cl_value}"),
            ("atlas", f"Atlas Credentials    {atlas_conn}"),
            ("atlas-admin", f"Atlas Admin API Keys {atlas_pub}"),
            ("voyage", f"Voyage AI API Key    {voyage}"),
            ("email", f"Email (for tagging)  {email}"),
            ("__back__", "<- Done editing"),
        ]

        if HAS_QUESTIONARY and sys.stdout.isatty():
            q_choices = [
                questionary.Choice(title=label, value=key) for key, label in FIELD_ITEMS
            ]
            key = questionary.select(
                "Select a field to change:",
                choices=q_choices,
                style=QSTYLE,
            ).ask()
            if key is None:
                break
        else:
            print("  Select a field to change:\n")
            for i, (k, label) in enumerate(FIELD_ITEMS[:-1], 1):
                print(f"    {i}) {label}")
            print("    Q) <- Done editing\n")
            raw = input("  Choice: ").strip().upper()
            if raw == "Q":
                break
            try:
                idx = int(raw) - 1
                if 0 <= idx < len(FIELD_ITEMS) - 1:
                    key = FIELD_ITEMS[idx][0]
                else:
                    print("  Invalid choice.")
                    continue
            except ValueError:
                print("  Invalid choice.")
                continue

        if key == "__back__":
            break

        _edit_key(key, env)
        print()
        print(f"  [ok] {EDIT_KEYS[key]} updated.")
        print()

        next_action = _select(
            "What next?",
            choices=["Edit another setting", "Deploy now", "Quit"],
        )
        if next_action == "Deploy now":
            env = _load_env()
            if not show_review_and_confirm(env):
                sys.exit(0)
            return
        elif next_action == "Quit":
            sys.exit(0)


# ── Post-terraform: ASP Setup ────────────────────────────────────────────────
def _run_asp_post_terraform(env: dict, root: Path) -> None:
    """Run ASP setup after successful Terraform deployment."""
    print("\n=== Post-Terraform: Atlas Stream Processing Setup ===")

    atlas_pub = env.get("ATLAS_PUBLIC_KEY")
    atlas_priv = env.get("ATLAS_PRIVATE_KEY")
    atlas_proj = env.get("ATLAS_PROJECT_ID")
    atlas_cluster = env.get("ATLAS_CLUSTER_NAME", "Cluster0")

    if not (atlas_pub and atlas_priv and atlas_proj):
        print("  [warn] Atlas Admin API keys not configured -- skipping ASP setup")
        print("    Run manually: uv run asp-setup")
        return

    # Read Confluent Kafka credentials: .env first, terraform output fallback
    confluent_bootstrap = env.get("CONFLUENT_BOOTSTRAP_SERVER", "")
    confluent_api_key = env.get("CONFLUENT_KAFKA_API_KEY", "")
    confluent_api_secret = env.get("CONFLUENT_KAFKA_API_SECRET", "")

    # Fall back to terraform core outputs for any missing Kafka credentials.
    if not all([confluent_bootstrap, confluent_api_key, confluent_api_secret]):
        from scripts.common.terraform_outputs import get_core_outputs

        outputs = get_core_outputs(root)
        if outputs:
            if not confluent_bootstrap:
                confluent_bootstrap = outputs.get(
                    "confluent_kafka_cluster_bootstrap_endpoint", {}
                ).get("value", "")
            if not confluent_api_key:
                confluent_api_key = outputs.get("app_manager_kafka_api_key", {}).get(
                    "value", ""
                )
            if not confluent_api_secret:
                confluent_api_secret = outputs.get(
                    "app_manager_kafka_api_secret", {}
                ).get("value", "")

    if not (confluent_bootstrap and confluent_api_key and confluent_api_secret):
        print("  [warn] Confluent Kafka credentials not available")
        print("    Run manually: uv run asp-setup")
        return

    voyage_api_key = env.get("TF_VAR_voyage_api_key", "")
    if not voyage_api_key:
        print("  [warn] Voyage AI API key not configured -- skipping ASP setup")
        print("    Run manually: uv run asp-setup")
        return
    from scripts.asp_setup import VOYAGE_API_ENDPOINT_DEFAULT

    voyage_api_endpoint = (
        env.get("TF_VAR_voyage_api_endpoint") or VOYAGE_API_ENDPOINT_DEFAULT
    )

    mongo_conn = env.get("TF_VAR_mongodb_connection_string", "")
    mongo_user = env.get("TF_VAR_mongodb_username", "")
    mongo_pass = env.get("TF_VAR_mongodb_password", "")

    # Schema Registry + Kafka REST credentials from terraform output
    schema_registry_url = ""
    schema_registry_key = ""
    schema_registry_secret = ""
    kafka_rest_endpoint = env.get("CONFLUENT_KAFKA_REST_ENDPOINT", "")
    kafka_cluster_id = env.get("CONFLUENT_KAFKA_CLUSTER_ID", "")
    from scripts.common.terraform_outputs import get_core_outputs

    tf_outputs = get_core_outputs(root)
    if tf_outputs:
        schema_registry_url = tf_outputs.get(
            "confluent_schema_registry_rest_endpoint", {}
        ).get("value", "")
        schema_registry_key = tf_outputs.get(
            "app_manager_schema_registry_api_key", {}
        ).get("value", "")
        schema_registry_secret = tf_outputs.get(
            "app_manager_schema_registry_api_secret", {}
        ).get("value", "")
        if not kafka_rest_endpoint:
            kafka_rest_endpoint = tf_outputs.get(
                "confluent_kafka_cluster_rest_endpoint", {}
            ).get("value", "")
        if not kafka_cluster_id:
            kafka_cluster_id = tf_outputs.get("confluent_kafka_cluster_id", {}).get(
                "value", ""
            )

    try:
        from scripts.asp_setup import run_asp_setup

        success = run_asp_setup(
            atlas_public_key=atlas_pub,
            atlas_private_key=atlas_priv,
            project_id=atlas_proj,
            cluster_name=atlas_cluster,
            confluent_bootstrap_server=confluent_bootstrap,
            confluent_api_key=confluent_api_key,
            confluent_api_secret=confluent_api_secret,
            voyage_api_key=voyage_api_key,
            voyage_api_endpoint=voyage_api_endpoint,
            schema_registry_url=schema_registry_url,
            schema_registry_key=schema_registry_key,
            schema_registry_secret=schema_registry_secret,
            mongodb_connection_string=mongo_conn,
            mongodb_username=mongo_user,
            mongodb_password=mongo_pass,
            kafka_rest_endpoint=kafka_rest_endpoint,
            kafka_cluster_id=kafka_cluster_id,
        )
        if not success:
            # ASP setup failure was previously
            # a warning that let the deploy continue with DEPLOY_PHASE
            # already at "asp_setup" — downstream phases (flink_dml)
            # would then fail with cascading errors, and the resume
            # system would think the deploy succeeded. Make this fatal.
            cli_output.error(
                "ASP setup failed. The deploy cannot continue safely — "
                "downstream Flink statements depend on Atlas connections."
            )
            # _save_env_many raises ValueError on \n / \r
            # in values. Guard so a future maintainer adding a
            # multi-line failure detail doesn't replace the recovery
            # hint with a stack trace.
            try:
                _save_env_many(
                    {
                        "DEPLOY_LAST_FAILED_PHASE": "asp_setup",
                        "DEPLOY_LAST_FAILURE": "ASP setup returned False",
                    }
                )
            except ValueError:
                pass  # best-effort breadcrumb
            cli_output.info(
                "Recover by addressing the failure above, then re-run "
                "`uv run deploy` (it will resume from asp_setup)."
            )
            sys.exit(1)
    except ImportError as e:
        print(f"  [warn] Could not import asp_setup: {e}")
        print("    Run manually: uv run asp-setup")
    except Exception as e:
        # an exception from run_asp_setup
        # is just as fatal as `success=False`. Previously this was a
        # `[warn]` that let the deploy march into flink_dml.
        import re as _re

        detail = _re.sub(r"[\r\n]+", " ", str(e))[:500]
        cli_output.error(f"ASP setup raised: {detail}. The deploy cannot continue.")
        try:
            _save_env_many(
                {
                    "DEPLOY_LAST_FAILED_PHASE": "asp_setup",
                    "DEPLOY_LAST_FAILURE": f"ASP setup raised: {detail}",
                }
            )
        except ValueError:
            pass
        cli_output.info(
            "Recover by addressing the failure above, then re-run "
            "`uv run deploy` (it will resume from asp_setup)."
        )
        sys.exit(1)


# ── Stale agents state quarantine ────────────────────────────────────────────
# Statement names defined in terraform/agents/main.tf. When terraform's
# state doesn't list one of these but the live env does, terraform's
# CREATE will 409 and the apply fails. _sweep_orphan_agents_statements
# deletes the server-side orphan so the next apply succeeds.
_AGENTS_TF_STATEMENT_NAMES = [
    "mongodb-connection-create",
    "mongodb-mcp-connection-create",
    "mongodb-mcp-model-create",
    "voyage-connection-create",
    "voyage-query-embedding-model-create",
    "documents-vectordb-create-table",
    "ride-requests-create-table",
    "anomalies-per-zone-create-table",
    "anomalies-sink-create-table",
    "zone-traffic-sink-create-table",
    "windowed-traffic-create-view",
]


def _sweep_orphan_agents_statements(root: Path) -> int:
    """Delete TF-managed Flink statements that exist server-side but are not
    in agents/terraform.tfstate. Such orphans cause `terraform apply` to
    fail with HTTP 409 "Statement with name X already exists" because
    terraform tries to CREATE without first refreshing the orphan.

    This typically happens when a previous apply partially succeeded, the
    user destroyed core but not agents, or a transient provider error left
    Confluent and terraform out of sync.

    Returns the number of orphans deleted.
    """
    import json as _json

    core_state = root / "terraform" / "core" / "terraform.tfstate"
    if not core_state.exists():
        return 0  # Core not deployed yet — nothing to sweep against

    from scripts.common.terraform_outputs import get_core_outputs

    outputs = get_core_outputs(root)
    if not outputs:
        return 0

    flink_key = outputs.get("app_manager_flink_api_key", {}).get("value", "")
    flink_secret = outputs.get("app_manager_flink_api_secret", {}).get("value", "")
    org_id = outputs.get("confluent_organization_id", {}).get("value", "")
    env_id = outputs.get("confluent_environment_id", {}).get("value", "")
    flink_endpoint = outputs.get("confluent_flink_rest_endpoint", {}).get("value", "")
    if not all([flink_key, flink_secret, org_id, env_id, flink_endpoint]):
        return 0

    # Compute set of statement names already tracked in agents state
    in_state: set = set()
    agents_state = root / "terraform" / "agents" / "terraform.tfstate"
    if agents_state.exists():
        try:
            text = agents_state.read_text()
            data = _json.loads(text)
            for res in data.get("resources", []):
                if res.get("type") != "confluent_flink_statement":
                    continue
                for inst in res.get("instances", []):
                    name = inst.get("attributes", {}).get("statement_name")
                    if name:
                        in_state.add(name)
        except Exception:
            pass

    # REST mechanics delegated to FlinkRestClient.
    from scripts.common.flink_rest import FlinkRestClient

    flink_client = FlinkRestClient(
        rest_endpoint=flink_endpoint,
        api_key=flink_key,
        api_secret=flink_secret,
        org_id=org_id,
        env_id=env_id,
        compute_pool_id=outputs.get("confluent_flink_compute_pool_id", {}).get(
            "value", ""
        ),
        service_account_id=outputs.get("app_manager_service_account_id", {}).get(
            "value", ""
        ),
        catalog="",
        database="",
    )

    deleted = 0
    for name in _AGENTS_TF_STATEMENT_NAMES:
        if name in in_state:
            continue
        # Check live state via client.get (returns None on 404)
        try:
            existing = flink_client.get(name)
        except Exception:
            continue
        if existing is None:
            continue  # Not on server, no orphan
        # Orphan confirmed — delete via client (which handles its own 404 case)
        try:
            flink_client.delete(name)
            print(
                f"  [info] Deleted orphan Flink statement '{name}' (existed server-side, not in state)"
            )
            deleted += 1
        except Exception as exc:
            print(f"  [warn] Could not delete orphan '{name}': {exc}")
    if deleted:
        # Confluent's DELETE is async — give it a beat before terraform retries CREATE
        time.sleep(15)
    return deleted


from scripts.common.atlas_reconcile import (
    quarantine_stale_agents_state as _quarantine_stale_agents_state,
)


def _pre_apply_atlas_cidr_state_mv(atlas_path: Path, creds: dict) -> None:
    """Pre-apply state-mv for the workshop → hardened CIDR upgrade.

    the moved block in terraform/atlas/main.tf can only
    target a constant key (`"0.0.0.0/0"`). When the user switches
    `atlas_access_cidrs` from the workshop default to a /32 egress IP,
    the resource at `.workshop["0.0.0.0/0"]` would otherwise be planned
    for destruction (no key match) and recreated at the new key — a
    delete+create that leaves the Atlas access list empty mid-apply.

    Detect that case here and `terraform state mv` the resource to the
    new key BEFORE apply runs. The subsequent apply then sees a no-op
    on the access list resource. Safe to skip when:
      * the legacy address isn't in state (fresh deploys, post-migration)
      * the target key is already populated (already migrated)
      * atlas_access_cidrs is unset or empty (defaults apply)
    """
    if not atlas_path.exists():
        return
    state_path = atlas_path / "terraform.tfstate"
    if not state_path.exists():
        return  # First deploy — let HCL moved block do the legacy migration

    # run terraform init idempotently before state list.
    # `_reconcile_orphan_atlas_db_user` only initializes when an orphan is
    # found in Atlas; on a fresh checkout with restored state but missing
    # `.terraform/`, `state list` returns non-zero and the function would
    # silently return — leaving the next apply to plan delete+create on
    # the access list.
    try:
        subprocess.run(
            ["terraform", "init", "-input=false", "-backend=false"],
            cwd=atlas_path,
            capture_output=True,
            text=True,
            timeout=120,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(
            f"  [warn] terraform init failed before state migration: "
            f"{(e.stderr or e.stdout or '').strip().splitlines()[-1] if (e.stderr or e.stdout) else 'unknown'}. "
            "Apply may plan delete+create on the access list."
        )
        return
    except Exception:
        return

    # Determine the intended new key.
    # `_refresh_creds` and `tfvars.py:255`
    # both use the unprefixed `workshop_mode` key. Reading
    # `TF_VAR_workshop_mode` here meant workshop participants would
    # accidentally trip the migration despite opting into workshop mode.
    workshop_mode = (creds.get("workshop_mode") or "").lower() == "true"
    cidrs_str = (creds.get("TF_VAR_atlas_access_cidrs") or "").strip()
    if workshop_mode:
        return  # Workshop mode keeps "0.0.0.0/0" — nothing to migrate.
    # Parse the tfvars-style list ["x", "y"] or fall back to JSON parse.
    # strip surrounding quotes on the plain
    # string path so a copy-pasted '"0.0.0.0/0"' is recognised.
    # handle multi-CIDR lists by iterating every parsed
    # entry, not just the first.
    target_cidrs: list[str] = []
    if cidrs_str:
        try:
            import json as _json

            if cidrs_str.startswith("["):
                parsed = _json.loads(cidrs_str)
                target_cidrs = [str(c).strip() for c in parsed if str(c).strip()]
            else:
                target_cidrs = [cidrs_str.strip().strip('"').strip("'")]
        except Exception:
            target_cidrs = []
    target_cidrs = [c for c in target_cidrs if c and c != "0.0.0.0/0"]
    if not target_cidrs:
        return  # No upgrade scenario.

    # Check whether the legacy keyed resource is in state.
    src_addr = 'mongodbatlas_project_ip_access_list.workshop["0.0.0.0/0"]'
    try:
        lst = subprocess.run(
            ["terraform", "state", "list"],
            cwd=atlas_path,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception:
        return
    if lst.returncode != 0:
        return
    state_entries = lst.stdout.splitlines()
    if src_addr not in state_entries:
        return  # Legacy address not present — nothing to move.

    # migrate to the FIRST target CIDR (terraform state mv only
    # supports one move per call). Additional CIDRs will be normal creates
    # — they didn't exist in state before, so no delete+create gap.
    target_cidr = target_cidrs[0]
    dst_addr = f'mongodbatlas_project_ip_access_list.workshop["{target_cidr}"]'
    if dst_addr in state_entries:
        return  # Already migrated.

    print(f"\n  [info] Migrating Atlas access list state: " f"{src_addr} -> {dst_addr}")
    mv = subprocess.run(
        ["terraform", "state", "mv", src_addr, dst_addr],
        cwd=atlas_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if mv.returncode == 0:
        print(
            "  [ok] State migration complete; apply will be a no-op for the access list."
        )
        if len(target_cidrs) > 1:
            print(
                f"  [info] Additional CIDRs in atlas_access_cidrs "
                f"({', '.join(target_cidrs[1:])}) will be CREATED by the "
                "next apply — no migration needed (didn't exist before)."
            )
    else:
        err = (mv.stderr or mv.stdout or "").strip().splitlines()
        last = err[-1] if err else "unknown"
        print(
            f"  [warn] terraform state mv failed ({last}); "
            "apply may plan delete+create on the access list."
        )


# ── Save Atlas-cluster connection string to .env ─────────────────
def _persist_atlas_cluster_connection_string(root: Path) -> bool:
    """When create_atlas_cluster=true, read atlas_cluster_connection_string
    from the standalone terraform/atlas state and persist it to .env
    so downstream tooling (agents tfvars, ASP setup, MCP service phase) can use
    it.

    Returns True if a connection string was written, False otherwise.
    """

    if (os.environ.get("TF_VAR_create_atlas_cluster") or "").lower() != "true":
        return False

    from scripts.common.terraform_outputs import get_atlas_outputs

    outputs = get_atlas_outputs(root)
    if not outputs:
        print(
            "  [warn] Could not read atlas_cluster_connection_string (no atlas state)"
        )
        return False

    conn_str = outputs.get("atlas_cluster_connection_string", {}).get("value", "")
    if not conn_str:
        print(
            "  [warn] atlas_cluster_connection_string output is empty (cluster may still be provisioning)"
        )
        return False

    db_user = os.environ.get("TF_VAR_atlas_db_username") or "streaming_agents_app"
    db_pass = os.environ.get("TF_VAR_atlas_db_password") or ""

    _save_env_many(
        {
            "TF_VAR_mongodb_connection_string": conn_str,
            "TF_VAR_mongodb_username": db_user,
            "TF_VAR_mongodb_password": db_pass,
        }
    )
    os.environ["TF_VAR_mongodb_connection_string"] = conn_str
    os.environ["TF_VAR_mongodb_username"] = db_user
    os.environ["TF_VAR_mongodb_password"] = db_pass
    print("  [ok] Atlas cluster connection string persisted to .env")
    return True


# ── Pre-apply: reconcile orphaned Atlas DB user ──────────────────────────────
# Re-exported under stable private names verified by
# (test_pass3_fixes asserts deploy._atlas_db_user_exists is
# atlas_reconcile.db_user_exists). Imported-but-unused-here is intentional.
from scripts.common.atlas_reconcile import (
    db_user_exists as _atlas_db_user_exists,  # noqa: F401
    delete_db_user as _delete_atlas_db_user,  # noqa: F401
)


def _reconcile_orphan_atlas_db_user() -> None:
    """Thin wrapper around atlas_reconcile.reconcile_orphan_db_user.

    pulls credentials from .env FIRST, then falls back to
    os.environ. deploy.py only mirrors TF_VAR_* keys into os.environ
    (not ATLAS_PUBLIC_KEY etc.), so reading os.environ alone caused
    the reconcile to silently early-return for users who only set
    Atlas keys in .env — the documented path. Result
    was orphan USER_ALREADY_EXISTS 409 on the next apply.

    """
    from scripts.common.atlas_reconcile import reconcile_orphan_db_user

    env = _load_env()
    reconcile_orphan_db_user(
        project_root=_project_root(),
        public_key=env.get("ATLAS_PUBLIC_KEY")
        or os.environ.get("ATLAS_PUBLIC_KEY", ""),
        private_key=env.get("ATLAS_PRIVATE_KEY")
        or os.environ.get("ATLAS_PRIVATE_KEY", ""),
        project_id=env.get("ATLAS_PROJECT_ID")
        or os.environ.get("ATLAS_PROJECT_ID", ""),
        username=(
            env.get("TF_VAR_atlas_db_username")
            or os.environ.get("TF_VAR_atlas_db_username")
            or "streaming_agents_app"
        ),
    )


# ── Save Kafka/SR credentials to .env ─────────────────────────────
def _save_terraform_credentials(root: Path) -> bool:
    """Read Kafka and Schema Registry credentials from terraform output and
    save them to .env so that CLI tools (asp-setup, datagen, etc.)
    can use them without reading terraform state directly.

    Returns True if all required credentials were saved, False otherwise.
    """
    from scripts.common.terraform_outputs import get_core_outputs

    outputs = get_core_outputs(root)
    if not outputs:
        print("  [FAIL] Could not read terraform outputs (no state or terraform error)")
        return False

    cred_map = {
        "CONFLUENT_BOOTSTRAP_SERVER": "confluent_kafka_cluster_bootstrap_endpoint",
        "CONFLUENT_KAFKA_API_KEY": "app_manager_kafka_api_key",
        "CONFLUENT_KAFKA_API_SECRET": "app_manager_kafka_api_secret",
        "CONFLUENT_SCHEMA_REGISTRY_URL": "confluent_schema_registry_rest_endpoint",
        "CONFLUENT_SCHEMA_REGISTRY_API_KEY": "app_manager_schema_registry_api_key",
        "CONFLUENT_SCHEMA_REGISTRY_API_SECRET": "app_manager_schema_registry_api_secret",
        "CONFLUENT_KAFKA_REST_ENDPOINT": "confluent_kafka_cluster_rest_endpoint",
        "CONFLUENT_KAFKA_CLUSTER_ID": "confluent_kafka_cluster_id",
        # preflight --phase flink_dml reads this from .env
        # but until now nothing wrote it. FlinkRestClient.from_env relies on it.
        "CONFLUENT_FLINK_REST_ENDPOINT": "confluent_flink_rest_endpoint",
    }

    pairs = {}
    missing = []
    for env_key, tf_key in cred_map.items():
        val = outputs.get(tf_key, {}).get("value", "")
        if val:
            pairs[env_key] = val
        else:
            missing.append(env_key)

    if pairs:
        try:
            _save_env_many(pairs)
        except ValueError as e:
            # _save_env_many raises on \n/\r in
            # values. Catch and return False so the
            # bool contract holds — caller will sys.exit(1)
            # with a clear message rather than dying on an uncaught
            # exception.
            print(f"  [FAIL] Could not persist terraform credentials: {e}")
            return False
        print(f"  [ok] Saved {len(pairs)} Kafka/SR credentials to .env")

    if missing:
        print(f"  [warn] Missing credentials: {', '.join(missing)}")
        print("         Downstream tools (asp-setup, datagen) may fail.")
        return False

    return True


# ── Create Flink DML statements via REST API ────────────────────────────────
def _create_flink_dml_statements(root: Path) -> bool:
    """Create DDL and streaming DML statements via the Flink REST API.

    The anomalies_enriched table is handled in two steps:
    1. ``anomalies-enriched-ctas`` — DDL: CREATE TABLE IF NOT EXISTS (COMPLETED is expected)
    2. ``anomalies-enriched-insert`` — DML: INSERT INTO ... SELECT (streaming, must reach RUNNING)

    The remaining 3 statements are INSERT INTO (streaming DML).

    After creating DML statements, the deploy waits for them to reach RUNNING
    state before publishing data.

    SQL templates live in ``terraform/agents/sql/*.sql`` and use ``{catalog}``
    / ``{database}`` placeholders that are filled from core terraform outputs.

    Returns True when the ESSENTIAL pipeline came up, False otherwise. The
    MCP-dependent ``dispatch-insert`` is intentionally best-effort (it has a
    documented dashboard recovery path), so its skip/failure does NOT fail the
    phase — but setup failures, DDL failure, and any of the other 4 core DML
    statements failing to reach/stay RUNNING DO, so the caller can refuse to
    mark the deploy complete on a broken pipeline.
    """
    import json
    import urllib.request
    import urllib.error

    # DDL statements: CREATE TABLE — expected phase is COMPLETED
    DDL_STATEMENTS = [
        "anomalies-enriched-ctas",
        "completed-actions-ctas",
    ]

    # DML statements: INSERT INTO — expected phase is RUNNING
    # Ordered: each statement may depend on artifacts created by the previous one
    DML_STATEMENTS = [
        "zone-traffic-sink-insert",
        "anomaly-detection-insert",
        "anomalies-enriched-insert",
        "anomalies-sink-insert",
        "dispatch-insert",
    ]

    print("\n=== Creating Flink Streaming Statements ===")

    def _ensure_flink_topics(
        rest_endpoint, cluster_id, kafka_api_key, kafka_api_secret
    ):
        """Recreate all Kafka topics required by Flink DML, plus delete
        their Schema Registry subjects.

        Topics must exist before Flink DML statements are submitted,
        otherwise statements may FAIL immediately with schema resolution
        errors. We DELETE + recreate (rather than ensure-exists) so that
        any stale Avro records from a previous deploy — possibly with a
        different schema ID — cannot poison the new statements with
        SourceInvalidValue (1200) deserialization errors. These pipeline
        topics carry only transient streaming data that the live source
        immediately repopulates, so deleting them is safe.

        Also deletes Schema Registry subjects (-value and -key) for each
        topic. Stale `-key` subjects can cause Flink to reconstruct tables
        with extra `key` columns from the old key schema.
        """
        # anomalies_enriched and completed_actions are CREATED BY
        # their CTAS DDL. Pre-creating their Kafka topics causes Confluent
        # to auto-register a phantom raw-byte catalog table that blocks
        # the CTAS DDL. Let CTAS create them. The other 5 topics still
        # need pre-creation because their tables ARE typed by terraform
        # DDL (CREATE TABLE … no AS SELECT).
        topics = [
            "ride_requests",
            "windowed_traffic",
            "anomalies_per_zone",
            "zone_traffic_sink",
            "anomalies_sink",
        ]
        # Streaming-output topics that are SAFE to delete + recreate on
        # every deploy. ride_requests is the pipeline INPUT — it has
        # been populated by publish_data already at this point in the
        # deploy flow, so deleting it would discard the baseline data.
        # The output topics are continuously repopulated by Flink as
        # soon as DML restarts, so wiping stale records is safe.
        purge_topics = {
            "windowed_traffic",
            "anomalies_per_zone",
            "zone_traffic_sink",
            "anomalies_sink",
        }
        cred = basic_auth_token(kafka_api_key, kafka_api_secret)
        topic_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {cred}",
        }

        # Step 1: Delete output topics (so any stale records / schema bindings die)
        for topic in topics:
            if topic not in purge_topics:
                continue
            del_url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics/{topic}"
            del_req = urllib.request.Request(
                del_url, method="DELETE", headers=topic_headers
            )
            try:
                urllib.request.urlopen(del_req, timeout=30)
                print(f"  [ok] Deleted stale topic '{topic}' (will recreate)")
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    pass  # didn't exist — nothing to delete
                else:
                    print(f"  [warn] Failed to delete topic '{topic}': HTTP {e.code}")
            except Exception as e:
                print(f"  [warn] Failed to delete topic '{topic}': {e}")

        # Step 2: Delete output topics' Schema Registry subjects (-value and -key)
        sr_url = outputs.get("confluent_schema_registry_rest_endpoint", {}).get(
            "value", ""
        )
        sr_key = outputs.get("app_manager_schema_registry_api_key", {}).get("value", "")
        sr_secret = outputs.get("app_manager_schema_registry_api_secret", {}).get(
            "value", ""
        )
        if sr_url and sr_key and sr_secret:
            sr_cred = basic_auth_token(sr_key, sr_secret)
            sr_headers = {"Authorization": f"Basic {sr_cred}"}
            for topic in topics:
                if topic not in purge_topics:
                    continue
                for suffix in ("-value", "-key"):
                    subject = f"{topic}{suffix}"
                    # Soft delete first, then permanent delete so re-creation
                    # can register a fresh schema with id 1.
                    for params in ("", "?permanent=true"):
                        sr_del = urllib.request.Request(
                            f"{sr_url}/subjects/{subject}{params}",
                            method="DELETE",
                            headers=sr_headers,
                        )
                        try:
                            urllib.request.urlopen(sr_del, timeout=15)
                        except urllib.error.HTTPError as e:
                            if e.code in (404, 40401):
                                break
                        except Exception:
                            break

        # Step 3: Wait for delete propagation
        time.sleep(5)

        # Step 4: Create fresh topics
        for topic in topics:
            create_url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics"
            body = json.dumps(
                {
                    "topic_name": topic,
                    "partitions_count": 6,
                }
            ).encode()
            create_req = urllib.request.Request(
                create_url, data=body, method="POST", headers=topic_headers
            )
            for attempt in range(3):
                try:
                    urllib.request.urlopen(create_req, timeout=30)
                    print(f"  [ok] Created topic '{topic}'")
                    time.sleep(2)
                    break
                except urllib.error.HTTPError as e:
                    resp_body = e.read().decode() if e.fp else ""
                    if "TopicExistsException" in resp_body or e.code == 409:
                        if attempt < 2:
                            time.sleep(5)
                            continue
                        print(
                            f"  [ok] Topic '{topic}' already exists (delete still propagating)"
                        )
                        break
                    print(f"  [warn] Failed to create topic '{topic}': HTTP {e.code}")
                    break
                except Exception as e:
                    print(f"  [warn] Failed to create topic '{topic}': {e}")
                    break

    # ── Read credentials from core terraform output ───
    from scripts.common.terraform_outputs import get_core_outputs

    outputs = get_core_outputs(root)
    if not outputs:
        print("  [warn] Could not read core terraform outputs.")
        print("         Create DML statements manually.")
        return False

    flink_key = outputs.get("app_manager_flink_api_key", {}).get("value", "")
    flink_secret = outputs.get("app_manager_flink_api_secret", {}).get("value", "")
    org_id = outputs.get("confluent_organization_id", {}).get("value", "")
    env_id = outputs.get("confluent_environment_id", {}).get("value", "")
    compute_pool_id = outputs.get("confluent_flink_compute_pool_id", {}).get(
        "value", ""
    )
    principal_id = outputs.get("app_manager_service_account_id", {}).get("value", "")
    flink_endpoint = outputs.get("confluent_flink_rest_endpoint", {}).get("value", "")
    catalog = outputs.get("confluent_environment_display_name", {}).get("value", "")
    database = outputs.get("confluent_kafka_cluster_display_name", {}).get("value", "")

    required = {
        "flink_key": flink_key,
        "flink_secret": flink_secret,
        "org_id": org_id,
        "env_id": env_id,
        "compute_pool_id": compute_pool_id,
        "principal_id": principal_id,
        "flink_endpoint": flink_endpoint,
        "catalog": catalog,
        "database": database,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"  [warn] Missing terraform outputs: {', '.join(missing)}")
        print("         Create DML statements manually.")
        return False

    cred_bytes = basic_auth_token(flink_key, flink_secret)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {cred_bytes}",
    }
    base_url = f"{flink_endpoint}/sql/v1/organizations/{org_id}/environments/{env_id}/statements"

    # Build a FlinkRestClient for the new code paths (drop_table, list,
    # delete_and_wait). The legacy inner _submit_statement / _wait_for_deletion
    # / _delete_and_wait helpers below ALSO delegate to this client where
    # appropriate; the helpers are retained as named entry points because
    # several tests grep for them by name ( + source contract).
    from scripts.common.flink_rest import FlinkRestClient

    flink_client = FlinkRestClient(
        rest_endpoint=flink_endpoint,
        api_key=flink_key,
        api_secret=flink_secret,
        org_id=org_id,
        env_id=env_id,
        compute_pool_id=compute_pool_id,
        service_account_id=principal_id,
        catalog=catalog,
        database=database,
    )

    # ── Pre-create Kafka topics before any DDL/DML ──────────────────────
    kafka_rest_endpoint = outputs.get("confluent_kafka_cluster_rest_endpoint", {}).get(
        "value", ""
    )
    kafka_cluster_id = outputs.get("confluent_kafka_cluster_id", {}).get("value", "")
    kafka_api_key = outputs.get("app_manager_kafka_api_key", {}).get("value", "")
    kafka_api_secret = outputs.get("app_manager_kafka_api_secret", {}).get("value", "")
    if kafka_rest_endpoint and kafka_cluster_id and kafka_api_key:
        print("\n  Pre-creating Kafka topics for Flink...")
        _ensure_flink_topics(
            kafka_rest_endpoint, kafka_cluster_id, kafka_api_key, kafka_api_secret
        )
        # after deleting + recreating Kafka topics, restart any
        # ASP processors that consume them. Without this their consumer
        # group offsets point at the old topic generation, leaving them
        # `STARTED` but silently not consuming.
        try:
            from scripts.common.asp_restart import restart_processors_for_topics
            from requests.auth import HTTPDigestAuth

            atlas_pub = os.environ.get("ATLAS_PUBLIC_KEY", "").strip()
            atlas_priv = os.environ.get("ATLAS_PRIVATE_KEY", "").strip()
            atlas_proj = os.environ.get("ATLAS_PROJECT_ID", "").strip()
            if atlas_pub and atlas_priv and atlas_proj:
                print("  Restarting ASP processors (post-topic-recreate)...")
                restart_processors_for_topics(
                    project_id=atlas_proj,
                    instance="asp-instance",
                    topics=["zone_traffic_sink", "anomalies_sink", "completed_actions"],
                    auth=HTTPDigestAuth(atlas_pub, atlas_priv),
                    timeout_per_processor=60,
                )
        except Exception as exc:
            # never abort deploy on ASP restart failure
            print(f"  [warn] ASP restart raised: {exc} (continuing)")

        # _ensure_flink_topics just deleted +
        # recreated the 4 streaming-output topics. Confluent auto-
        # registers recreated topics as raw-byte VARBINARY catalog
        # tables, CLOBBERING the terraform-typed tables/view. Restore
        # them: DROP the clobbered catalog entries, then
        # `terraform apply -replace` to reinstall the typed definitions.
        # Mirrors pipeline_reset.restart_flink_dml so the deploy and
        # datagen paths converge. Without this, DML reading from
        # windowed_traffic / anomalies_per_zone FAILS with
        # "Column 'pickup_zone'/'is_surge' not found".
        try:
            from scripts.pipeline_reset import (
                FLINK_CATALOG_TABLES as _PR_CATALOG_TABLES,
                _run_terraform_ddl_replace as _pr_terraform_ddl_replace,
            )

            print(
                "\n  Restoring terraform-typed catalog tables "
                "(post topic recreate)..."
            )
            for _tbl in _PR_CATALOG_TABLES:
                try:
                    flink_client.drop_table(_tbl, if_exists=True)
                except Exception as e:
                    print(f"  [info] catalog drop for {_tbl} returned: {e}")
                time.sleep(1)
            if _pr_terraform_ddl_replace(root):
                print("  [ok] Terraform-typed tables restored via apply -replace")
            else:
                print(
                    "  [warn] Could not restore typed tables via terraform "
                    "-replace (agents state missing?) — DML may fail with "
                    "column-not-found. Run 'uv run datagen' to recover."
                )
        except Exception as exc:
            print(f"  [warn] typed-table restore raised: {exc} (continuing)")
    else:
        print(
            "  [warn] Could not determine Kafka REST endpoint — skipping topic pre-creation"
        )

    sql_dir = root / "terraform" / "agents" / "sql"

    def _wait_for_deletion(stmt_name, max_wait=30):
        """Poll until a statement is fully deleted (404)."""
        check_url = f"{base_url}/{stmt_name}"
        for _ in range(max_wait // 3):
            time.sleep(3)
            try:
                req = urllib.request.Request(check_url, method="GET", headers=headers)
                urllib.request.urlopen(req, timeout=10)
                # Still exists — keep waiting
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return True  # deleted
            except Exception:
                pass  # network error — keep polling, only trust explicit 404
        return False

    def _delete_and_wait(stmt_name):
        """Delete a statement and wait until it's fully gone.
        Returns True if deletion confirmed, False if timed out."""
        check_url = f"{base_url}/{stmt_name}"
        try:
            del_req = urllib.request.Request(
                check_url, method="DELETE", headers=headers
            )
            urllib.request.urlopen(del_req, timeout=15)
        except Exception:
            pass
        if not _wait_for_deletion(stmt_name):
            print(f"  [warn] {stmt_name} still deleting after timeout")
            return False
        return True

    def _submit_statement(stmt_name, is_ddl=False):
        """Submit a single Flink SQL statement. Returns True if successful."""
        sql_file = sql_dir / f"{stmt_name}.sql"
        if not sql_file.exists():
            print(f"  [FAIL] SQL template not found: {sql_file}")
            return False

        sql = sql_file.read_text().strip().format(catalog=catalog, database=database)

        # Check if statement already exists
        check_url = f"{base_url}/{stmt_name}"
        try:
            check_req = urllib.request.Request(check_url, method="GET", headers=headers)
            with urllib.request.urlopen(check_req, timeout=15) as resp:
                existing = json.loads(resp.read())
                phase = existing.get("status", {}).get("phase", "UNKNOWN")
                if phase == "RUNNING":
                    print(f"  [ok] {stmt_name} already running")
                    return True
                elif phase == "COMPLETED" and is_ddl:
                    print(f"  [ok] {stmt_name} already completed (DDL)")
                    return True
                elif phase == "COMPLETED" and not is_ddl:
                    # DML statement showing COMPLETED means it stopped; delete and recreate
                    print(f"  [info] {stmt_name} completed unexpectedly, recreating...")
                    if not _delete_and_wait(stmt_name):
                        return False
                elif phase == "STOPPED":
                    # Try to resume it
                    try:
                        patch_body = json.dumps(
                            [{"op": "replace", "path": "/spec/stopped", "value": False}]
                        ).encode()
                        patch_req = urllib.request.Request(
                            check_url,
                            data=patch_body,
                            method="PATCH",
                            headers={**headers, "Content-Type": "application/json"},
                        )
                        with urllib.request.urlopen(
                            patch_req, timeout=15
                        ) as patch_resp:
                            data = json.loads(patch_resp.read())
                            new_phase = data.get("status", {}).get("phase", "unknown")
                            if new_phase != "STOPPED":
                                print(
                                    f"  [ok] {stmt_name} resumed (phase: {new_phase})"
                                )
                                return True
                    except Exception:
                        pass
                    # Resume failed — delete and recreate
                    print(f"  [info] {stmt_name} could not be resumed, recreating...")
                    if not _delete_and_wait(stmt_name):
                        return False
                else:
                    # Delete the failed/stopping/deleting statement and recreate
                    print(f"  [info] {stmt_name} in {phase} state, recreating...")
                    if not _delete_and_wait(stmt_name):
                        return False
        except urllib.error.HTTPError as e:
            if e.code != 404:
                body_text = e.read().decode()[:200] if e.fp else ""
                print(f"  [warn] Error checking {stmt_name}: HTTP {e.code} {body_text}")
        except Exception:
            pass  # 404 = doesn't exist yet, proceed to create

        # Build statement properties
        properties = {
            "sql.current-catalog": catalog,
            "sql.current-database": database,
        }

        # Create the statement with retry logic for transient errors
        payload = {
            "name": stmt_name,
            "spec": {
                "statement": sql,
                "properties": properties,
                "compute_pool_id": compute_pool_id,
                "principal": principal_id,
            },
        }
        body = json.dumps(payload).encode()

        max_attempts = 3
        retry_backoff = [3, 6, 12]
        for attempt in range(max_attempts):
            req = urllib.request.Request(
                base_url, data=body, method="POST", headers=headers
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read())
                    phase = data.get("status", {}).get("phase", "unknown")
                    print(f"  [ok] Created {stmt_name} (phase: {phase})")
                    return True
            except urllib.error.HTTPError as e:
                body_text = e.read().decode()[:300] if e.fp else ""
                if e.code in (429,) or e.code >= 500:
                    if attempt < max_attempts - 1:
                        wait = retry_backoff[attempt]
                        print(
                            f"  [retry] {stmt_name}: HTTP {e.code}, retrying in {wait}s (attempt {attempt + 1}/{max_attempts})"
                        )
                        time.sleep(wait)
                        continue
                print(f"  [FAIL] Could not create {stmt_name}: HTTP {e.code}")
                print(f"         {body_text}")
                return False
            except Exception as e:
                if attempt < max_attempts - 1:
                    wait = retry_backoff[attempt]
                    print(
                        f"  [retry] {stmt_name}: {e}, retrying in {wait}s (attempt {attempt + 1}/{max_attempts})"
                    )
                    time.sleep(wait)
                    continue
                print(f"  [FAIL] Could not create {stmt_name}: {e}")
                return False
        return False

    # Drop phantom catalog tables for CTAS-managed targets before
    # submitting DDL. If Confluent has already auto-registered raw-byte
    # tables (e.g. from a previous deploy where these topics were
    # pre-created, or via cross-statement side effects), CREATE TABLE IF
    # NOT EXISTS will NOOP against the phantom and downstream INSERTs fail
    # with "Different number of columns" / "Column 'pickup_zone' not
    # found". Mirrors pipeline_reset._drop_flink_catalog_tables.
    # SQL "DROP TABLE IF EXISTS" is built and submitted by
    # FlinkRestClient.drop_table — see scripts/common/flink_rest.py.
    ctas_targets = ["anomalies_enriched", "completed_actions"]
    # each CTAS target's table is paired with the
    # CTAS *statement* that creates it. Dropping the table alone is not
    # enough — _submit_statement sees the statement still COMPLETED and
    # skips recreation, leaving the table dropped-but-not-recreated, so
    # anomalies-sink-insert FAILS with "Table 'anomalies_enriched' does
    # not exist". Delete the statement too so the submit recreates both.
    _ctas_stmt_for = {
        "anomalies_enriched": "anomalies-enriched-ctas",
        "completed_actions": "completed-actions-ctas",
    }
    for table in ctas_targets:
        try:
            flink_client.drop_table(table, if_exists=True)
        except Exception as e:
            # Non-fatal: if there's no phantom, this is a no-op anyway
            print(f"  [info] phantom drop for {table} returned: {e}")
        # delete the CTAS statement so it is recreated (not
        # skipped as already-COMPLETED against a now-dropped table).
        _delete_and_wait(_ctas_stmt_for[table])
        time.sleep(2)

    # Step 1: Submit DDL statements (CREATE TABLE — expect COMPLETED)
    ddl_ok = True
    for stmt_name in DDL_STATEMENTS:
        if not _submit_statement(stmt_name, is_ddl=True):
            ddl_ok = False

    # Step 1b: Wait for DDL to reach COMPLETED before DML creation
    if ddl_ok and DDL_STATEMENTS:
        print("\n  Waiting for DDL statements to reach COMPLETED...")
        ddl_max_wait = 60
        ddl_poll = 5
        ddl_elapsed = 0
        ddl_pending = set(DDL_STATEMENTS)
        while ddl_pending and ddl_elapsed < ddl_max_wait:
            time.sleep(ddl_poll)
            ddl_elapsed += ddl_poll
            for stmt_name in list(ddl_pending):
                check_url = f"{base_url}/{stmt_name}"
                try:
                    check_req = urllib.request.Request(
                        check_url, method="GET", headers=headers
                    )
                    with urllib.request.urlopen(check_req, timeout=10) as resp:
                        data = json.loads(resp.read())
                        phase = data.get("status", {}).get("phase", "")
                        if phase == "COMPLETED":
                            ddl_pending.discard(stmt_name)
                        elif phase in ("FAILED",):
                            detail = data.get("status", {}).get("detail", "")
                            print(f"  [FAIL] DDL {stmt_name} failed: {detail[:200]}")
                            ddl_ok = False
                            ddl_pending.discard(stmt_name)
                except Exception:
                    pass
        if ddl_pending:
            print(f"  [warn] DDL timed out ({ddl_max_wait}s): {', '.join(ddl_pending)}")
            ddl_ok = False
        elif ddl_ok:
            print("  [ok] All DDL statements completed")

    if not ddl_ok:
        print("  [FAIL] DDL did not complete. Skipping DML creation.")
        print("         Fix DDL issues and re-run deploy, or create DML manually.")
        return False

    # Step 1c: Create the MCP tool + dispatch agent BEFORE the
    # DML batch. dispatch-insert references `boat_dispatch_agent`, which
    # was previously created by the dashboard's "Run Agent Dispatch" button.
    # On a fresh deploy that button hasn't been clicked yet, so dispatch-insert
    # FAILED with "Agent does not exist". The dashboard SQL is the single
    # source of truth — we import it here and submit verbatim with idempotent
    # IF NOT EXISTS semantics.
    #
    # bootstrap is wrapped in a polling helper rather than
    # fire-and-forget with fixed sleeps. Previously the block did
    # best-effort DELETE → sleep(2) → CREATE with no FAILED detection
    # and a final sleep(8) as the only sync before dispatch-insert. If
    # Flink's agent registry hadn't caught up, dispatch-insert FAILED
    # with "Agent does not exist" and only the 60s stability validation
    # caught it. The new helper waits for the DELETE to 404, submits
    # the CREATE, and polls phase to COMPLETED|FAILED (max 60s).
    def _bootstrap_agent_statement(
        stmt_name: str, sql: str, max_wait: int = 60
    ) -> bool:
        """Best-effort DELETE + CREATE with phase polling.

        Returns True when the statement reaches COMPLETED, False on
        FAILED, timeout, or transport error. dispatch-insert references
        the agent created here; downstream callers should NOT submit
        dispatch-insert when this returns False.
        """
        # 1. DELETE existing copy and wait for 404 (up to 30s).
        check_url = f"{base_url}/{stmt_name}"

        # single retry of transient 5xx before falling
        # into the 30s poll. Cheap (1 extra HTTP call); common case
        # (Flink momentarily slow) succeeds in ~5s instead of timeout+409.
        def _try_delete() -> tuple[bool, int | None]:
            """Returns (succeeded, http_code_or_None)."""
            try:
                del_req = urllib.request.Request(
                    check_url,
                    method="DELETE",
                    headers=headers,
                )
                urllib.request.urlopen(del_req, timeout=10)
                return True, None
            except urllib.error.HTTPError as e:
                return False, e.code
            except Exception:
                return False, None

        ok, code = _try_delete()
        if not ok:
            if code in (401, 403):
                # surface auth failures immediately.
                print(
                    f"  [FAIL] {stmt_name}: HTTP {code} (auth) on DELETE; not retrying"
                )
                return False
            if code is not None and code >= 500:
                # transient 5xx → one short-delay retry.
                time.sleep(3)
                ok, code = _try_delete()
                if not ok and code in (401, 403):
                    print(
                        f"  [FAIL] {stmt_name}: HTTP {code} (auth) on DELETE retry; not retrying further"
                    )
                    return False
            # Any other non-404 — fall through and try create anyway.
        # Poll for actual deletion.
        delete_deadline = time.time() + 30
        while time.time() < delete_deadline:
            try:
                probe = urllib.request.Request(check_url, method="GET", headers=headers)
                urllib.request.urlopen(probe, timeout=10)
                time.sleep(1)
                continue  # still exists
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    break
                # auth failure → break out of the poll
                # immediately so the user sees the auth error instead
                # of a 30s stall + cryptic "HTTP 401" at CREATE.
                if e.code in (401, 403):
                    print(
                        f"  [FAIL] {stmt_name}: HTTP {e.code} (auth) on GET; not retrying"
                    )
                    return False
            except Exception:
                pass
            time.sleep(1)
        # 2. CREATE
        payload = json.dumps(
            {
                "name": stmt_name,
                "spec": {
                    "statement": sql,
                    "properties": {
                        "sql.current-catalog": catalog,
                        "sql.current-database": database,
                    },
                    "compute_pool_id": compute_pool_id,
                    "principal": principal_id,
                },
            }
        ).encode()
        try:
            req = urllib.request.Request(
                base_url, data=payload, method="POST", headers=headers
            )
            urllib.request.urlopen(req, timeout=30)
        except urllib.error.HTTPError as e:
            body_text = e.read().decode()[:300] if e.fp else ""
            print(f"  [FAIL] {stmt_name}: HTTP {e.code} {body_text}")
            return False
        except Exception as e:
            print(f"  [FAIL] {stmt_name}: {e}")
            return False
        # 3. Poll phase. Agent + tool DDL completes (terminal); a stalled
        # PENDING/RUNNING phase past max_wait is a failure for our purposes.
        deadline = time.time() + max_wait
        while time.time() < deadline:
            try:
                probe = urllib.request.Request(check_url, method="GET", headers=headers)
                with urllib.request.urlopen(probe, timeout=10) as resp:
                    data = json.loads(resp.read())
                    phase = data.get("status", {}).get("phase", "")
                    if phase == "COMPLETED":
                        print(f"  [ok] {stmt_name} reached COMPLETED")
                        return True
                    if phase in ("FAILED", "DEGRADED"):
                        detail = data.get("status", {}).get("detail", "")
                        print(f"  [FAIL] {stmt_name} {phase}: {detail[:200]}")
                        return False
            except Exception:
                pass
            time.sleep(3)
        print(f"  [warn] {stmt_name} did not reach COMPLETED within {max_wait}s")
        return False

    agent_bootstrap_ok = True
    try:
        from scripts.dashboard import AGENT_SQL_CREATE_TOOL, AGENT_SQL_CREATE_AGENT

        agent_steps = [
            ("create-tool-mongodb-fleet", AGENT_SQL_CREATE_TOOL),
            ("create-agent-boat-dispatch", AGENT_SQL_CREATE_AGENT),
        ]
        for stmt_name, sql in agent_steps:
            if not _bootstrap_agent_statement(stmt_name, sql):
                agent_bootstrap_ok = False
                # Continue submitting the rest; failure surfaces below.
    except ImportError as e:
        print(f"  [warn] Could not import agent SQL from dashboard: {e}")
        print(
            "  [warn] dispatch-insert will likely FAIL. Run dashboard's 'Run Agent Dispatch' to recover."
        )
        agent_bootstrap_ok = False

    # Step 2: Submit DML statements (INSERT INTO — expect RUNNING)
    #
    # `dispatch-insert` is the only DML that initializes an MCP client at
    # submit time (the agent contacts mongodb-mcp-connection). On a fresh
    # deploy, the MCP service may still be cold-pulling on ECS. Submit
    # everything else first; gate dispatch-insert on a 200 from /mcp,
    # polling for up to ~3 minutes.
    MCP_DEPENDENT = "dispatch-insert"
    early_dml = [s for s in DML_STATEMENTS if s != MCP_DEPENDENT]
    late_dml = [s for s in DML_STATEMENTS if s == MCP_DEPENDENT]

    # _ensure_flink_topics deleted+recreated the
    # output topics' Avro `-value` schema subjects, and the
    # terraform-replace re-registered fresh schemas. Any DML statement
    # left "already running" would keep its STALE compiled output schema
    # and FAIL at write time with SerializationErrorValue (2200) "Cannot
    # write AVRO record" — exactly the failure that left zone_traffic
    # empty despite ride_requests carrying data. _submit_statement skips
    # RUNNING statements, so we must force-delete them here so the submit
    # loop recreates each one bound to the NEW schema. Mirrors
    # pipeline_reset, which deletes all DML before recreating.
    def _force_recreate_dml() -> None:
        for stmt_name in DML_STATEMENTS:
            _delete_and_wait(stmt_name)

    _force_recreate_dml()

    dml_created = []
    for stmt_name in early_dml:
        if _submit_statement(stmt_name, is_ddl=False):
            dml_created.append(stmt_name)

    if late_dml:
        mcp_url = os.environ.get("TF_VAR_mcp_server_url", "")
        mcp_token = os.environ.get("TF_VAR_mcp_auth_token", "")
        mcp_ready = False
        if mcp_url and mcp_token:
            print(f"\n  Probing MCP server before submitting {MCP_DEPENDENT}...")
            mcp_max_wait = 180  # 3 min
            mcp_poll = 15
            mcp_elapsed = 0
            while mcp_elapsed < mcp_max_wait:
                if _check_mcp_health(mcp_url, mcp_token):
                    mcp_ready = True
                    print(f"  [ok] MCP healthy after {mcp_elapsed}s")
                    break
                print(
                    f"  ... MCP still warming up ({mcp_elapsed}s elapsed, {mcp_max_wait - mcp_elapsed}s remaining)"
                )
                time.sleep(mcp_poll)
                mcp_elapsed += mcp_poll
        else:
            print(
                f"  [warn] No MCP URL/token in env — cannot health-check {MCP_DEPENDENT}"
            )

        # also gate on agent_bootstrap_ok. dispatch-insert
        # references boat_dispatch_agent — submitting it when the agent
        # CREATE failed guarantees a FAILED Flink statement.
        if mcp_ready and agent_bootstrap_ok:
            for stmt_name in late_dml:
                if _submit_statement(stmt_name, is_ddl=False):
                    dml_created.append(stmt_name)
        else:
            # Skip submitting dispatch-insert when MCP is unhealthy or
            # the agent bootstrap failed. Submitting either way would
            # guarantee a FAILED statement that blocks the pipeline.
            reason = (
                "MCP server is not healthy"
                if not mcp_ready
                else "agent / tool bootstrap did not reach COMPLETED"
            )
            print(f"  [SKIP] {MCP_DEPENDENT} not submitted — {reason}.")
            print("         The other 4 DML statements were created. To recover:")
            if not mcp_ready:
                print(
                    "         1. Check ECS task logs for the MCP service (CloudWatch)."
                )
                print(
                    "         2. Common causes: Atlas IP allowlist missing 0.0.0.0/0,"
                )
                print(
                    "            bad TF_VAR_mongodb_connection_string, image arch mismatch."
                )
            else:
                print("         1. Check Flink Console for create-tool-mongodb-fleet")
                print("            and create-agent-boat-dispatch failure details.")
                print(
                    "         2. Verify mongodb-mcp-connection in the catalog matches"
                )
                print("            the deployed MCP URL.")
            print("         3. Once fixed: re-run 'uv run deploy' (resumes from")
            print(
                "            DEPLOY_PHASE=flink_dml) or click 'Run Agent Dispatch' in"
            )
            print("            the dashboard.")

    # Step 3: Wait for DML statements to reach RUNNING state
    if dml_created:
        print("\n  Waiting for DML statements to reach RUNNING state...")
        max_wait = 120  # seconds (increased from 60 for larger pipelines)
        poll_interval = 5
        elapsed = 0
        pending = set(dml_created)
        failed = set()
        while pending and elapsed < max_wait:
            time.sleep(poll_interval)
            elapsed += poll_interval
            for stmt_name in list(pending):
                check_url = f"{base_url}/{stmt_name}"
                try:
                    check_req = urllib.request.Request(
                        check_url, method="GET", headers=headers
                    )
                    with urllib.request.urlopen(check_req, timeout=10) as resp:
                        data = json.loads(resp.read())
                        phase = data.get("status", {}).get("phase", "")
                        if phase == "RUNNING":
                            pending.discard(stmt_name)
                        elif phase in ("FAILED", "STOPPED", "COMPLETED"):
                            detail = data.get("status", {}).get("detail", "")
                            print(
                                f"  [FAIL] {stmt_name} reached {phase}: {detail[:300]}"
                            )
                            failed.add(stmt_name)
                            pending.discard(stmt_name)
                except Exception:
                    pass
            if pending:
                print(f"  ... {len(pending)} statement(s) still pending ({elapsed}s)")

        if not pending and not failed:
            print("  [ok] All DML statements are RUNNING")
        elif failed:
            print(
                f"  [FAIL] {len(failed)} statement(s) failed: {', '.join(sorted(failed))}"
            )
        if pending:
            print(f"  [warn] Timed out waiting for: {', '.join(pending)}")
            print("         Statements may start processing after data begins flowing.")

        # Stability validation: a Flink statement can transition
        # RUNNING -> FAILED/DEGRADED moments after starting (e.g. when
        # its consumer hits a stale offset with an old schema ID).
        # Watch for ~60s after the initial RUNNING check.
        if dml_created and not pending:
            print("\n  Validating DML stability for 60s...")
            stability_running = set(dml_created) - failed
            stability_failed: set[str] = set()
            stability_elapsed = 0
            stability_max = 60
            stability_poll = 10
            while stability_running and stability_elapsed < stability_max:
                time.sleep(stability_poll)
                stability_elapsed += stability_poll
                for stmt_name in list(stability_running):
                    check_url = f"{base_url}/{stmt_name}"
                    try:
                        check_req = urllib.request.Request(
                            check_url, method="GET", headers=headers
                        )
                        with urllib.request.urlopen(check_req, timeout=10) as resp:
                            data = json.loads(resp.read())
                            phase = data.get("status", {}).get("phase", "")
                            if phase in ("FAILED", "DEGRADED", "STOPPED"):
                                detail = data.get("status", {}).get("detail", "")
                                print(
                                    f"  [LATE-FAIL] {stmt_name} transitioned to {phase}: {detail[:300]}"
                                )
                                stability_failed.add(stmt_name)
                                stability_running.discard(stmt_name)
                    except Exception:
                        pass
            if stability_failed:
                print(
                    f"  [warn] {len(stability_failed)} statement(s) destabilized: {', '.join(sorted(stability_failed))}"
                )
                print(
                    "         Likely cause: stale Avro records on a topic from a prior deploy."
                )
                print(
                    '         Recover with: uv run python -c \'from pathlib import Path; from scripts.pipeline_reset import reset_pipeline, restart_flink_dml; reset_pipeline(Path(".")); restart_flink_dml(Path("."))\''
                )
            else:
                print("  [ok] All DML statements remained stable")

    print()
    print("  Flink statements created.")
    print("  Run 'uv run datagen' to publish ride data.")

    # The CTAS DDLs above (re)created the anomalies_enriched /
    # completed_actions catalog tables, which (re)creates their backing Kafka
    # topics. Any ASP processor consuming those topics has consumer offsets
    # pointing at the old topic generation — bounce them or they stay
    # STARTED but silently consume nothing.
    try:
        from scripts.common.asp_restart import restart_processors_for_topics
        from requests.auth import HTTPDigestAuth

        atlas_pub = os.environ.get("ATLAS_PUBLIC_KEY", "").strip()
        atlas_priv = os.environ.get("ATLAS_PRIVATE_KEY", "").strip()
        atlas_proj = os.environ.get("ATLAS_PROJECT_ID", "").strip()
        if atlas_pub and atlas_priv and atlas_proj:
            print("  Restarting ASP processors (post-CTAS-recreate)...")
            restart_processors_for_topics(
                project_id=atlas_proj,
                instance="asp-instance",
                topics=["anomalies_enriched", "completed_actions"],
                auth=HTTPDigestAuth(atlas_pub, atlas_priv),
                timeout_per_processor=60,
            )
    except Exception as exc:
        # never abort deploy on ASP restart failure
        print(f"  [warn] post-CTAS ASP restart raised: {exc} (continuing)")

    # Essential-success verdict: the core DML statements plus DDL must be
    # RUNNING and stable. Two statements are best-effort and must NOT fail the
    # phase:
    #   - dispatch-insert (MCP_DEPENDENT): documented dashboard recovery path.
    #   - anomalies-enriched-insert (RAG vector search): its per-anomaly
    #     VECTOR_SEARCH_AGG against the Atlas federated table reliably times
    #     out inside Flink under load, so it is OFF the critical path — the
    #     anomaly sink now reads detection output directly (see
    #     anomalies-sink-insert.sql). Enrichment is optional; its failure must
    #     not abort the deploy.
    # `failed`/`stability_failed`/`pending` only exist when the wait loop ran
    # (dml_created non-empty); default to empty.
    BEST_EFFORT = {MCP_DEPENDENT, "anomalies-enriched-insert"}
    essential = set(DML_STATEMENTS) - BEST_EFFORT
    all_failed = (
        (locals().get("failed") or set())
        | (locals().get("stability_failed") or set())
        | (locals().get("pending") or set())
    )
    essential_failed = essential & all_failed
    # Any essential statement that was never even created is also a failure.
    essential_created = essential & set(dml_created)
    essential_missing = essential - essential_created
    if essential_failed or essential_missing:
        broken = sorted(essential_failed | essential_missing)
        print(f"  [FAIL] Essential DML not healthy: {', '.join(broken)}")
        return False
    return True


# ── MCP Server integration ───────────────────────────────────────────────────

from scripts.common.flink_pipeline import (
    check_mcp_health as _check_mcp_health,
    CONNECTION_DRIFT_TRIGGERS as _CONNECTION_DRIFT_TRIGGERS,
    CONNECTION_TF_RESOURCES as _CONNECTION_TF_RESOURCES,
    detect_connection_drift as _detect_connection_drift,
)


def _drop_stale_mcp_catalog_objects(root: Path) -> bool:
    """Drop Flink catalog objects that reference the old MCP connection.

    Called when MCP URL changes between deploys. The cascade is:
    connection -> model -> tool -> agent -> INSERT statement.

    Returns True on full success, False when the cascade aborted partway
    (caller must NOT proceed with terraform apply -replace — would
    create duplicates next to the orphaned objects). H-NEW-1.
    """
    import json
    import urllib.request
    import urllib.error

    from scripts.common.terraform_outputs import get_core_outputs

    outputs = get_core_outputs(root)
    if not outputs:
        # Nothing to drop — core terraform outputs are unreadable, so there
        # is no stale catalog to cascade. Returning True (vs. a bare None)
        # so the caller does NOT mistake "nothing to do" for "cascade
        # aborted" and hard-abort the deploy (H-NEW-1 contract).
        print("  [info] No core terraform outputs found — nothing to drop")
        return True

    flink_key = outputs.get("app_manager_flink_api_key", {}).get("value", "")
    flink_secret = outputs.get("app_manager_flink_api_secret", {}).get("value", "")
    org_id = outputs.get("confluent_organization_id", {}).get("value", "")
    env_id = outputs.get("confluent_environment_id", {}).get("value", "")
    flink_endpoint = outputs.get("confluent_flink_rest_endpoint", {}).get("value", "")
    catalog = outputs.get("confluent_environment_display_name", {}).get("value", "")
    database = outputs.get("confluent_kafka_cluster_display_name", {}).get("value", "")

    if not all(
        [flink_key, flink_secret, org_id, env_id, flink_endpoint, catalog, database]
    ):
        # Incomplete Flink endpoint info — same rationale as above: there is
        # nothing we can (or need to) drop, so report success rather than
        # tripping the caller's "cascade aborted" abort path.
        print("  [info] Incomplete Flink connection info — nothing to drop")
        return True

    # REST mechanics delegated to FlinkRestClient. The DROP
    # statements themselves are still submitted via raw POST (with custom
    # statement names that differ per drop step) because the DROP cascade
    # order matters and the client's drop_table only handles tables/views,
    # not AGENT/TOOL/MODEL/CONNECTION.
    cred_bytes = basic_auth_token(flink_key, flink_secret)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {cred_bytes}",
    }
    base_url = f"{flink_endpoint}/sql/v1/organizations/{org_id}/environments/{env_id}/statements"

    from scripts.common.flink_rest import FlinkRestClient

    flink_client = FlinkRestClient(
        rest_endpoint=flink_endpoint,
        api_key=flink_key,
        api_secret=flink_secret,
        org_id=org_id,
        env_id=env_id,
        compute_pool_id=outputs.get("confluent_flink_compute_pool_id", {}).get(
            "value", ""
        ),
        service_account_id=outputs.get("app_manager_service_account_id", {}).get(
            "value", ""
        ),
        catalog=catalog,
        database=database,
    )

    drop_sqls = [
        ("mcp-drop-agent", "DROP AGENT IF EXISTS `boat_dispatch_agent`"),
        ("mcp-drop-tool", "DROP TOOL IF EXISTS `mongodb_fleet`"),
        ("mcp-drop-model", "DROP MODEL IF EXISTS `mongodb_mcp_model`"),
        ("mcp-drop-connection", "DROP CONNECTION IF EXISTS `mongodb-mcp-connection`"),
    ]

    # poll each DROP statement for COMPLETED before
    # proceeding to the next. Previous fixed `time.sleep(2)` between
    # async submits + global `time.sleep(10)` was insufficient under
    # Flink load — statement records got deleted before the SQL
    # actually executed, leaving the underlying CONNECTION alive and
    # then duplicated by terraform apply -replace.
    #
    # return tristate (COMPLETED / FAILED /
    # timeout) so the caller can distinguish a hard FAILED (cascade
    # corrupted, abort) from a slow Flink (warn + continue).
    def _wait_drop_completed(stmt_name: str, max_wait: int = 30) -> str:
        """Returns 'COMPLETED' / 'FAILED' / 'TIMEOUT'."""
        deadline = time.time() + max_wait
        while time.time() < deadline:
            try:
                probe = urllib.request.Request(
                    f"{base_url}/{stmt_name}",
                    method="GET",
                    headers=headers,
                )
                with urllib.request.urlopen(probe, timeout=10) as resp:
                    data = json.loads(resp.read())
                    phase = data.get("status", {}).get("phase", "")
                    if phase == "COMPLETED":
                        return "COMPLETED"
                    if phase == "FAILED":
                        return "FAILED"
            except Exception:
                pass
            time.sleep(2)
        return "TIMEOUT"

    cascade_aborted = False
    failed_step = None
    for stmt_name, sql in drop_sqls:
        if cascade_aborted:
            # previous drop FAILED hard; the cascade can't continue
            # safely (this drop's parent object is still referenced).
            print(f"    [SKIP] {stmt_name}: cascade aborted by prior FAILED drop")
            continue
        payload = json.dumps(
            {
                "name": stmt_name,
                "spec": {
                    "statement": sql,
                    "properties": {
                        "sql.current-catalog": catalog,
                        "sql.current-database": database,
                    },
                },
            }
        ).encode()
        req = urllib.request.Request(
            base_url, data=payload, method="POST", headers=headers
        )
        try:
            urllib.request.urlopen(req, timeout=30)
            print(f"    {sql}")
        except Exception:
            pass
        # Wait for THIS drop to complete before the next (cascade order
        # matters: agent → tool → model → connection).
        outcome = _wait_drop_completed(stmt_name)
        if outcome == "FAILED":
            print(
                f"    [FAIL] {stmt_name} FAILED. Aborting cascade — "
                "terraform apply -replace would otherwise leave duplicate "
                "catalog objects. Fix the FAILED drop in the Confluent "
                "Console (or wait for the reference to clear) then re-run."
            )
            cascade_aborted = True
            failed_step = stmt_name
        elif outcome == "TIMEOUT":
            print(f"    [warn] {stmt_name} did not reach COMPLETED within 30s")

    # when the cascade aborted, do NOT proceed with the
    # bulk DELETE of statement records. The caller MUST see the failure
    # so it can stop the deploy before terraform apply -replace creates
    # duplicates next to the orphaned catalog objects.
    if cascade_aborted:
        print(
            f"  [FAIL] MCP catalog cascade aborted at '{failed_step}'. "
            "Statement records preserved for triage; terraform apply will "
            "NOT proceed."
        )
        return False

    # Delete temp statements and terraform-managed ones via client.delete
    stmts_to_delete = [s[0] for s in drop_sqls] + [
        "mongodb-mcp-connection-create",
        "mongodb-mcp-model-create",
        "dashboard-create-tool",
        "dashboard-create-agent",
        "dashboard-create-completed-actions",
        "dashboard-create-completed-actions-table",
    ]
    for name in stmts_to_delete:
        try:
            flink_client.delete(name)
        except Exception:
            pass

    print("  [ok] Stale MCP catalog objects dropped")
    return True


def _deploy_mcp_if_needed(env: dict, region: str) -> dict:
    """Single-phase MCP deploy. Used by the BYO-cluster flow.

    For the create-cluster flow, deploy.py instead drives build_mcp_image and
    create_mcp_service explicitly so the build can overlap with cluster
    provisioning.
    """
    mcp_url = env.get("TF_VAR_mcp_server_url")
    mcp_token = env.get("TF_VAR_mcp_auth_token")

    if mcp_url and mcp_token:
        print("\n=== MCP Server: Checking existing deployment ===")
        if _check_mcp_health(mcp_url, mcp_token):
            print(f"  [ok] MCP server healthy at {mcp_url}")
            return env
        print(
            "  [warn] Existing MCP server not responding. Tearing down before redeploy..."
        )
        # Proactively delete the broken service so the rebuild gets a fresh
        # ECS Express service + ALB target groups. Without this, ECS Express
        # creates a name-suffixed service while the broken one stays around,
        # and listener weights may stay pinned to the dead TG.
        try:
            from scripts.mcp_deploy import destroy_mcp_server

            destroy_mcp_server(region)
            time.sleep(15)
        except Exception as exc:
            print(f"  [warn] Pre-redeploy cleanup raised: {exc} (continuing)")

    print("\n=== MCP Server: Deploying to ECS Express Mode ===")
    mongo_conn = env.get("TF_VAR_mongodb_connection_string", "")
    if not mongo_conn:
        print("  [FAIL] MongoDB connection string required for MCP server.")
        print("         Configure it first, then re-run deploy.")
        sys.exit(1)

    try:
        from scripts.mcp_deploy import deploy_mcp_server, check_prerequisites

        errors = check_prerequisites()
        if errors:
            print("  [FAIL] MCP server pre-flight failed:")
            for e in errors:
                print(f"    - {e}")
            sys.exit(1)

        url, token = deploy_mcp_server(
            region=region,
            auth_token=mcp_token,
            mongo_conn=mongo_conn,
        )

        _save_env_many(
            {
                "TF_VAR_mcp_server_url": url,
                "TF_VAR_mcp_auth_token": token,
            }
        )
        env["TF_VAR_mcp_server_url"] = url
        env["TF_VAR_mcp_auth_token"] = token
        os.environ["TF_VAR_mcp_server_url"] = url
        os.environ["TF_VAR_mcp_auth_token"] = token

    except ImportError as e:
        print(f"  [FAIL] Cannot import mcp_deploy: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"  [FAIL] MCP server deployment failed: {e}")
        sys.exit(1)

    return env


def _start_mcp_image_build_async(env: dict, region: str):
    """Kick off MCP image build (Phase A) in a background thread.

    Returns (thread, result_holder) or (None, None) when MCP is already healthy.

    The result_holder is a dict that the thread populates with either
    ``{"build": {...}, "auth_token": str}`` on success or
    ``{"error": str}`` on failure. Callers join the thread and inspect.
    """
    mcp_url = env.get("TF_VAR_mcp_server_url")
    mcp_token = env.get("TF_VAR_mcp_auth_token")

    if mcp_url and mcp_token:
        print("\n=== MCP Server: Checking existing deployment ===")
        if _check_mcp_health(mcp_url, mcp_token):
            print(f"  [ok] MCP server healthy at {mcp_url} — skipping build")
            return (None, None)
        print("  [warn] Existing MCP server unhealthy. Tearing down before redeploy...")
        try:
            from scripts.mcp_deploy import destroy_mcp_server

            destroy_mcp_server(region)
            time.sleep(15)
        except Exception as exc:
            print(f"  [warn] Pre-redeploy cleanup raised: {exc} (continuing)")

    print("\n=== MCP Server: Starting parallel image build (Phase A) ===")

    # Pre-flight on the main thread so a missing dep aborts before terraform.
    try:
        from scripts.mcp_deploy import check_prerequisites

        errors = check_prerequisites()
        if errors:
            print("  [FAIL] MCP server pre-flight failed:")
            for e in errors:
                print(f"    - {e}")
            sys.exit(1)
    except ImportError as e:
        print(f"  [FAIL] Cannot import mcp_deploy: {e}")
        sys.exit(1)

    auth_token = mcp_token or secrets.token_urlsafe(32)
    holder = {}

    def _worker():
        try:
            from scripts.mcp_deploy import build_mcp_image

            holder["build"] = build_mcp_image(region=region)
            holder["auth_token"] = auth_token
        except Exception as exc:
            holder["error"] = str(exc)

    t = threading.Thread(target=_worker, name="mcp-image-build", daemon=False)
    t.start()
    return (t, holder)


def _join_and_create_mcp_service(
    env: dict, region: str, root: Path, thread, holder, mongo_conn: str
) -> dict:
    """Wait for Phase A build to finish, then run Phase B (ECS service create).

    Updates env in place with TF_VAR_mcp_server_url / TF_VAR_mcp_auth_token.
    """
    if thread is None:
        return env  # MCP was already healthy — nothing to do

    print("\n=== MCP Server: Waiting for image build to finish ===")
    thread.join()
    if "error" in holder:
        print(f"  [FAIL] MCP image build failed: {holder['error']}")
        sys.exit(1)

    if not mongo_conn:
        print("  [FAIL] No MongoDB connection string for MCP service phase.")
        sys.exit(1)

    print("\n=== MCP Server: Creating service (Phase B) ===")
    try:
        from scripts.mcp_deploy import create_mcp_service

        url, token = create_mcp_service(
            image_uri=holder["build"]["image_uri"],
            exec_role=holder["build"]["exec_role"],
            infra_role=holder["build"]["infra_role"],
            region=region,
            auth_token=holder["auth_token"],
            mongo_conn=mongo_conn,
        )
        _save_env_many(
            {
                "TF_VAR_mcp_server_url": url,
                "TF_VAR_mcp_auth_token": token,
            }
        )
        env["TF_VAR_mcp_server_url"] = url
        env["TF_VAR_mcp_auth_token"] = token
        os.environ["TF_VAR_mcp_server_url"] = url
        os.environ["TF_VAR_mcp_auth_token"] = token
    except Exception as e:
        print(f"  [FAIL] MCP service create failed: {e}")
        sys.exit(1)

    return env


# ── Exit handlers (, 313) ───────────────────────────────────────────
def _install_exit_handlers(env: dict) -> None:
    """Install SIGINT/SIGTERM handlers that record the current DEPLOY_PHASE
    and print a resume hint before exiting with status 130.

    Best-effort: state writes are wrapped in try/except so a disk error
    inside the handler can never block the exit. The handler does NOT call
    os._exit; it uses sys.exit so atexit and finalizers still run.
    """

    def _on_signal(signum, _frame):
        # Best-effort state persistence
        phase = "<unknown>"
        try:
            phase = _load_env().get("DEPLOY_PHASE", "<unknown>")
        except Exception:
            pass
        try:
            _save_env_many({"DEPLOY_LAST_INTERRUPTED_PHASE": phase})
        except Exception:
            pass
        try:
            cli_output.warn(f"Interrupted (signal {signum}) during phase: {phase}")
            log_path = getattr(cli_output._S, "log_path", None)
            if log_path:
                cli_output.info(f"Session log: {log_path}")
            cli_output.info(
                "Resume with: uv run deploy --from-phase <phase>  "
                "(or --force to restart from the beginning)"
            )
        except Exception:
            pass
        sys.exit(130)

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)


# ── Deployment execution ──────────────────────────────────────────────────────
def run_deployment(env: dict, args=None) -> None:
    from scripts.common.terraform_runner import run_terraform
    from scripts.common.tfvars import write_tfvars_for_deployment

    # Phase guard precedence: --force > --from-phase > DEPLOY_PHASE.
    # When called without args (e.g. legacy / test entry), construct a fresh
    # SimpleNamespace so _should_run_phase does the right thing for every phase.
    if args is None:
        from types import SimpleNamespace

        args = SimpleNamespace(force=False, from_phase=None)

    # Interactive resume prompt — only fires when no override
    # flags were passed AND DEPLOY_PHASE is set. Mutates args (force /
    # from_phase) based on user choice, or short-circuits to summary/cancel.
    resume_choice = _resume_prompt(env, args)
    if resume_choice == "summary":
        print_deployment_summary(env, _project_root())
        return
    if resume_choice == "cancel":
        cli_output.info("Cancelled.")
        return

    # Phase-aware preflight — runs once at the start. Skipped
    # via --skip-preflight. Failures abort the deploy with exit code 1.
    if not getattr(args, "skip_preflight", False):
        from scripts.preflight import run_preflight

        passed, warned, failed = run_preflight(env=env)
        if failed > 0:
            cli_output.error(
                f"Preflight: {failed} fatal failure(s) — aborting. "
                "Use --skip-preflight to override."
            )
            sys.exit(1)
        if warned > 0:
            cli_output.warn(f"Preflight: {warned} warning(s) — continuing")

    def _run(phase: str) -> bool:
        return _should_run_phase(phase, _load_env(), args)

    def _skip(phase: str, reason: str = "already complete") -> None:
        cli_output.info(f"[skip] {phase} ({reason})")

    # Install signal handlers early so an interrupt during any phase below
    # records DEPLOY_LAST_INTERRUPTED_PHASE before exiting.
    _install_exit_handlers(env)

    root = _project_root()
    region = env.get("TF_VAR_cloud_region", "us-east-1")

    # Load all TF_VAR_* into os.environ for Terraform
    for k, v in env.items():
        if k.startswith("TF_VAR_") and v:
            os.environ[k] = v

    # Also set cloud_region if not already present
    if "TF_VAR_cloud_region" not in os.environ:
        os.environ["TF_VAR_cloud_region"] = region

    creating_cluster = (env.get("TF_VAR_create_atlas_cluster") or "").lower() == "true"

    # Build creds dict (strip TF_VAR_ prefix for write_tfvars_for_deployment)
    def _refresh_creds():
        base = {
            k: v for k, v in _load_env().items() if k.startswith("TF_VAR_") and v
        } | {
            k: v
            for k, v in _load_env().items()
            if k
            in (
                "ATLAS_PUBLIC_KEY",
                "ATLAS_PRIVATE_KEY",
                "ATLAS_PROJECT_ID",
                "ATLAS_CLUSTER_NAME",
            )
        }
        # propagate --workshop-mode into tfvars
        # generation so the Atlas IP access list / MCP secrets policy
        # picks the right default.
        base["workshop_mode"] = (
            "true" if getattr(args, "workshop_mode", False) else "false"
        )
        return base

    # Record last run
    _save_env("DEPLOY_LAST_RUN", datetime.now(timezone.utc).isoformat())

    # ── Branching deploy flow ────────────────────────────────────────────────
    # When the user opted to create an Atlas cluster via Terraform:
    #   1. Kick off MCP image build (Phase A) in a background thread.
    #   2. Apply the standalone atlas module (provisions the M10) — this is
    #      the long pole at ~7-15 min when Atlas is not queue-loaded.
    #   3. Persist the connection string from the atlas module's output.
    #   4. Join the MCP build thread, then run MCP service-create (Phase B)
    #      with the real connection string.
    #   5. Apply core + agents in sequence.
    #
    # The atlas module has its own state, so destroy/redeploy of core+agents
    # leaves the cluster intact (use `uv run destroy --include-cluster` to
    # tear it down).
    # ────────────────────────────────────────────────────────────────────────

    mcp_build_thread = None
    mcp_build_holder = None

    if creating_cluster:
        # Phase A — start MCP image build in background (no cluster needed).
        # This runs whether or not atlas_terraform itself is skipped, because
        # the MCP build is the parallel pipeline used by the mcp_server phase.
        mcp_build_thread, mcp_build_holder = _start_mcp_image_build_async(env, region)

    if creating_cluster and _run("atlas_terraform"):
        # Apply the standalone atlas module
        print("\n=== Phase: Provisioning Atlas Cluster (terraform/atlas) ===")
        # NOTE: DEPLOY_PHASE records the last COMPLETED phase (that is what
        # _should_run_phase / _next_work_phase assume). It is therefore written
        # at the END of each phase, not the start — a phase that starts and then
        # fails must be RE-RUN on resume, not skipped.
        creds = _refresh_creds()
        write_tfvars_for_deployment(root, region, creds, ["atlas"])
        # If a project-scoped DB user with the same name survived a prior
        # partial deploy, import it into terraform state so apply can refresh
        # its password — falls back to delete if import fails. Otherwise
        # terraform fails with USER_ALREADY_EXISTS (HTTP 409).
        _reconcile_orphan_atlas_db_user()
        atlas_path = root / "terraform" / "atlas"
        # handle workshop → hardened CIDR upgrade via
        # `terraform state mv` BEFORE apply. The HCL moved block can
        # only target a CONSTANT key ("0.0.0.0/0"), so when the user
        # switches `atlas_access_cidrs` from the workshop default to a
        # /32 egress IP, terraform would otherwise plan delete + create
        # — re-opening the mid-apply access-list gap.
        _pre_apply_atlas_cidr_state_mv(atlas_path, creds)
        if not run_terraform(atlas_path, replace_resources=[]):
            # On failure, ensure background MCP build doesn't leak.
            if mcp_build_thread is not None:
                print(
                    "  [warn] Atlas terraform failed; waiting for MCP build thread before exiting..."
                )
                mcp_build_thread.join()
            print("\n  [FAIL] Atlas terraform failed. Stopping.")
            sys.exit(1)

        # Persist connection string from atlas module output
        if not _persist_atlas_cluster_connection_string(root):
            print(
                "  [FAIL] Could not read connection string from atlas terraform output."
            )
            if mcp_build_thread is not None:
                mcp_build_thread.join()
            sys.exit(1)

        # Reload env so subsequent steps see the new connection string
        env = _load_env()
        for k, v in env.items():
            if k.startswith("TF_VAR_") and v:
                os.environ[k] = v

        # Phase succeeded — record it as the last completed phase.
        _save_env("DEPLOY_PHASE", "atlas_terraform")

    # ── Phase: MCP Server Deploy ─────────────────────────────────────────
    old_mcp_url = env.get("TF_VAR_mcp_server_url", "")
    new_mcp_url = old_mcp_url
    mcp_url_changed = False
    if _run("mcp_server"):
        if creating_cluster:
            # Phase B — finish MCP service create using the real connection string
            env = _join_and_create_mcp_service(
                env,
                region,
                root,
                mcp_build_thread,
                mcp_build_holder,
                mongo_conn=env.get("TF_VAR_mongodb_connection_string", ""),
            )
        else:
            # BYO flow — original serial deploy
            env = _deploy_mcp_if_needed(env, region)

        new_mcp_url = env.get("TF_VAR_mcp_server_url", "")
        mcp_url_changed = bool(
            old_mcp_url and new_mcp_url and old_mcp_url != new_mcp_url
        )

        if mcp_url_changed:
            print(
                "\n  [info] MCP URL changed — dropping stale Flink catalog objects..."
            )
            # abort the deploy on a cascade failure
            # rather than continuing into terraform apply (which would
            # create new objects next to the orphaned ones).
            if not _drop_stale_mcp_catalog_objects(root):
                cli_output.error(
                    "MCP catalog cascade aborted — refusing to apply "
                    "terraform on top of a partially-dropped catalog. "
                    "Fix the FAILED DROP in the Confluent Console (or "
                    "wait for the reference to clear), then re-run "
                    "`uv run deploy`."
                )
                try:
                    _save_env_many(
                        {
                            "DEPLOY_LAST_FAILED_PHASE": "mcp_server",
                            "DEPLOY_LAST_FAILURE": ("MCP catalog cascade aborted"),
                        }
                    )
                except ValueError:
                    pass
                sys.exit(1)

        # Phase succeeded.
        _save_env("DEPLOY_PHASE", "mcp_server")
    else:
        _skip("mcp_server")
        # If a background MCP image build was started but mcp_server is
        # being skipped (resume), join the thread so we don't leak it.
        if mcp_build_thread is not None:
            mcp_build_thread.join()

    creds = _refresh_creds()

    # Atlas module is now independent and is applied (above) only when creating
    # a new cluster. Core + agents are always applied.
    envs = ["core", "agents"]

    # Write terraform.tfvars files
    write_tfvars_for_deployment(root, region, creds, envs)

    if _run("terraform"):
        print("\n=== Starting Deployment ===")

        for e in envs:
            env_path = root / "terraform" / e
            if not env_path.exists():
                print(f"  Warning: {env_path} does not exist, skipping.")
                continue

            # After core succeeds, before agents runs, check for stale agents
            # state pinned to a previous (destroyed) Confluent environment, and
            # sweep any server-side orphans that would 409 the next CREATE.
            if e == "agents":
                _quarantine_stale_agents_state(root)
                _sweep_orphan_agents_statements(root)

            replace_resources = []
            if e == "agents" and mcp_url_changed:
                replace_resources = [
                    "confluent_flink_statement.mongodb_mcp_connection",
                    "confluent_flink_statement.mongodb_mcp_model",
                ]
            # detect drift on the other credentialed
            # connection inputs (Mongo URI/creds, Voyage key) and
            # `-replace` the corresponding terraform resources. The
            # CONNECTION resources have `ignore_changes = [statement]` so
            # terraform won't notice the rotation on its own.
            #
            # read current creds from os.environ (not the
            # `env` dict). The atlas-terraform phase reloads os.environ
            # at line 2845 but does NOT mutate the `env` dict, so the
            # resume-after-atlas path saw blank current values and never
            # triggered -replace. os.environ is the canonical source.
            if e == "agents":
                last_creds = {
                    k: os.environ.get(
                        f"DEPLOY_LAST_{k}", env.get(f"DEPLOY_LAST_{k}", "")
                    )
                    for k in _CONNECTION_DRIFT_TRIGGERS
                }
                current_creds = {
                    k: os.environ.get(k, env.get(k, ""))
                    for k in _CONNECTION_DRIFT_TRIGGERS
                }
                drifted = _detect_connection_drift(last_creds, current_creds)
                for sym_name in drifted:
                    tf_addr = _CONNECTION_TF_RESOURCES.get(sym_name)
                    if tf_addr and tf_addr not in replace_resources:
                        replace_resources.append(tf_addr)
                        print(
                            f"  [info] Credential drift detected for {sym_name} — will -replace {tf_addr}"
                        )
            # When agents apply hits a propagation-lag retry, sweep any
            # server-side Flink statements the failed apply left behind.
            # Without this, the next CREATE 409s on the FAILED statement.
            retry_hook = None
            if e == "agents":

                def retry_hook(_attempt: int) -> None:  # type: ignore[no-redef]
                    _sweep_orphan_agents_statements(root)

            if not run_terraform(
                env_path,
                replace_resources=replace_resources,
                pre_retry_hook=retry_hook,
            ):
                print(f"\n  [FAIL] Deployment failed at {e}. Stopping.")
                sys.exit(1)

            # persist the credential snapshot
            # immediately after the agents apply succeeds — that's when
            # the rotated creds were first consumed. Without this, a
            # deploy that succeeds at `agents` but fails at `flink_dml`
            # leaves a stale baseline, so the next deploy computes
            # drift against the wrong reference. The end-of-deploy call
            # below remains as defense in depth.
            if e == "agents":
                _persist_credential_snapshot()

        print("\n  [ok] All Terraform deployments completed successfully!")
        # invalidate the cached `terraform output -json`
        # results so the credentials phase below reads fresh values.
        from scripts.common.terraform_outputs import _clear_cache as _clear_tf_cache

        _clear_tf_cache()
        # Phase succeeded.
        _save_env("DEPLOY_PHASE", "terraform")
    else:
        _skip("terraform")

    # ── Save Kafka/SR credentials to .env ─────────────────────
    if _run("credentials"):
        # _save_terraform_credentials returns False when
        # any required cred is missing; downstream phases would silently
        # use blank values and produce confusing failures. Fail loudly.
        if not _save_terraform_credentials(root):
            cli_output.error(
                "Terraform credential persistence failed — required "
                "Kafka/SR endpoints or keys are empty. Fix terraform "
                "outputs and re-run deploy (it will resume from this "
                "phase)."
            )
            sys.exit(1)
        # Phase succeeded.
        _save_env("DEPLOY_PHASE", "credentials")
    else:
        _skip("credentials")

    # ── Publish baseline ride data ────────────────────────────────────────
    # Publish BEFORE ASP setup: the ~30s of Kafka producing serves as a natural
    # auth propagation buffer. By the time ASP processors try to connect to Kafka,
    # the API key will have propagated. Also registers schemas needed by Flink DML.
    if _run("publish_data"):
        if not _publish_local_data(root):
            try:
                _save_env_many(
                    {
                        "DEPLOY_LAST_FAILED_PHASE": "publish_data",
                        "DEPLOY_LAST_FAILURE": "initial data publish failed",
                    }
                )
            except ValueError:
                pass
            cli_output.error(
                "Initial data publish failed — ASP/Flink phases depend on the "
                "schemas + seed data it registers. Fix the publish error and "
                "re-run `uv run deploy` (it resumes from publish_data)."
            )
            sys.exit(1)
        # Phase succeeded.
        _save_env("DEPLOY_PHASE", "publish_data")
    else:
        _skip("publish_data")

    # ── Post-terraform: ASP Setup ────────────────────────────────────────────
    if _run("asp_setup"):
        # _run_asp_post_terraform sys.exit(1)s on failure, so reaching the
        # next line means the phase succeeded.
        _run_asp_post_terraform(env, root)
        _save_env("DEPLOY_PHASE", "asp_setup")
    else:
        _skip("asp_setup")

    # ── Create DML streaming statements via Flink REST API ─────────────
    if _run("flink_dml"):
        if not _create_flink_dml_statements(root):
            try:
                _save_env_many(
                    {
                        "DEPLOY_LAST_FAILED_PHASE": "flink_dml",
                        "DEPLOY_LAST_FAILURE": "essential Flink DML not healthy",
                    }
                )
            except ValueError:
                pass
            cli_output.error(
                "Essential Flink DML statements did not come up healthy. The "
                "pipeline is not streaming. Fix the failures above and re-run "
                "`uv run deploy` (it resumes from flink_dml)."
            )
            sys.exit(1)
        # Phase succeeded. Re-publish the ride data: creating the Flink DDL
        # above drops + recreates the ride_requests catalog table, which
        # deletes the backing Kafka topic — including everything the earlier
        # publish_data phase put on it. Without this re-publish a fresh
        # deploy ends with an EMPTY pipeline (no windows, no anomalies) and
        # Mission Control opens onto a dead screen. Best-effort: the
        # pipeline itself is healthy either way, so a publish hiccup warns
        # instead of failing the deploy.
        if not _publish_local_data(root):
            cli_output.warn(
                "Post-DML data re-publish failed — the pipeline is up but "
                "idle. Run `uv run datagen` to populate it."
            )
        _save_env("DEPLOY_PHASE", "flink_dml")
    else:
        _skip("flink_dml")

    # ── Launch Mission Control + finalize ─────────────────────────────
    _save_env("DEPLOY_PHASE", "complete")
    # Clear interrupted/failure markers on a clean completion.
    _clear_deploy_failure_state()
    # persist a snapshot of credentialed inputs so
    # the NEXT deploy can detect credential rotations and `-replace`
    # the affected Flink CONNECTION resources.
    _persist_credential_snapshot()
    # Mission Control (the live HUD) is the single UI: the live_server
    # process serves the static SPA, the bootstrap API and the SSE stream on
    # one port. The Streamlit dashboard is decommissioned from the deploy
    # flow (its module remains for manual/legacy use via `uv run dashboard`).
    _, live_port = _launch_live_server(root)
    if live_port:
        import webbrowser

        url = f"http://localhost:{live_port}"
        webbrowser.open(url)
        print(f"  [ok] Mission Control is open at {url}")
    print_deployment_summary(_load_env(), root)


def _persist_credential_snapshot() -> None:
    """Snapshot the credentialed TF_VAR values to DEPLOY_LAST_* keys.

     The next deploy's `_detect_connection_drift` reads
    these to detect rotation of mongo / voyage / mcp / bedrock creds and
    `-replace` the affected Flink CONNECTION resource.
    """
    env = _load_env()
    snapshot = {}
    for var in _CONNECTION_DRIFT_TRIGGERS:
        v = env.get(var, "")
        if v:
            snapshot[f"DEPLOY_LAST_{var}"] = v
    if snapshot:
        try:
            _save_env_many(snapshot)
        except Exception as e:
            # Non-fatal — credential drift detection is a defense-in-depth
            # layer; deploy still succeeded.
            print(f"  [warn] Could not persist credential snapshot: {e}")


def _clear_deploy_failure_state() -> None:
    """when DEPLOY_PHASE reaches 'complete', remove the
    DEPLOY_LAST_INTERRUPTED_PHASE / DEPLOY_LAST_FAILURE /
    DEPLOY_LAST_FAILED_PHASE breadcrumbs so a subsequent --summary run
    accurately reflects success state."""
    p = _env_path()
    if not p.exists():
        return
    try:
        from dotenv import unset_key
    except ImportError:
        return
    for key in (
        "DEPLOY_LAST_INTERRUPTED_PHASE",
        "DEPLOY_LAST_FAILURE",
        "DEPLOY_LAST_FAILED_PHASE",
    ):
        try:
            unset_key(str(p), key)
        except (KeyError, OSError):
            pass


# ── Deployment summary ─────────────────────────────────────
def print_deployment_summary(env: dict, root: Path) -> None:
    """Print a structured summary of the deployment.

    Renders Confluent / Atlas / MCP / Dashboard / Next-steps sections via
    `cli_output`. Component health is sourced from `scripts.health.collect_report`
    .
    """
    from scripts import health as _health

    cli_output.section("Deployment summary")

    # Confluent
    cli_output.subsection("Confluent")
    cli_output.kv(
        "Organization", env.get("TF_VAR_confluent_cloud_api_key", "?")[:8] + "…"
    )
    cli_output.kv("Bootstrap", env.get("CONFLUENT_BOOTSTRAP_SERVER", "?"))
    cli_output.kv("Cluster ID", env.get("CONFLUENT_KAFKA_CLUSTER_ID", "?"))

    # Pull current health snapshot under a hard timeout — collect_report
    # makes live API calls to Atlas/Confluent and can hang indefinitely if
    # those endpoints are unreachable. `uv run deploy --summary` should be
    # snappy, so cap it at 8s and degrade to an unknown report on miss.
    import threading as _thr

    _box: dict = {"report": None, "error": None}

    def _collect():
        try:
            _box["report"] = _health.collect_report()
        except BaseException as exc:  # noqa: BLE001 — best-effort
            _box["error"] = exc

    _t = _thr.Thread(target=_collect, daemon=True)
    _t.start()
    _t.join(timeout=8.0)
    if _t.is_alive():
        report = {
            "overall": "unknown",
            "flink": [],
            "asp": [],
            "kafka": [],
            "mongo": [],
            "_error": "health check timed out after 8s",
        }
    elif _box["error"] is not None:
        report = {
            "overall": "unknown",
            "flink": [],
            "asp": [],
            "kafka": [],
            "mongo": [],
            "_error": str(_box["error"]),
        }
    else:
        report = _box["report"] or {
            "overall": "unknown",
            "flink": [],
            "asp": [],
            "kafka": [],
            "mongo": [],
        }

    flink_running = sum(1 for e in report.get("flink", []) if e.get("status") == "ok")
    flink_failed = sum(1 for e in report.get("flink", []) if e.get("status") == "fail")
    flink_unknown = sum(
        1 for e in report.get("flink", []) if e.get("status") not in ("ok", "fail")
    )
    cli_output.kv(
        "Flink statements",
        f"{flink_running} ok, {flink_failed} failed, {flink_unknown} unknown",
    )

    # Atlas
    cli_output.subsection("Atlas")
    uri = env.get("TF_VAR_mongodb_connection_string", "")
    masked_uri = _mask_uri_host(uri) if uri else "?"
    cli_output.kv("Cluster", masked_uri)
    cli_output.kv("ASP instance", env.get("ATLAS_CLUSTER_NAME", "asp-instance"))
    asp_started = sum(1 for e in report.get("asp", []) if e.get("status") == "ok")
    asp_failed = sum(1 for e in report.get("asp", []) if e.get("status") == "fail")
    cli_output.kv("ASP processors", f"{asp_started} started, {asp_failed} failed")

    # MCP
    cli_output.subsection("MCP")
    cli_output.kv("URL", env.get("TF_VAR_mcp_server_url", "?"))

    # Mission Control — the single UI
    cli_output.subsection("Mission Control")
    cli_output.kv("URL", env.get("LIVE_SSE_URL", "http://localhost:8502"))

    # Next steps
    cli_output.subsection("Next steps")
    cli_output.info("uv run surge        # trigger an on-cue surge (demo money shot)")
    cli_output.info("uv run datagen      # start ShadowTraffic data generation")
    cli_output.info("uv run health       # full component health check")
    cli_output.info("uv run live         # re-launch Mission Control if closed")


def _mask_uri_host(uri: str) -> str:
    """Return a masked form of a mongodb+srv:// URI safe for display."""
    if not uri:
        return "?"
    # Hide credentials but keep host visible.
    try:
        scheme, _, rest = uri.partition("://")
        if "@" in rest:
            _, _, host = rest.partition("@")
        else:
            host = rest
        host = host.split("/", 1)[0]
        return f"{scheme}://***@{host}"
    except Exception:
        return "***"


# ── Publish local data ──────────────────────────────────────────────────────
def _publish_local_data(root: Path) -> bool:
    """Publish pre-generated ride data to bootstrap the streaming pipeline.

    Returns True on success. This is NOT optional: the publish registers the
    Avro schemas and seeds the topic that the later ASP + Flink DML phases
    depend on. A non-zero publish must therefore fail the phase (the caller
    aborts) rather than warn-and-continue into cascading downstream errors.
    A genuinely-absent data file is the one tolerated case.
    """
    data_file = root / "assets" / "data" / "ride_requests.jsonl"
    if not data_file.exists():
        print("\n  [warn] Local data file not found — skipping initial data publish.")
        print(f"         Expected: {data_file}")
        return True

    print("\n=== Publishing Initial Ride Data ===")
    result = subprocess.run(
        ["uv", "run", "publish_data", "--data-file", str(data_file), "--force"],
        cwd=root,
    )
    if result.returncode == 0:
        print("  [ok] Initial data published. Streaming pipeline is active.")
        return True
    print(
        "  [FAIL] Initial data publish failed (exit "
        f"{result.returncode}). Downstream ASP/Flink phases depend on the "
        "schemas and seed data this registers."
    )
    return False


# ── Launch dashboard ────────────────────────────────────────────────────────
def _is_port_in_use(port: int) -> bool:
    """Check if a TCP port is already in use on localhost."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return False
        except OSError:
            return True


DASHBOARD_DEFAULT_PORT = 8501
DASHBOARD_MAX_PORT = 8510


def _find_free_dashboard_port(
    start: int = DASHBOARD_DEFAULT_PORT, end: int = DASHBOARD_MAX_PORT
) -> int | None:
    """Return the first port in [start..end] not in use, or None if all are taken."""
    for p in range(start, end + 1):
        if not _is_port_in_use(p):
            return p
    return None


def _launch_dashboard(root: Path) -> None:
    """Launch the Streamlit dashboard in the background and open the browser.

    If the default port (8501) is held by another app, pick the next free
    port in 8501..8510. Don't assume an in-use port is "our" dashboard —
    that silently sends the user to whatever other app is bound there.

    Verifies the port is actually bound before reporting success, and tees
    Streamlit stderr to a log file so a crashed startup can be diagnosed.
    The child runs in its own process group (start_new_session=True) so it
    survives this script exiting and the terminal closing.
    """
    import webbrowser
    import socket

    dashboard_script = root / "scripts" / "dashboard.py"
    if not dashboard_script.exists():
        print("\n  [warn] Dashboard script not found — skipping.")
        return

    port = _find_free_dashboard_port()
    if port is None:
        print("\n=== Launching Dashboard ===")
        print(
            f"  [warn] All ports {DASHBOARD_DEFAULT_PORT}..{DASHBOARD_MAX_PORT} are in use."
        )
        print("         Free one and run: uv run dashboard")
        return

    print(f"\n=== Launching Dashboard (port {port}) ===")
    if port != DASHBOARD_DEFAULT_PORT:
        print(
            f"  [info] Port {DASHBOARD_DEFAULT_PORT} is taken — using {port} instead."
        )

    log_dir = root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"dashboard-{port}.log"

    try:
        log_fh = open(log_path, "w")
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(dashboard_script),
                "--server.port",
                str(port),
                "--server.headless",
                "true",
            ],
            cwd=root,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,  # detach from terminal so SIGHUP doesn't kill it
        )
    except Exception as e:
        print(f"  [warn] Could not launch dashboard: {e}")
        print("         Run manually: uv run dashboard")
        return

    # Poll up to 20s for the port to actually bind. Streamlit takes ~2-5s
    # cold; if it never binds, surface stderr from the log file.
    deadline = time.time() + 20
    bound = False
    while time.time() < deadline:
        if proc.poll() is not None:
            break  # process exited
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                bound = True
                break
        except OSError:
            time.sleep(0.5)

    if not bound:
        print(f"  [warn] Dashboard did not bind port {port} within 20s.")
        if proc.poll() is not None:
            print(f"         Streamlit exited with code {proc.returncode}.")
        try:
            with open(log_path) as f:
                tail = f.read()[-1000:]
            if tail.strip():
                print(f"         Last log output ({log_path}):")
                for line in tail.splitlines()[-10:]:
                    print(f"         | {line}")
        except OSError:
            pass
        print("         Run manually: uv run dashboard")
        return

    # Persist the actually-bound port so print_deployment_summary (which
    # reads DASHBOARD_PORT from a fresh _load_env()) reports the real URL
    # instead of always assuming the default 8501.
    _save_env_many({"DASHBOARD_PORT": str(port)})

    url = f"http://localhost:{port}"
    webbrowser.open(url)
    # Open Mission Control last so the webinar hero screen is the focused tab.
    live_url = os.environ.get("LIVE_SSE_URL")
    if live_url:
        webbrowser.open(live_url)
    print(f"  [ok] Dashboard running at {url}")
    if live_url:
        print(f"  [ok] Mission Control at {live_url}")
    print(f"        Logs: {log_path}")
    print()
    print("  Deployment complete! Mission Control is open in your browser.")
    print(f"  To stop the dashboard: kill {proc.pid}")


# ── Launch live SSE sidecar ───────────────────────────────────────────────────
LIVE_DEFAULT_PORT = 8502
LIVE_MAX_PORT = 8510


def _find_free_live_port(
    start: int = LIVE_DEFAULT_PORT, end: int = LIVE_MAX_PORT
) -> "int | None":
    """Return the first free port in [start..end] for the SSE sidecar, or None."""
    for p in range(start, end + 1):
        if not _is_port_in_use(p):
            return p
    return None


def _launch_live_server(root: Path):
    """Launch the live SSE sidecar (`uv run live`) in the background.

    Mirrors `_launch_dashboard`: picks a free port in 8502..8510, tees stderr
    to logs/, and detaches into its own process group so it survives this
    script exiting. Warns and returns (None, None) if it can't launch — the
    dashboard still works, the overlay just degrades to OFFLINE (spec
    REQ-E-040/041). Persists LIVE_SSE_URL so the dashboard component connects
    to the right port.

    Returns (proc, port) on success, (None, None) otherwise.
    """
    import socket

    live_script = root / "scripts" / "live_server.py"
    if not live_script.exists():
        print("\n  [warn] Live SSE sidecar script not found — skipping.")
        return None, None

    port = _find_free_live_port()
    if port is None:
        print("\n=== Launching Live SSE Sidecar ===")
        print(f"  [warn] All ports {LIVE_DEFAULT_PORT}..{LIVE_MAX_PORT} are in use.")
        print("         The dashboard live overlay will show OFFLINE.")
        print("         Free one and run: uv run live")
        return None, None

    print(f"\n=== Launching Live SSE Sidecar (port {port}) ===")
    log_dir = root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"live-{port}.log"

    try:
        log_fh = open(log_path, "w")
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "scripts.live_server",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=root,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception as e:
        print(f"  [warn] Could not launch live sidecar: {e}")
        print("         The dashboard live overlay will show OFFLINE.")
        return None, None

    # Poll up to 15s for the port to bind.
    deadline = time.time() + 15
    bound = False
    while time.time() < deadline:
        if proc.poll() is not None:
            break
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                bound = True
                break
        except OSError:
            time.sleep(0.4)

    if not bound:
        print(f"  [warn] Live sidecar did not bind port {port} within 15s.")
        print(f"         Logs: {log_path}")
        print("         The dashboard live overlay will show OFFLINE.")
        return None, None

    # Persist the URL so the dashboard's overlay connects to the right port.
    _save_env_many({"LIVE_SSE_URL": f"http://localhost:{port}"})
    os.environ["LIVE_SSE_URL"] = f"http://localhost:{port}"
    print(f"  [ok] Live SSE sidecar on http://localhost:{port}")
    print(f"        Logs: {log_path}")
    return proc, port


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming Agents deploy script")
    parser.add_argument(
        "--edit",
        nargs="?",
        const="__menu__",
        metavar="KEY",
        help="Edit a saved variable. Keys: " + ", ".join(EDIT_KEYS),
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Force full setup flow even if saved credentials exist",
    )
    parser.add_argument(
        "--plain",
        action="store_true",
        help="Plain text mode: disable rich/questionary UI (useful on EC2 or dumb terminals)",
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Disable logging CLI output to logs/deploy-<timestamp>.log",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress [info] output. Warnings, errors, success, steps still print.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Emit [dbg] output to stdout. Without --debug, debug only goes to log.",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print a deployment summary from the current .env state and exit. "
        "Read-only; does not perform any deploy work or launch the dashboard.",
    )
    parser.add_argument(
        "--from-phase",
        choices=WORK_PHASES,
        metavar="PHASE",
        default=None,
        help=(
            "Resume the deploy from the given phase, skipping all earlier ones. "
            "Valid phases (in order): "
            + ", ".join(WORK_PHASES)
            + ". Mutually exclusive with --force."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore DEPLOY_PHASE and execute every phase from the beginning. "
        "Mutually exclusive with --from-phase.",
    )
    parser.add_argument(
        "--list-phases",
        action="store_true",
        help="Print WORK_PHASES (one per line, with index) and exit.",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip run_preflight at the start of run_deployment.",
    )
    parser.add_argument(
        "--workshop-mode",
        action="store_true",
        help=(
            "Workshop convenience defaults: Atlas IP access list "
            "0.0.0.0/0 (no IP scoping). Without this flag the deploy "
            "scopes the Atlas allow-list to the deployer's egress IP "
            "(detected via checkip.amazonaws.com). "
        ),
    )
    parser.add_argument(
        "--non-interactive",
        "-y",
        action="store_true",
        dest="non_interactive",
        help=(
            "Run unattended: read all credentials from .env and/or "
            "environment variables, fail fast (exit 2) if any required "
            "credential is missing, and auto-confirm all interactive "
            "prompts (review, Bedrock advisory, resume). Implies --plain. "
            "Compatible with --from-phase, --force, --skip-preflight, "
            "and --workshop-mode."
        ),
    )
    args = parser.parse_args()

    # bootstrap session logging AFTER arg parse, gated
    # on --no-log. Previously bootstrap ran first and ignored args. The
    # early-exit flags below (--list-phases, --summary) are intentionally
    # before bootstrap so they don't leave an empty log file.
    if args.list_phases:
        for i, phase in enumerate(WORK_PHASES):
            print(f"  {i}  {phase}")
        print(f"  -  {COMPLETE_MARKER}  (terminal marker; not a --from-phase value)")
        sys.exit(0)

    # Mutex: --force and --from-phase
    if args.force and args.from_phase:
        parser.error("--force and --from-phase are mutually exclusive")

    # --non-interactive implies --plain (rich/questionary UI
    # would still try to render even when no human is present). Set the
    # module-level _NON_INTERACTIVE flag here so every interactive
    # surface called below sees a consistent value.
    if args.non_interactive:
        args.plain = True
        global _NON_INTERACTIVE
        _NON_INTERACTIVE = True

    if not args.no_log:
        from scripts.common.cli_logging import bootstrap_logging

        bootstrap_logging("deploy")

    if args.plain:
        global HAS_RICH, HAS_QUESTIONARY
        HAS_RICH = False
        HAS_QUESTIONARY = False

    # Initialize the typed CLI output system. The session
    # log (logs/deploy-<UTC>.log) coexists with cli_logging's pty session
    # capture — the two use different mechanisms and capture
    # different surfaces.
    cli_output.init(quiet=args.quiet, debug=args.debug)

    _banner()

    env = _load_env()

    # --non-interactive hydrates credentials from process
    # environment variables into .env (existing .env values win), then
    # validates that every required credential is set. Missing creds are
    # a hard fail with a list of the offending keys and an exit code of
    # 2 (distinguishable from preflight's exit 1).
    if args.non_interactive:
        n_hydrated = _hydrate_env_from_environment()
        if n_hydrated:
            cli_output.info(
                f"Non-interactive: hydrated {n_hydrated} credential(s) "
                f"from environment variables into .env."
            )
        env = _load_env()
        missing = _missing_required_credentials(env)
        if missing:
            cli_output.error(
                "Non-interactive deploy: required credentials are missing. "
                "Set each of the following in .env or as an environment "
                "variable, then re-run:"
            )
            for key in missing:
                cli_output.error(f"  - {key}")
            cli_output.info(
                "Tip: copy the example .env from docs/CONFIGURATION.md, "
                "or export the variables before invoking uv run deploy -y."
            )
            sys.exit(2)

    # --summary mode: print summary and exit.
    # Read-only: no deploy work, no dashboard launch.
    if args.summary:
        print_deployment_summary(env, _project_root())
        sys.exit(0)

    # Edit mode (skips pre-flight)
    if args.edit is not None:
        if not env:
            print("  No saved configuration found. Run the full setup first.")
            sys.exit(1)
        if args.edit == "__menu__":
            run_edit_menu(env)
        elif args.edit in EDIT_KEYS:
            _edit_key(args.edit, env)
            print(f"\n  [ok] {EDIT_KEYS[args.edit]} updated.")
        else:
            print(f"  Unknown key: {args.edit!r}")
            print(f"  Valid keys: {', '.join(EDIT_KEYS)}")
            sys.exit(1)
        sys.exit(0)

    # Pre-flight
    if not _preflight(env):
        print()
        sys.exit(1)

    # workshop-mode banner. Print before any prompts so
    # the user knows which defaults are about to apply.
    # info, not warn — the user explicitly
    # opted into workshop mode; it's a confirmation, not a warning.
    if args.workshop_mode:
        cli_output.info(
            "Workshop mode: Atlas IP allow-list will be 0.0.0.0/0 " "(no IP scoping)."
        )
    else:
        cli_output.info(
            "Hardened defaults: Atlas IP allow-list will be scoped to "
            "your egress IP (detected via checkip.amazonaws.com). "
            "Pass --workshop-mode to opt into the open default for "
            "workshop participants on heterogeneous networks."
        )

    # Mode selection
    if not args.full and _is_ready(env):
        env = run_quick_deploy(env)
    else:
        env = run_full_flow(env)

    # Wrap deployment in a top-level try/except so an uncaught exception
    # records DEPLOY_LAST_FAILURE / DEPLOY_LAST_FAILED_PHASE before re-raising,
    # giving the user a resume hint and the session log path.
    try:
        run_deployment(env, args)
    except SystemExit:
        raise
    except BaseException as exc:
        try:
            phase = _load_env().get("DEPLOY_PHASE", "<unknown>")
        except Exception:
            phase = "<unknown>"
        try:
            _save_env_many(
                {
                    "DEPLOY_LAST_FAILURE": f"{type(exc).__name__}: {exc}",
                    "DEPLOY_LAST_FAILED_PHASE": phase,
                }
            )
        except Exception:
            pass
        try:
            cli_output.error(f"Deploy failed in phase: {phase}")
            log_path = getattr(cli_output._S, "log_path", None)
            if log_path:
                cli_output.info(f"Session log: {log_path}")
            cli_output.info(
                "Resume with: uv run deploy --from-phase <phase>  "
                "(or --force to restart from the beginning)"
            )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
