#!/usr/bin/env python3
"""Deploy MongoDB MCP Server to AWS ECS Express Mode.

Usage:
    uv run mcp-deploy              # Deploy (auto-generates auth token)
    uv run mcp-deploy --destroy    # Tear down the ECS service

This script is called automatically by deploy.py but can also run standalone.
"""

import json
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path

# ── Constants ─────────────────────────────────────────────────────────────────
ECR_REPO_NAME = "mongodb-mcp-server"
ECS_SERVICE_NAME = "mongodb-mcp"
CONTAINER_PORT = 8080  # Proxy port (exposed to ALB)
MCP_SERVER_PORT = 8000  # Internal MCP server port (proxy forwards to this)
DEFAULT_REGION = "us-east-1"
HEALTH_CHECK_TIMEOUT = 720  # 12 min — ECS Express cold image pulls observed at 8-11 min
HEALTH_CHECK_INTERVAL = 15


def _run_aws(args: list, region: str, timeout: int = 60) -> subprocess.CompletedProcess:
    """Run an AWS CLI command with region and credentials from environment."""
    cmd = ["aws"] + args + ["--region", region, "--output", "json"]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _project_root() -> Path:
    """Walk up from this file to find pyproject.toml."""
    p = Path(__file__).resolve().parent
    while p != p.parent:
        if (p / "pyproject.toml").exists():
            return p
        p = p.parent
    return Path(__file__).resolve().parent.parent


# ── Pre-flight checks ─────────────────────────────────────────────────────────

def check_prerequisites() -> list:
    """Verify Docker and AWS CLI are available. Returns list of errors."""
    errors = []

    # Docker
    try:
        r = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=10)
        if r.returncode != 0:
            errors.append("Docker daemon is not running. Please start Docker Desktop.")
    except FileNotFoundError:
        errors.append("Docker is not installed. Install from https://docker.com")

    # AWS CLI
    try:
        r = subprocess.run(["aws", "--version"], capture_output=True, text=True, timeout=5)
        if r.returncode != 0:
            errors.append("AWS CLI is not working.")
    except FileNotFoundError:
        errors.append("AWS CLI is not installed. Install from https://aws.amazon.com/cli/")

    # ECS Express Mode support
    try:
        r = subprocess.run(
            ["aws", "ecs", "create-express-gateway-service", "help"],
            capture_output=True, text=True, timeout=10,
        )
        if "CREATE-EXPRESS-GATEWAY-SERVICE" not in r.stdout:
            errors.append(
                "AWS CLI does not support ECS Express Mode. "
                "Upgrade with: pip install --upgrade awscli"
            )
    except Exception:
        errors.append("Cannot verify ECS Express Mode support.")

    # AWS credentials
    try:
        r = subprocess.run(
            ["aws", "sts", "get-caller-identity", "--output", "json"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode != 0:
            errors.append("AWS credentials not configured. Run: aws configure")
    except Exception:
        errors.append("Cannot verify AWS credentials.")

    return errors


# ── ECR ───────────────────────────────────────────────────────────────────────

def _ensure_ecr_repo(region: str) -> str:
    """Create ECR repository if it doesn't exist. Returns repo URI."""
    r = _run_aws(["ecr", "describe-repositories", "--repository-names", ECR_REPO_NAME], region)
    if r.returncode == 0:
        data = json.loads(r.stdout)
        return data["repositories"][0]["repositoryUri"]

    # Create it
    r = _run_aws(["ecr", "create-repository", "--repository-name", ECR_REPO_NAME], region)
    if r.returncode != 0:
        raise RuntimeError(f"Failed to create ECR repo: {r.stderr}")
    data = json.loads(r.stdout)
    return data["repository"]["repositoryUri"]


def _docker_login_ecr(region: str, repo_uri: str) -> None:
    """Authenticate Docker to ECR."""
    password = subprocess.run(
        ["aws", "ecr", "get-login-password", "--region", region],
        capture_output=True, text=True, timeout=30,
    )
    if password.returncode != 0:
        raise RuntimeError(f"ECR login failed: {password.stderr}")

    registry = repo_uri.split("/")[0]
    login = subprocess.run(
        ["docker", "login", "--username", "AWS", "--password-stdin", registry],
        input=password.stdout, capture_output=True, text=True, timeout=30,
    )
    if login.returncode != 0:
        raise RuntimeError(f"Docker login failed: {login.stderr}")


# ── Docker Build & Push ───────────────────────────────────────────────────────

def _build_and_push(docker_context: Path, repo_uri: str, region: str) -> str:
    """Build Docker image for linux/amd64 and push to ECR. Returns image URI."""
    image_uri = f"{repo_uri}:latest"

    _docker_login_ecr(region, repo_uri)

    print("  Building Docker image (linux/amd64, no-cache)...")
    r = subprocess.run(
        ["docker", "buildx", "build", "--platform", "linux/amd64",
         "--no-cache", "--pull",
         "-t", image_uri, "--push", str(docker_context)],
        capture_output=True, text=True, timeout=600,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Docker build/push failed: {r.stderr[-500:]}")

    print(f"  Pushed: {image_uri}")
    return image_uri


# ── IAM Roles ─────────────────────────────────────────────────────────────────

def _ensure_execution_role(region: str) -> str:
    """Ensure ecsTaskExecutionRole exists. Returns ARN."""
    r = _run_aws(["iam", "get-role", "--role-name", "ecsTaskExecutionRole"], region)
    if r.returncode == 0:
        return json.loads(r.stdout)["Role"]["Arn"]

    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "ecs-tasks.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    })
    r = _run_aws([
        "iam", "create-role",
        "--role-name", "ecsTaskExecutionRole",
        "--assume-role-policy-document", trust_policy,
    ], region)
    if r.returncode != 0:
        raise RuntimeError(f"Failed to create execution role: {r.stderr}")

    _run_aws([
        "iam", "attach-role-policy",
        "--role-name", "ecsTaskExecutionRole",
        "--policy-arn", "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
    ], region)

    return json.loads(r.stdout)["Role"]["Arn"]


def _ensure_infrastructure_role(region: str) -> str:
    """Ensure ECS Express infrastructure role exists. Returns ARN."""
    role_name = "ecsInfrastructureRoleForExpressServices"

    # Check under service-role path first
    r = _run_aws(["iam", "get-role", "--role-name", role_name], region)
    if r.returncode == 0:
        return json.loads(r.stdout)["Role"]["Arn"]

    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "ecs.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    })
    r = _run_aws([
        "iam", "create-role",
        "--role-name", role_name,
        "--assume-role-policy-document", trust_policy,
    ], region)
    if r.returncode != 0:
        raise RuntimeError(f"Failed to create infra role: {r.stderr}")

    _run_aws([
        "iam", "attach-role-policy",
        "--role-name", role_name,
        "--policy-arn",
        "arn:aws:iam::aws:policy/service-role/AmazonECSInfrastructureRoleforExpressGatewayServices",
    ], region)

    return json.loads(r.stdout)["Role"]["Arn"]


# ── ECS Express Mode ──────────────────────────────────────────────────────────

def _get_existing_service(region: str) -> dict | None:
    """Check if the ECS Express service already exists. Returns service dict or None."""
    r = _run_aws([
        "ecs", "describe-express-gateway-service",
        "--service-name", ECS_SERVICE_NAME,
    ], region, timeout=30)
    if r.returncode == 0:
        data = json.loads(r.stdout)
        svc = data.get("service", {})
        status = svc.get("status", {}).get("statusCode", "")
        if status == "ACTIVE":
            return svc
    return None


def _extract_endpoint(service: dict) -> str | None:
    """Extract the HTTPS endpoint from an ECS Express service response."""
    for config in service.get("activeConfigurations", []):
        for path in config.get("ingressPaths", []):
            ep = path.get("endpoint", "")
            if ep:
                return f"https://{ep}" if not ep.startswith("https://") else ep
    return None


def _create_ecs_express(
    image_uri: str,
    exec_role_arn: str,
    infra_role_arn: str,
    region: str,
    auth_token: str,
    mongo_conn: str,
    service_name: str | None = None,
) -> tuple:
    """Create ECS Express Gateway service. Returns (endpoint_url, service_name)."""
    if service_name is None:
        service_name = ECS_SERVICE_NAME

    headers_json = json.dumps({"Authorization": f"Bearer {auth_token}"})

    # Only pass runtime-variable env vars. Static config (disabled tools, loggers)
    # is baked into the Dockerfile to avoid maintenance drift.
    container_config_obj = {
        "image": image_uri,
        "containerPort": CONTAINER_PORT,
        "environment": [
            {"name": "MDB_MCP_CONNECTION_STRING", "value": mongo_conn},
            {"name": "MDB_MCP_HTTP_HEADERS", "value": headers_json},
            {"name": "MDB_MCP_EXTERNALLY_MANAGED_SESSIONS", "value": "true"},
            {"name": "MDB_MCP_HTTP_RESPONSE_TYPE", "value": "json"},
            {"name": "MDB_MCP_TRANSPORT", "value": "http"},
            {"name": "MDB_MCP_HTTP_HOST", "value": "0.0.0.0"},
            {"name": "MDB_MCP_HTTP_PORT", "value": str(MCP_SERVER_PORT)},
            {"name": "MDB_MCP_LOGGERS", "value": "stderr,mcp"},
        ],
    }

    # write the container config
    # (containing the bearer token and MongoDB connection string with
    # embedded password) to a 0o600 temp file inside a user-private
    # ~/.cache/streaming-agents directory rather than /tmp. While the
    # AWS CLI is running, /proc/<pid>/fd/* may be readable by other
    # local users; placing the tempfile outside /tmp narrows that
    # exposure to processes running under the same UID.
    import tempfile
    cache_dir = Path.home() / ".cache" / "streaming-agents"
    cache_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Tighten parent in case it pre-existed with broader perms.
    try:
        os.chmod(cache_dir, 0o700)
    except OSError:
        pass

    # hold the tempfile inside the SAME try/finally that
    # unlinks it. Previously mkstemp + write + chmod sat OUTSIDE the
    # outer try, so an OSError on chmod (corrupted FS, read-only mount)
    # leaked a tempfile containing MCP_CONNECTION_STRING.
    tmp_path: str | None = None
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=str(cache_dir), prefix="mcp-cfg-", suffix=".json",
        )
        try:
            with os.fdopen(tmp_fd, "wb") as f:
                f.write(json.dumps(container_config_obj).encode())
        except Exception:
            try:
                os.close(tmp_fd)
            except OSError:
                pass
            raise
        os.chmod(tmp_path, 0o600)
        primary_container_arg = f"file://{tmp_path}"

        r = _run_aws([
            "ecs", "create-express-gateway-service",
            "--service-name", service_name,
            "--execution-role-arn", exec_role_arn,
            "--infrastructure-role-arn", infra_role_arn,
            "--primary-container", primary_container_arg,
        ], region, timeout=120)

        if r.returncode != 0:
            stderr = r.stderr
            if "already exists" in stderr.lower() or "ServiceAlreadyExists" in stderr or "not idempotent" in stderr.lower() or "still draining" in stderr.lower():
                # Service name is reserved (INACTIVE/DRAINING). Retry with a
                # cryptographically random suffix and up to 3 attempts.
                # previously a modular `time.time` suffix
                # was used, which collides within ~27.7h windows.
                for attempt in range(3):
                    service_name = f"{ECS_SERVICE_NAME}-{secrets.token_hex(3)}"
                    print(f"  [MCP Server] Name conflict, retry {attempt+1}/3 as: {service_name}")
                    r = _run_aws([
                        "ecs", "create-express-gateway-service",
                        "--service-name", service_name,
                        "--execution-role-arn", exec_role_arn,
                        "--infrastructure-role-arn", infra_role_arn,
                        "--primary-container", primary_container_arg,
                    ], region, timeout=120)
                    if r.returncode == 0:
                        break
                    if not ("already exists" in r.stderr.lower()
                            or "still draining" in r.stderr.lower()
                            or "not idempotent" in r.stderr.lower()):
                        raise RuntimeError(f"ECS Express create failed: {r.stderr}")
                else:
                    raise RuntimeError(
                        f"ECS Express create failed after 3 name-suffix retries: {r.stderr}"
                    )
            else:
                raise RuntimeError(f"ECS Express create failed: {stderr}")

        data = json.loads(r.stdout)
        endpoint = _extract_endpoint(data["service"])
        if not endpoint:
            raise RuntimeError("ECS Express service created but no endpoint found in response")
        return (endpoint, service_name)
    finally:
        # unlink the temp config file
        # immediately. Inside the same try/finally that owns mkstemp so
        # an OSError on chmod (or any other line above the outer try
        # body) doesn't leak the secret-bearing tempfile.
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


# ── Health Check Fix ──────────────────────────────────────────────────────────

def _our_target_groups_for_endpoint(
    region: str, endpoint: str | None,
) -> list[dict]:
    """Return ONLY the ecs-gateway-tg-* target groups associated with the
    listener rule matching `endpoint`'s host header.

    the previous implementation mutated EVERY `ecs-gateway-tg-*`
    in the account. On multi-tenant AWS accounts (or accounts with leftover
    blue/green TGs from prior teardowns), this corrupted unrelated ECS
    Express services. We now scope to TGs that the listener rule for OUR
    endpoint's host header points at.

    Falls back to the broad set (with a [warn]) when endpoint is unknown
    or the host-header rule can't be located — preserves the original
    behavior so existing deploys don't break.
    """
    r = _run_aws(["elbv2", "describe-target-groups"], region, timeout=30)
    if r.returncode != 0:
        return []
    try:
        data = json.loads(r.stdout)
    except (ValueError, KeyError):
        return []
    all_candidates = [
        tg for tg in data.get("TargetGroups", [])
        if tg.get("TargetGroupName", "").startswith("ecs-gateway-tg-")
        and tg.get("HealthCheckPort") == str(CONTAINER_PORT)
    ]
    if not endpoint:
        if all_candidates:
            print(
                "  [warn] _fix_alb_health_check called without endpoint — "
                "falling back to all ecs-gateway-tg-*. On multi-tenant "
                "AWS accounts this may modify unrelated services."
            )
        return all_candidates

    # Derive host header from endpoint (https://host/...)
    try:
        from urllib.parse import urlparse
        host = urlparse(endpoint).hostname or ""
    except Exception:
        host = ""
    if not host:
        return all_candidates

    # Find listener rules whose host-header condition matches our endpoint.
    # warn on the broad-set fallback. Previously
    # describe-load-balancers failures silently returned `all_candidates`
    # (multi-tenant leak); the H-C warn only fired on the "no rule
    # matched our host header" path. Both fallbacks now visible.
    lb_r = _run_aws(["elbv2", "describe-load-balancers"], region, timeout=30)
    if lb_r.returncode != 0:
        if all_candidates:
            print(
                f"  [warn] _our_target_groups_for_endpoint: "
                f"`describe-load-balancers` failed; falling back to all "
                f"{len(all_candidates)} ecs-gateway-tg-*. Multi-tenant "
                f"AWS accounts may see unrelated service mutation."
            )
        return all_candidates
    try:
        lbs = json.loads(lb_r.stdout).get("LoadBalancers", [])
    except (ValueError, KeyError):
        if all_candidates:
            print(
                "  [warn] _our_target_groups_for_endpoint: malformed "
                "load-balancer response; falling back to all candidates."
            )
        return all_candidates

    our_tg_arns: set[str] = set()
    for lb in lbs:
        if not lb.get("LoadBalancerName", "").startswith("ecs-express-gateway-alb"):
            continue
        ll = _run_aws([
            "elbv2", "describe-listeners",
            "--load-balancer-arn", lb["LoadBalancerArn"],
        ], region, timeout=15)
        if ll.returncode != 0:
            continue
        try:
            listeners = json.loads(ll.stdout).get("Listeners", [])
        except (ValueError, KeyError):
            continue
        for lst in listeners:
            rr = _run_aws([
                "elbv2", "describe-rules",
                "--listener-arn", lst["ListenerArn"],
            ], region, timeout=15)
            if rr.returncode != 0:
                continue
            try:
                rules = json.loads(rr.stdout).get("Rules", [])
            except (ValueError, KeyError):
                continue
            for rule in rules:
                # Only rules whose host-header condition contains OUR host.
                # DNS hostnames are case-insensitive,
                # but ELB host-header values may be configured with
                # mixed case from CloudFormation copy-paste. Compare
                # lowercased on both sides so we don't fall through to
                # the broad-set fallback for a benign case mismatch.
                matched = False
                host_lc = (host or "").lower()
                for cond in rule.get("Conditions", []):
                    if cond.get("Field") == "host-header":
                        vals = (
                            cond.get("HostHeaderConfig", {}).get("Values", [])
                            or cond.get("Values", [])
                        )
                        if any((v or "").lower() == host_lc for v in vals):
                            matched = True
                            break
                if not matched:
                    continue
                for action in rule.get("Actions", []):
                    if action.get("Type") != "forward":
                        continue
                    fwd = action.get("ForwardConfig", {})
                    for t in fwd.get("TargetGroups", []):
                        arn = t.get("TargetGroupArn")
                        if arn:
                            our_tg_arns.add(arn)
                    if action.get("TargetGroupArn"):
                        our_tg_arns.add(action["TargetGroupArn"])

    if not our_tg_arns:
        # the silent broad-set fallback was
        # itself a regression to the H-2 multi-tenant bug. Warn loudly
        # so the operator notices BEFORE we mutate unrelated TGs. The
        # fallback is preserved so first-deploy scenarios (where the
        # listener rule isn't yet attached) still work.
        if all_candidates:
            print(
                f"  [warn] _our_target_groups_for_endpoint could not match "
                f"endpoint host {host!r} to any listener rule; falling back "
                f"to all {len(all_candidates)} ecs-gateway-tg-* in the "
                f"account. On multi-tenant AWS accounts this may modify "
                f"unrelated ECS Express services."
            )
        return all_candidates
    return [tg for tg in all_candidates if tg.get("TargetGroupArn") in our_tg_arns]


def _fix_alb_health_check(region: str, endpoint: str | None = None) -> None:
    """Update health check on ECS Express target groups bound to OUR service.

    ECS Express provisions blue/green target groups per service: a "live" TG
    holding the current task and a "next" TG that will receive the next
    deployment. Both share the same listener rule with weighted forward.
    Fixing only the first one leaves the other on default config (path '/',
    matcher '200'); the proxy returns 403 on '/' so the un-fixed TG never
    goes healthy, weights never flip, and the URL serves 503.

    scoped to TGs attached to the listener rule that matches
    our endpoint's host header — no longer walks every `ecs-gateway-tg-*`
    in the account (which corrupted unrelated services on multi-tenant
    accounts). Pass `endpoint` (the deploy hands it through).
    """
    candidates = _our_target_groups_for_endpoint(region, endpoint)
    if not candidates:
        return

    fixed_count = 0
    skipped_count = 0
    candidate_arns: list[str] = []
    for tg in candidates:
        tg_arn = tg["TargetGroupArn"]
        candidate_arns.append(tg_arn)

        current_matcher = tg.get("Matcher", {}).get("HttpCode", "200")
        current_path = tg.get("HealthCheckPath", "/")
        if current_matcher == "200-499" and current_path == "/mcp":
            skipped_count += 1
            continue

        _run_aws([
            "elbv2", "modify-target-group",
            "--target-group-arn", tg_arn,
            "--health-check-path", "/mcp",
            "--matcher", "HttpCode=200-499",
        ], region)
        fixed_count += 1

    if fixed_count:
        print(f"  Fixed ALB health check on {fixed_count} target group(s) (path: /mcp, matcher: 200-499)")
    if skipped_count and not fixed_count:
        print(f"  ALB health check already configured on {skipped_count} target group(s)")

    # Manual blue/green flip: ensure every listener rule that fans out to
    # our TGs sends 100% weight to whichever TG has a registered target.
    if candidate_arns:
        _flip_listener_weights_to_registered_tg(region, candidate_arns)


def _flip_listener_weights_to_registered_tg(region: str, candidate_tg_arns: list[str]) -> None:
    """For each listener rule that forwards to two of our TGs, set weight 100%
    to the one with a registered target. ECS Express normally manages this,
    but if the new TG never goes healthy on first deploy (because of the
    health check bug we just fixed), the weight stays 100% on an empty TG
    and the URL serves 503.
    """
    # Map TG ARN -> has registered targets?
    has_targets: dict[str, bool] = {}
    for arn in candidate_tg_arns:
        h = _run_aws([
            "elbv2", "describe-target-health",
            "--target-group-arn", arn,
        ], region, timeout=15)
        if h.returncode != 0:
            has_targets[arn] = False
            continue
        try:
            hd = json.loads(h.stdout)
            has_targets[arn] = bool(hd.get("TargetHealthDescriptions"))
        except (ValueError, KeyError):
            has_targets[arn] = False

    # Find ECS Express ALB listener
    lb_r = _run_aws(["elbv2", "describe-load-balancers"], region, timeout=30)
    if lb_r.returncode != 0:
        return
    try:
        lbs = json.loads(lb_r.stdout).get("LoadBalancers", [])
    except (ValueError, KeyError):
        return

    listener_arns: list[str] = []
    for lb in lbs:
        if not lb.get("LoadBalancerName", "").startswith("ecs-express-gateway-alb"):
            continue
        ll = _run_aws([
            "elbv2", "describe-listeners",
            "--load-balancer-arn", lb["LoadBalancerArn"],
        ], region, timeout=15)
        if ll.returncode != 0:
            continue
        try:
            for lst in json.loads(ll.stdout).get("Listeners", []):
                listener_arns.append(lst["ListenerArn"])
        except (ValueError, KeyError):
            continue

    flipped = 0
    for listener_arn in listener_arns:
        rr = _run_aws([
            "elbv2", "describe-rules",
            "--listener-arn", listener_arn,
        ], region, timeout=15)
        if rr.returncode != 0:
            continue
        try:
            rules = json.loads(rr.stdout).get("Rules", [])
        except (ValueError, KeyError):
            continue
        for rule in rules:
            if rule.get("IsDefault"):
                continue
            for action in rule.get("Actions", []):
                if action.get("Type") != "forward":
                    continue
                fwd = action.get("ForwardConfig", {})
                tgs = fwd.get("TargetGroups", [])
                if len(tgs) < 2:
                    continue
                rule_arns = [t.get("TargetGroupArn", "") for t in tgs]
                if not all(a in candidate_tg_arns for a in rule_arns):
                    continue
                # only flip when EXACTLY ONE TG has
                # registered targets. When both blue and green are
                # healthy, ECS Express is mid-flip in its own
                # controlled blue/green swap — intervening here picks
                # whichever TG appears first in the response order
                # (often the OLD task), rolls traffic backward, and
                # ping-pongs with the controller. Only the unambiguous
                # "exactly one side has any targets" case is the race
                # we're solving.
                with_targets = [a for a in rule_arns if has_targets.get(a)]
                if len(with_targets) != 1:
                    continue
                healthy_arn = with_targets[0]
                # Already weighted correctly?
                already_ok = all(
                    (t.get("Weight", 0) == 100) == (t.get("TargetGroupArn") == healthy_arn)
                    for t in tgs
                )
                if already_ok:
                    continue
                new_tgs = [
                    {"TargetGroupArn": a, "Weight": 100 if a == healthy_arn else 0}
                    for a in rule_arns
                ]
                new_action = json.dumps([{
                    "Type": "forward",
                    "ForwardConfig": {
                        "TargetGroups": new_tgs,
                        "TargetGroupStickinessConfig": {"Enabled": False},
                    },
                }])
                _run_aws([
                    "elbv2", "modify-rule",
                    "--rule-arn", rule["RuleArn"],
                    "--actions", new_action,
                ], region, timeout=15)
                flipped += 1

    if flipped:
        print(f"  Flipped listener weights on {flipped} rule(s) to registered targets")


# ── Wait for Healthy ──────────────────────────────────────────────────────────

def _wait_for_healthy(endpoint: str, auth_token: str, timeout: int = HEALTH_CHECK_TIMEOUT) -> bool:
    """Poll MCP endpoint until it responds with 200."""
    import urllib.request
    import urllib.error

    url = f"{endpoint}/mcp"
    body = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "deploy-check", "version": "1.0"},
        },
        "id": 1,
    }).encode()

    start = time.time()
    attempt = 0
    while time.time() - start < timeout:
        attempt += 1
        try:
            req = urllib.request.Request(
                url, data=body, method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                    "Authorization": f"Bearer {auth_token}",
                },
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, urllib.error.HTTPError, OSError):
            pass

        elapsed = int(time.time() - start)
        remaining = timeout - elapsed
        if attempt % 4 == 0:
            print(f"  Waiting for service... ({elapsed}s elapsed, {remaining}s remaining)")
        time.sleep(HEALTH_CHECK_INTERVAL)

    return False


# ── Two-phase Deploy (parallelizable) ────────────────────────────────────────

def build_mcp_image(
    region: str = DEFAULT_REGION,
    docker_context: Path | None = None,
) -> dict:
    """Phase A — ECR repo + Docker build/push + IAM roles.

    Independent of the MongoDB connection string, so this can run in parallel
    with Atlas cluster creation. Returns a dict with image_uri, exec_role,
    and infra_role for handoff to create_mcp_service.
    """
    if docker_context is None:
        docker_context = _project_root() / "mcp-server"

    print("  [MCP Server] (build phase) Ensuring ECR repository...")
    repo_uri = _ensure_ecr_repo(region)

    print("  [MCP Server] (build phase) Building & pushing image...")
    image_uri = _build_and_push(docker_context, repo_uri, region)

    print("  [MCP Server] (build phase) Ensuring IAM roles...")
    exec_role = _ensure_execution_role(region)
    infra_role = _ensure_infrastructure_role(region)

    return {
        "image_uri": image_uri,
        "exec_role": exec_role,
        "infra_role": infra_role,
    }


def create_mcp_service(
    image_uri: str,
    exec_role: str,
    infra_role: str,
    region: str,
    auth_token: str,
    mongo_conn: str,
) -> tuple:
    """Phase B — ECS Express + ALB health check.

    Requires the actual MongoDB connection string. Run this AFTER the Atlas
    cluster is IDLE and the connection string has been persisted.
    """
    if not mongo_conn:
        raise ValueError("create_mcp_service requires a non-empty mongo_conn")

    print("  [MCP Server] (service phase) Creating ECS Express service...")
    endpoint, svc_name = _create_ecs_express(
        image_uri, exec_role, infra_role, region, auth_token, mongo_conn,
    )
    print(f"  [MCP Server] Service endpoint: {endpoint} (name: {svc_name})")

    print("  [MCP Server] (service phase) Configuring ALB health check...")
    time.sleep(10)
    _fix_alb_health_check(region, endpoint=endpoint)

    print("  [MCP Server] (service phase) Waiting for healthy (~2-3 min)...")
    # Run the health-check / listener-flip in a loop alongside _wait_for_healthy:
    # ECS Express registers targets ~30-60s after service create, so the first
    # _fix_alb_health_check above may not see the registered target. Re-running
    # every ~60s catches the moment the new TG has a registered IP and flips
    # listener weights to it, unblocking the user-facing URL.
    healthy = _wait_for_healthy_with_alb_remediation(endpoint, auth_token, region)
    if not healthy:
        print("  [MCP Server] WARNING: Service did not become healthy within timeout.")
        print("  [MCP Server] It may still start shortly. URL is saved for retry.")
    else:
        print("  [MCP Server] Service is healthy and responding!")

    return (endpoint, auth_token)


def _wait_for_healthy_with_alb_remediation(
    endpoint: str, auth_token: str, region: str,
    timeout: int = HEALTH_CHECK_TIMEOUT,
) -> bool:
    """Like _wait_for_healthy, but periodically re-runs ALB health check
    remediation. The first remediation pass may run before ECS Express has
    registered targets; re-running every 60s catches the registration moment.
    """
    import urllib.request
    import urllib.error

    url = f"{endpoint}/mcp"
    body = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "deploy-check", "version": "1.0"},
        },
        "id": 1,
    }).encode()

    start = time.time()
    attempt = 0
    last_remediation = start  # we just ran it once already; next at +60s
    while time.time() - start < timeout:
        attempt += 1
        try:
            req = urllib.request.Request(
                url, data=body, method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                    "Authorization": f"Bearer {auth_token}",
                },
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, urllib.error.HTTPError, OSError):
            pass

        elapsed = int(time.time() - start)
        remaining = timeout - elapsed
        if attempt % 4 == 0:
            print(f"  Waiting for service... ({elapsed}s elapsed, {remaining}s remaining)")
        # Re-run ALB remediation every 60s in case targets were just registered.
        if time.time() - last_remediation >= 60:
            _fix_alb_health_check(region, endpoint=endpoint)
            last_remediation = time.time()
        time.sleep(HEALTH_CHECK_INTERVAL)

    return False


# ── Main Deploy Function ──────────────────────────────────────────────────────

def deploy_mcp_server(
    region: str = DEFAULT_REGION,
    auth_token: str | None = None,
    docker_context: Path | None = None,
    mongo_conn: str = "",
) -> tuple:
    """
    Deploy MongoDB MCP Server to ECS Express Mode (single-phase).

    Equivalent to build_mcp_image + create_mcp_service in sequence.

    Returns:
        (service_url: str, auth_token: str)
    """
    if not auth_token:
        auth_token = secrets.token_urlsafe(32)

    print("\n  [MCP Server] Checking for existing deployment...")

    existing = _get_existing_service(region)
    if existing:
        endpoint = _extract_endpoint(existing)
        if endpoint:
            print(f"  [MCP Server] Already deployed at: {endpoint}")
            if _wait_for_healthy(endpoint, auth_token, timeout=30):
                return (endpoint, auth_token)
            print("  [MCP Server] Existing service not responding with current token.")
            print("  [MCP Server] Deleting and redeploying...")
            destroy_mcp_server(region)
            time.sleep(15)

    build = build_mcp_image(region=region, docker_context=docker_context)
    return create_mcp_service(
        image_uri=build["image_uri"],
        exec_role=build["exec_role"],
        infra_role=build["infra_role"],
        region=region,
        auth_token=auth_token,
        mongo_conn=mongo_conn,
    )


# ── Destroy ───────────────────────────────────────────────────────────────────

def destroy_mcp_server(region: str = DEFAULT_REGION) -> None:
    """Delete the ECS Express MCP service(s).

    Discovers services by trying multiple sources:
      1. ECS list-services (preferred — needs ecs:ListServices)
      2. CloudWatch log groups under /aws/ecs/default/<name>-* (fallback when
         IAM lacks ecs:ListServices but has logs:DescribeLogGroups)
      3. The base ECS_SERVICE_NAME (last resort)
    """
    print("  [MCP Server] Deleting ECS Express service...")
    account_id = _get_account_id(region)
    deleted = False
    discovered_via_logs = False

    # Source 1: ECS list-services (preferred)
    names_to_try: list[str] = [ECS_SERVICE_NAME]
    r = _run_aws(["ecs", "list-services", "--cluster", "default"], region, timeout=30)
    if r.returncode == 0:
        try:
            data = json.loads(r.stdout)
            for arn in data.get("serviceArns", []):
                svc = arn.rsplit("/", 1)[-1]
                if svc.startswith(ECS_SERVICE_NAME) and svc not in names_to_try:
                    names_to_try.append(svc)
        except (ValueError, KeyError):
            pass
    elif "AccessDenied" in r.stderr or "not authorized" in r.stderr:
        # Source 2: discover from CloudWatch log groups. ECS Express creates
        # one log group per service named /aws/ecs/default/<service>-<hash>.
        log_r = _run_aws([
            "logs", "describe-log-groups",
            "--log-group-name-prefix", f"/aws/ecs/default/{ECS_SERVICE_NAME}",
        ], region, timeout=30)
        if log_r.returncode == 0:
            try:
                groups = json.loads(log_r.stdout).get("logGroups", [])
                for g in groups:
                    # /aws/ecs/default/mongodb-mcp-77779-fd86 → mongodb-mcp-77779
                    short = g.get("logGroupName", "").split("/")[-1]
                    parts = short.rsplit("-", 1)
                    if len(parts) == 2 and parts[0].startswith(ECS_SERVICE_NAME):
                        candidate = parts[0]
                        if candidate not in names_to_try:
                            names_to_try.append(candidate)
                            discovered_via_logs = True
            except (ValueError, KeyError):
                pass
        if discovered_via_logs:
            print("  [MCP Server] (ecs:ListServices denied — discovered services via log groups)")

    for name in names_to_try:
        arn = f"arn:aws:ecs:{region}:{account_id}:service/default/{name}"
        r = _run_aws([
            "ecs", "delete-express-gateway-service", "--service-arn", arn,
        ], region, timeout=60)
        if r.returncode == 0:
            print(f"  [MCP Server] Deleted: {name}")
            deleted = True
        elif "ServiceNotFoundException" not in r.stderr and "not found" not in r.stderr.lower():
            # Try by name as fallback
            r2 = _run_aws([
                "ecs", "delete-express-gateway-service", "--service-name", name,
            ], region, timeout=60)
            if r2.returncode == 0:
                print(f"  [MCP Server] Deleted: {name}")
                deleted = True
            elif "AccessDenied" in r2.stderr or "not authorized" in r2.stderr:
                print(f"  [MCP Server] Cannot delete {name}: IAM lacks ecs:DeleteExpressGatewayService")
                print(f"  [MCP Server] Delete manually: aws ecs delete-express-gateway-service --service-arn {arn}")

    if not deleted:
        print("  [MCP Server] No active services found (already deleted).")

    # clean up orphaned ALB target groups.
    # ECS Express creates blue/green target group pairs (ecs-gateway-tg-*)
    # that don't auto-delete when the service is deleted. They accumulate
    # across destroys and distort _fix_alb_health_check on next deploy.
    _cleanup_orphan_target_groups(region)

    # clean up CloudWatch log groups created by ECS Express.
    # Each service creates /aws/ecs/default/<service>-<hash>; these never
    # auto-delete, accumulate cost, and may rebind to unrelated services
    # with the same prefix and silently collect their data.
    _cleanup_log_groups(region, names_to_try)


def _cleanup_log_groups(region: str, service_names: list[str]) -> None:
    """Best-effort delete of /aws/ecs/default/<service>-<hash> log groups
    that belong to OUR ECS Express services.

    Scope to log groups whose
    name exactly matches `/aws/ecs/default/<service-name>-<hash>` where
    `<hash>` is exactly 8 hex chars (ECS Express's deterministic suffix).
    Prefix-only matching would catch `mcp-server-prod`, `mcp-server-2`,
    etc. on the same AWS account — multi-tenant scope leak mirroring
    the H-2 bug the prior pass just fixed.

    Silently no-ops on IAM denial (some operators have
    `logs:DescribeLogGroups` but not `logs:DeleteLogGroup`).
    """
    import re as _re
    deleted = 0
    iam_denied = False
    seen_prefixes: set[str] = set()
    for name in service_names:
        prefix = f"/aws/ecs/default/{name}"
        if prefix in seen_prefixes:
            continue
        seen_prefixes.add(prefix)
        # tight regex pinning the ECS Express log-group format.
        # the actual AWS-generated suffix is 4 hex chars
        # (e.g. /aws/ecs/default/mongodb-mcp-fd86 — see service-discovery
        # logic at the call sites). The earlier {8,} requirement matched
        # nothing, so the cleanup silently deleted 0 log groups across
        # every destroy. {4,} matches the real format AND still rejects
        # `mongodb-mcp-prod` / `mongodb-mcp-staging` (non-hex tails).
        exact_re = _re.compile(rf"^{_re.escape(prefix)}-[0-9a-f]{{4,}}$")
        r = _run_aws([
            "logs", "describe-log-groups",
            "--log-group-name-prefix", prefix,
        ], region, timeout=30)
        if r.returncode != 0:
            continue
        try:
            groups = json.loads(r.stdout).get("logGroups", [])
        except (ValueError, KeyError):
            continue
        # break the inner loop (not return) on IAM
        # AccessDenied so retry-suffixed service names later in
        # `service_names` get a chance to be inspected. Emit the warning
        # exactly once via a sentinel.
        for g in groups:
            lg_name = g.get("logGroupName", "")
            if not lg_name or not exact_re.match(lg_name):
                # Skip foreign log groups (e.g. mcp-server-prod) that
                # share the same path prefix but aren't ours.
                continue
            d_r = _run_aws([
                "logs", "delete-log-group", "--log-group-name", lg_name,
            ], region, timeout=30)
            if d_r.returncode == 0:
                deleted += 1
            elif "AccessDenied" in d_r.stderr or "not authorized" in d_r.stderr:
                iam_denied = True
                break  # inner loop only — try other service prefixes
        if iam_denied:
            # No point continuing to enumerate other prefixes; the IAM
            # denial applies account-wide.
            break
    if iam_denied:
        print(
            "  [MCP Server] (logs:DeleteLogGroup denied — leaving "
            "remaining log groups in place)"
        )
    if deleted:
        print(f"  [MCP Server] Cleaned up {deleted} CloudWatch log group(s).")


def _cleanup_orphan_target_groups(region: str) -> None:
    """Delete `ecs-gateway-tg-*` target groups with no registered targets.

     Best-effort: IAM may lack elasticloadbalancing
    permissions, in which case we log + continue rather than failing
    the entire destroy.
    """
    r = _run_aws(["elbv2", "describe-target-groups"], region, timeout=30)
    if r.returncode != 0:
        if "AccessDenied" in r.stderr or "not authorized" in r.stderr:
            print("  [MCP Server] (elbv2 describe-target-groups denied — skipping TG cleanup)")
        return
    try:
        data = json.loads(r.stdout)
    except (ValueError, KeyError):
        return
    tgs = data.get("TargetGroups", [])
    candidates = [tg for tg in tgs
                  if tg.get("TargetGroupName", "").startswith("ecs-gateway-tg-")]
    if not candidates:
        return
    # identify orphan by ABSENCE of LoadBalancerArns
    # rather than absence of registered targets. ECS deregisters
    # targets asynchronously (30-90s) after service delete; using
    # target absence skipped the cleanup window. A TG with no
    # attached LBs is unambiguously orphan regardless of target state.
    deleted_count = 0
    for tg in candidates:
        arn = tg.get("TargetGroupArn", "")
        if not arn:
            continue
        lb_arns = tg.get("LoadBalancerArns") or []
        if lb_arns:
            # Still attached to a load balancer — ECS Express may be
            # flipping to it; leave it alone regardless of registered
            # target state. M-8 narrowed orphan detection to "no LB
            # attached" precisely to avoid tearing down live TGs.
            # removed dead targets_remain computation.
            continue
        # No LB attached → unambiguously orphan, safe to delete.
        d_r = _run_aws([
            "elbv2", "delete-target-group", "--target-group-arn", arn,
        ], region, timeout=30)
        if d_r.returncode == 0:
            deleted_count += 1
    if deleted_count:
        print(f"  [MCP Server] Cleaned up {deleted_count} orphan ALB target group(s).")


def _get_account_id(region: str) -> str:
    """Get current AWS account ID."""
    r = subprocess.run(
        ["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"],
        capture_output=True, text=True, timeout=10,
    )
    return r.stdout.strip() if r.returncode == 0 else ""


# ── CLI Entry Point ───────────────────────────────────────────────────────────

def _resolve_cloud_region() -> str:
    """Resolve the AWS region the way deploy.py/destroy.py do: prefer
    TF_VAR_cloud_region from .env, then the environment, then DEFAULT_REGION.

    Keeps standalone ``uv run mcp-deploy [--destroy]`` in lockstep with the
    orchestrated deploy/destroy (both read TF_VAR_cloud_region) so a teardown
    targets the same region the service was actually created in.
    """
    try:
        creds_path = _project_root() / ".env"
        if creds_path.exists():
            from dotenv import dotenv_values
            val = (dotenv_values(creds_path).get("TF_VAR_cloud_region") or "").strip()
            if val:
                return val
    except Exception:
        pass
    return (os.environ.get("TF_VAR_cloud_region") or "").strip() or DEFAULT_REGION


def main():
    """CLI entry point for standalone usage."""
    import argparse

    parser = argparse.ArgumentParser(description="Deploy MongoDB MCP Server to AWS ECS Express Mode")
    parser.add_argument("--destroy", action="store_true", help="Tear down the MCP server")
    parser.add_argument(
        "--region", default=None,
        help="AWS region (default: $TF_VAR_cloud_region from .env, else us-east-1)",
    )
    parser.add_argument("--token", help="Auth token (auto-generated if not provided)")
    args = parser.parse_args()

    # An explicit --region wins; otherwise match deploy/destroy's source.
    region = args.region or _resolve_cloud_region()

    if args.destroy:
        destroy_mcp_server(region)
        return

    # Load credentials from .env if available
    mongo_conn = ""
    creds_path = _project_root() / ".env"
    if creds_path.exists():
        try:
            from dotenv import dotenv_values
            creds = dotenv_values(creds_path)
            mongo_conn = creds.get("TF_VAR_mongodb_connection_string", "")
        except ImportError:
            pass

    if not mongo_conn:
        mongo_conn = os.environ.get("TF_VAR_mongodb_connection_string", "")

    if not mongo_conn:
        print("ERROR: MongoDB connection string not found.")
        print("Set TF_VAR_mongodb_connection_string or add it to .env")
        sys.exit(1)

    # Pre-flight
    errors = check_prerequisites()
    if errors:
        print("Pre-flight check failed:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    url, token = deploy_mcp_server(
        region=region,
        auth_token=args.token,
        mongo_conn=mongo_conn,
    )

    print(f"\n  MCP Server URL:   {url}")
    print(f"  MCP Auth Token:   {token}")

    # Auto-save to .env
    creds_path = _project_root() / ".env"
    _save_mcp_credentials(creds_path, url, token)


def _save_mcp_credentials(creds_path: Path, url: str, token: str) -> None:
    """Save MCP URL and token to .env.

    delegates to scripts.common.env_file.atomic_write_env
    — the canonical writer that's also used by deploy.py:_save_env_many.
    Single writer means no race between this function and a background
    MCP build thread / parallel deploy invocation. The shared helper
    handles:
      * mode 0o600 atomically
      * unique temp file path (L-NEW-1 — was deterministic .env.mcp-tmp)
      * line-based merge preserving comments and unrelated keys
      * structure-preserving update of existing TF_VAR_mcp_* lines
    """
    from scripts.common.env_file import atomic_write_env
    is_new_file = not creds_path.exists()
    atomic_write_env(creds_path, {
        "TF_VAR_mcp_server_url": url,
        "TF_VAR_mcp_auth_token": token,
    })
    if is_new_file:
        print("  [ok] Created .env with MCP credentials")
    else:
        print("  [ok] Saved MCP credentials to .env")


if __name__ == "__main__":
    main()
