"""Secret redaction for CLI/log output.

deploy/destroy/asp-setup emit lines into the session
log file that may contain credentials (MongoDB URIs with embedded
passwords, Kafka API secrets, MCP auth tokens, etc.). The log file lives
in the repo working tree and is easy to share by accident.

This module provides a single ``redact(text)`` helper that masks the
value side of secret-shaped key/value pairs and MongoDB URI passwords,
leaving non-secret identifiers (environment IDs, cluster names, hosts)
intact.

Patterns are intentionally conservative: a false-positive redaction is
preferable to a false-negative leak. Identifier-shaped values
(`env-abc12345`, `lkc-xxxxx`, host names, etc.) are matched against a
narrow allow-list of secret keys, not broad value patterns.

added coverage for MCP bearer tokens
(`MDB_MCP_HTTP_CLIENT_AUTH`), raw HTTP `Authorization: Bearer/Basic`
headers, and `ATLAS_PUBLIC_KEY` (half of HTTPDigestAuth and useful for
account fingerprinting).
"""
from __future__ import annotations

import re

# Mongo/Atlas URI with embedded user:pass. Mask everything between
# `//<user>:` and `@`.
_URI_USERPASS_RE = re.compile(
    r"(?P<scheme>mongodb(?:\+srv)?://[^:/\s]+:)(?P<pw>[^@\s]+)(?=@)",
    re.IGNORECASE,
)

# HTTP Authorization-header form. Matches both:
#   * Header dumps:   `Authorization: Bearer xxx`, `authorization: basic yyy`
#   * Env-var assigns: `MDB_MCP_HTTP_CLIENT_AUTH=Bearer xxx`
# Mask the token following the auth-scheme word. The key/separator on
# the left is captured into `prefix` so we preserve scheme context.
_AUTH_HEADER_RE = re.compile(
    r"(?P<prefix>"
    r"(?:"
    r"(?:authorization|proxy-authorization|[\w-]*(?:client[_-]?auth|bearer[_-]?token)[\w-]*)"
    r"\s*[:=]\s*"
    r")?"
    r"(?:bearer|basic|digest|token)\s+"
    r")(?P<tok>[A-Za-z0-9+/=._\-]+)",
    re.IGNORECASE,
)

# Heuristic for secret-shaped key=value or "key": "value" pairs.
# The key list is the allow-list of "this looks like a secret"; everything
# else passes through.
#
# expanded coverage. `client[_-]?auth` catches
# `MDB_MCP_HTTP_CLIENT_AUTH=Bearer ...`. `bearer` / `authorization`
# catch literal `Authorization=Bearer xxx` keyvalue forms (the
# header form above handles colon-separated header dumps).
# `public[_-]?key` catches Atlas paired-auth — public key alone isn't
# a credential but paired with private it's the HTTPDigestAuth input.
_SECRET_KEYS = (
    "password", "passwd", "secret", "api[_-]?key", "auth[_-]?token",
    "access[_-]?key", "session[_-]?token", "private[_-]?key", "voyage[_-]?key",
    "client[_-]?auth", "bearer", "authorization",
    "public[_-]?key",
)

# Build one regex matching: word_with_secret_key (= | : | space) value
# where value is quoted or unquoted. Both YAML-ish and JSON-ish.
_SECRET_KV_RE = re.compile(
    r"""
    (?P<key>[\w-]*(?:""" + "|".join(_SECRET_KEYS) + r""")[\w-]*)        # key
    (?P<sep>\s*[:=]\s*)                                                # separator (captures surrounding whitespace too — S12)
    (?P<quote>['"]?)                                                   # optional quote
    (?P<val>[^\s'"]+?)                                                 # value
    (?P=quote)                                                         # matching close-quote
    (?=\s|$|[,;])                                                      # end boundary
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _mask(value: str) -> str:
    """Mask a secret value, keeping first 4 / last 2 chars for traceability.

    Values shorter than 8 chars are fully redacted to avoid leaking.
    """
    if len(value) < 8:
        return "***"
    return f"{value[:4]}…{value[-2:]}"


def redact(text: str) -> str:
    """Return ``text`` with detected secrets masked.

    Idempotent: redact(redact(x)) == redact(x). To guarantee this, the
    replacement function skips values that already look masked (contain
    the ``…`` mask separator or are exactly ``***``). Without this skip,
    re-application would re-mask the partial-prefix form to ``***``.
    """
    if not text:
        return text

    # URI passwords first (more specific).
    def _repl_uri(m: "re.Match[str]") -> str:
        return f"{m.group('scheme')}***"
    text = _URI_USERPASS_RE.sub(_repl_uri, text)

    # HTTP Authorization-header form (handle BEFORE the K/V
    # regex; the colon-separated `Authorization: Bearer xxx` form would
    # also match _SECRET_KV_RE under the `authorization` alternation,
    # but this dedicated regex preserves the scheme word for context).
    def _repl_auth(m: "re.Match[str]") -> str:
        tok = m.group('tok')
        if tok == "***" or "…" in tok:
            return m.group(0)
        return f"{m.group('prefix')}{_mask(tok)}"
    text = _AUTH_HEADER_RE.sub(_repl_auth, text)

    # Key/value pairs.
    def _repl_kv(m: "re.Match[str]") -> str:
        val = m.group('val')
        # idempotency — don't re-mask an already-masked
        # value (e.g. `hunt…et` from a prior pass).
        if val == "***" or "…" in val:
            return m.group(0)
        return (
            f"{m.group('key')}{m.group('sep')}"
            f"{m.group('quote')}{_mask(val)}{m.group('quote')}"
        )
    text = _SECRET_KV_RE.sub(_repl_kv, text)

    return text
