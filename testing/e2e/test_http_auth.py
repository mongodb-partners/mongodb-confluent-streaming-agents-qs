"""Tests for the shared HTTP Basic-Auth helper (scripts/common/http_auth.py).

This helper replaced 17 hand-rolled base64 `key:secret` sites across 9 modules.
It must produce exactly the same token those sites did.
"""

from __future__ import annotations

import base64

from scripts.common.http_auth import basic_auth_header, basic_auth_token


def test_token_matches_manual_base64():
    key, secret = "AKIAKEY", "s3cr3t/val+ue="
    expected = base64.b64encode(f"{key}:{secret}".encode()).decode()
    assert basic_auth_token(key, secret) == expected


def test_header_has_basic_prefix():
    token = basic_auth_token("k", "s")
    assert basic_auth_header("k", "s") == f"Basic {token}"


def test_roundtrip_decodes_to_key_colon_secret():
    token = basic_auth_token("user", "pass")
    assert base64.b64decode(token).decode() == "user:pass"
