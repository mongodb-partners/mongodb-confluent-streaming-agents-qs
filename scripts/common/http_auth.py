"""Shared HTTP Basic-Auth helpers.

The base64 ``key:secret`` Basic-Auth token was hand-rolled in 17 places across
9 modules (Flink REST, Kafka REST, Schema Registry, MCP). Centralising it here
removes that duplication and gives one place to get the encoding right.
"""

from __future__ import annotations

import base64


def basic_auth_token(key: str, secret: str) -> str:
    """Return the base64 ``key:secret`` token (no ``Basic `` prefix).

    This is the value used in ``Authorization: Basic <token>`` and in the
    Confluent REST ``Authorization`` headers throughout the deploy scripts.
    """
    return base64.b64encode(f"{key}:{secret}".encode()).decode()


def basic_auth_header(key: str, secret: str) -> str:
    """Return a full ``Basic <token>`` Authorization header value."""
    return f"Basic {basic_auth_token(key, secret)}"
