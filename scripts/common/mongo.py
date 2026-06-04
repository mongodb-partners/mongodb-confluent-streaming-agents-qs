"""Shared MongoClient factory and connection-string builder.

Consolidates the URI-build + MongoClient instantiation that was previously
duplicated across asp_setup.py, dashboard.py, destroy.py, and
pipeline_reset.py. Provides consistent options (appName for Atlas
observability, retryWrites/retryReads, pool sizes) and caches clients
within the process so repeated calls with the same (uri, app_name) reuse
the connection pool.
"""

from __future__ import annotations

from functools import lru_cache
from urllib.parse import quote_plus

from pymongo import MongoClient


def build_uri(connection_string: str, username: str = "", password: str = "") -> str:
    """Build a MongoDB connection URI from possibly-partial inputs.

    Tolerates the same shapes the legacy inline blocks did:
    - bare host (no scheme)              → mongodb+srv:// + creds + host
    - URI without embedded credentials   → inject quote_plus(user):quote_plus(pw)
    - URI with embedded credentials      → return unchanged
    - URI with query string or path      → preserved

    .strip username and password to tolerate trailing
    newlines / CRs introduced by .env copy-paste. Without this, an
    accidental newline in a password gets URL-encoded as %0A and the
    resulting URI produces a cryptic ServerSelectionTimeoutError.
    """
    username = (username or "").strip()
    password = (password or "").strip()
    connection_string = (connection_string or "").strip()
    if "://" in connection_string:
        scheme, rest = connection_string.split("://", 1)
        # If a `@` appears before the first `/`, credentials are already embedded
        host_part = rest.split("/", 1)[0]
        if "@" in host_part:
            return connection_string
        if not username and not password:
            return connection_string
        return f"{scheme}://{quote_plus(username)}:{quote_plus(password)}@{rest}"
    # Bare host
    return f"mongodb+srv://{quote_plus(username)}:{quote_plus(password)}@{connection_string}"


# maxsize=16 leaves headroom for new callers without
# evicting an in-use dashboard client.
@lru_cache(maxsize=16)
def get_client(
    uri: str,
    app_name: str,
    *,
    server_selection_timeout_ms: int = 5000,
) -> MongoClient:
    """Return a cached, configured MongoClient.

    Cached by (uri, app_name) so repeated calls in the same process share
    a single connection pool. `app_name` shows up in Atlas server logs
    and the system.profiler — pick a stable identifier per caller.
    """
    return MongoClient(
        uri,
        appName=app_name,
        serverSelectionTimeoutMS=server_selection_timeout_ms,
        connectTimeoutMS=10000,
        socketTimeoutMS=20000,
        retryWrites=True,
        retryReads=True,
        maxPoolSize=20,
        minPoolSize=1,
    )
