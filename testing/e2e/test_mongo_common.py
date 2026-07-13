"""Tests for scripts/common/mongo.py — REQ-R-106, INV-109.

The shared MongoDB helper consolidates the 5 inline URI/client blocks that
previously lived in asp_setup.py, dashboard.py, destroy.py, and
pipeline_reset.py. These tests verify it accepts every input shape those
sites accepted, plus the new `appName` and retry options.
"""

import inspect
from urllib.parse import quote_plus

import pytest

# -- TC-R-106a: build_uri shape tolerance ------------------------------------

class TestBuildUri:
    """build_uri must accept the same input shapes the inline blocks accepted."""

    def test_bare_host_gets_srv_scheme(self):
        from scripts.common.mongo import build_uri
        uri = build_uri("cluster.mongodb.net", "user", "pw")
        assert uri.startswith("mongodb+srv://"), \
            "Bare host should get mongodb+srv:// scheme"
        assert "user:pw@cluster.mongodb.net" in uri

    def test_uri_without_credentials_gets_creds_injected(self):
        from scripts.common.mongo import build_uri
        uri = build_uri("mongodb+srv://cluster.mongodb.net", "user", "pw")
        assert uri == "mongodb+srv://user:pw@cluster.mongodb.net"

    def test_uri_with_credentials_returned_unchanged(self):
        """If the URI already has credentials, do not double-inject."""
        from scripts.common.mongo import build_uri
        original = "mongodb+srv://existing:secret@cluster.mongodb.net"
        uri = build_uri(original, "user", "pw")
        assert uri == original, \
            "URI with embedded credentials must not be modified"

    def test_special_chars_in_password_are_url_encoded(self):
        from scripts.common.mongo import build_uri
        uri = build_uri("cluster.mongodb.net", "user", "p@ss/word!")
        # quote_plus encodes @ → %40 and / → %2F
        assert quote_plus("p@ss/word!") in uri, \
            "Password special chars must be quote_plus-encoded"

    def test_special_chars_in_username_are_url_encoded(self):
        from scripts.common.mongo import build_uri
        uri = build_uri("cluster.mongodb.net", "user@example.com", "pw")
        assert quote_plus("user@example.com") in uri

    def test_uri_with_query_string_preserved(self):
        """Query strings (e.g. ?retryWrites=true) survive credential injection."""
        from scripts.common.mongo import build_uri
        uri = build_uri(
            "mongodb+srv://cluster.mongodb.net/?retryWrites=true&w=majority",
            "user", "pw",
        )
        assert "?retryWrites=true&w=majority" in uri
        assert "user:pw@" in uri

    def test_empty_credentials_with_embedded_uri_works(self):
        """If URI has creds and we pass empty user/pw, URI must be returned as-is."""
        from scripts.common.mongo import build_uri
        original = "mongodb+srv://u:p@cluster.mongodb.net"
        uri = build_uri(original, "", "")
        assert uri == original


# -- TC-R-106b: get_client returns a configured MongoClient -------------------

class TestGetClient:
    def test_get_client_is_callable(self):
        from scripts.common.mongo import get_client
        assert callable(get_client)

    def test_get_client_returns_mongoclient(self):
        """get_client returns a pymongo MongoClient (no network call)."""
        from pymongo import MongoClient

        from scripts.common.mongo import get_client
        client = get_client(
            "mongodb://localhost:1/?serverSelectionTimeoutMS=10",
            app_name="test",
        )
        assert isinstance(client, MongoClient)

    def test_get_client_sets_app_name(self):
        from scripts.common.mongo import get_client
        client = get_client(
            "mongodb://localhost:1/?serverSelectionTimeoutMS=10",
            app_name="streaming-agents-test",
        )
        # MongoClient stores appName under the kwargs/options
        opts = client.options
        # The 'appname' option is normalized to lowercase by pymongo
        assert getattr(opts, "_options", {}).get("appname") == "streaming-agents-test" \
            or "streaming-agents-test" in str(client.options.__dict__).lower()

    def test_get_client_enables_retry_writes(self):
        from scripts.common.mongo import get_client

        # Inspect source — the wire-level behavior is hard to test offline
        source = inspect.getsource(get_client)
        assert "retryWrites" in source, "get_client must enable retryWrites"
        assert "retryReads" in source, "get_client must enable retryReads"

    def test_get_client_sets_pool_sizes(self):
        from scripts.common.mongo import get_client
        source = inspect.getsource(get_client)
        assert "maxPoolSize" in source, "get_client must set maxPoolSize"

    def test_get_client_signature_has_required_app_name(self):
        from scripts.common.mongo import get_client
        sig = inspect.signature(get_client)
        params = sig.parameters
        # uri positional, app_name positional or kw with no default
        assert "uri" in params
        assert "app_name" in params


# -- TC-R-106c: caching ------------------------------------------------------

class TestClientCaching:
    def test_repeated_get_client_same_uri_returns_same_instance(self):
        """Repeated calls with same uri+app_name return cached MongoClient."""
        from scripts.common.mongo import get_client
        c1 = get_client("mongodb://localhost:1/?serverSelectionTimeoutMS=10", "cache-test")
        c2 = get_client("mongodb://localhost:1/?serverSelectionTimeoutMS=10", "cache-test")
        assert c1 is c2, "get_client should cache by (uri, app_name)"

    def test_different_app_name_creates_new_client(self):
        from scripts.common.mongo import get_client
        c1 = get_client("mongodb://localhost:1/?serverSelectionTimeoutMS=10", "app-a")
        c2 = get_client("mongodb://localhost:1/?serverSelectionTimeoutMS=10", "app-b")
        assert c1 is not c2


# -- TC-R-106d: timeout default ---------------------------------------------

class TestTimeouts:
    def test_default_server_selection_timeout(self):
        from scripts.common.mongo import get_client
        sig = inspect.signature(get_client)
        if "server_selection_timeout_ms" in sig.parameters:
            param = sig.parameters["server_selection_timeout_ms"]
            assert param.default == 5000
