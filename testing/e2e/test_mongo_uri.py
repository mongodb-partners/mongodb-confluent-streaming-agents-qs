"""Tests for the shared MongoDB URI resolver (scripts/common/mongo_uri.py).

Covers the spec's INV-005 (URI resolution chain unchanged) and INV-003
(single MongoClient factory / mock-parity with the dashboard's prior behavior).

Traceability: TC-REG-005 (resolution chain), TC-REG-003 (parity with dashboard).
"""
from __future__ import annotations

import importlib

import pytest

mongo_uri = importlib.import_module("scripts.common.mongo_uri")


# --- TC-REG-005: resolution chain (.env -> tfvars -> MONGODB_URI) ---------

def test_resolves_from_env_embedded_credentials(tmp_path):
    """WHEN .env has a full URI with embedded creds THE resolver SHALL return it."""
    (tmp_path / ".env").write_text(
        'TF_VAR_mongodb_connection_string=mongodb+srv://u:p@host.mongodb.net/db\n'
    )
    uri = mongo_uri.resolve_mongodb_uri(project_root=tmp_path)
    assert uri == "mongodb+srv://u:p@host.mongodb.net/db"


def test_resolves_from_env_builds_uri_from_parts(tmp_path):
    """WHEN .env has host + separate user/pw THE resolver SHALL build a URI."""
    (tmp_path / ".env").write_text(
        "TF_VAR_mongodb_connection_string=mongodb+srv://host.mongodb.net/db\n"
        "TF_VAR_mongodb_username=alice\n"
        "TF_VAR_mongodb_password=s3cret\n"
    )
    uri = mongo_uri.resolve_mongodb_uri(project_root=tmp_path)
    assert "alice:s3cret@host.mongodb.net" in uri


def test_falls_back_to_tfvars(tmp_path):
    """WHEN no .env THE resolver SHALL read terraform/agents/terraform.tfvars."""
    tfdir = tmp_path / "terraform" / "agents"
    tfdir.mkdir(parents=True)
    (tfdir / "terraform.tfvars").write_text(
        'mongodb_connection_string = "mongodb+srv://host.mongodb.net/db"\n'
        'mongodb_username = "bob"\n'
        'mongodb_password = "pw"\n'
    )
    uri = mongo_uri.resolve_mongodb_uri(project_root=tmp_path)
    assert uri is not None and "bob:pw@host.mongodb.net" in uri


def test_falls_back_to_env_var(tmp_path, monkeypatch):
    """WHEN no .env and no tfvars THE resolver SHALL read $MONGODB_URI."""
    monkeypatch.setenv("MONGODB_URI", "mongodb://localhost:27017")
    uri = mongo_uri.resolve_mongodb_uri(project_root=tmp_path)
    assert uri == "mongodb://localhost:27017"


def test_returns_none_when_nothing_configured(tmp_path, monkeypatch):
    """IF no source resolves THEN the resolver SHALL return None."""
    monkeypatch.delenv("MONGODB_URI", raising=False)
    assert mongo_uri.resolve_mongodb_uri(project_root=tmp_path) is None


# --- TC-REG-003: parity — dashboard delegates to the shared resolver ------

def test_dashboard_delegates_to_shared_resolver(tmp_path, monkeypatch):
    """INV-003/INV-005: the dashboard's _resolve_mongodb_uri SHALL produce the
    same result as the shared resolver for the same inputs (no behavioral drift).
    """
    dashboard = importlib.import_module("scripts.dashboard")
    (tmp_path / ".env").write_text(
        'TF_VAR_mongodb_connection_string=mongodb+srv://u:p@host.mongodb.net/db\n'
    )
    shared = mongo_uri.resolve_mongodb_uri(project_root=tmp_path)
    via_dashboard = dashboard._resolve_mongodb_uri(project_root=tmp_path)
    assert shared == via_dashboard == "mongodb+srv://u:p@host.mongodb.net/db"
