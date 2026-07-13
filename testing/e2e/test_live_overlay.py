"""Tests for the live overlay (no-build HTML/JS) in scripts/dashboard.py.

The overlay is a self-contained HTML+CSS+JS island embedded via
st.components.v1.html that opens an EventSource to the SSE sidecar and renders
the ops ticker, surge/dispatch banners, and the LIVE/RECONNECTING/OFFLINE
indicator. These tests assert the wiring is present and correct; the live
browser behavior (boundary B6) is exercised via playwright-cli as a demo.

Traceability: TC-E-010..014, TC-E-020, TC-E-021, TC-REG-001.
"""

from __future__ import annotations

import importlib

dashboard = importlib.import_module("scripts.dashboard")


def _html(sse_url="http://localhost:8502"):
    return dashboard._render_live_overlay(sse_url)


# --- TC-E-021: configurable SSE URL wired into an EventSource -------------


def test_overlay_opens_eventsource_to_configured_url():
    html = _html("http://example.test:9999")
    assert "EventSource(" in html
    assert "http://example.test:9999/api/stream" in html


def test_overlay_url_defaults_and_env_override(monkeypatch):
    # REQ-E-021: default is localhost:8502 (independent of ambient env — a
    # prior deploy-launch test may have set LIVE_SSE_URL in os.environ).
    monkeypatch.delenv("LIVE_SSE_URL", raising=False)
    default_url = dashboard._live_sse_url()
    assert default_url == "http://localhost:8502"
    monkeypatch.setenv("LIVE_SSE_URL", "http://host.internal:8600")
    assert dashboard._live_sse_url() == "http://host.internal:8600"


# --- TC-E-014: MongoDB theme tokens ---------------------------------------


def test_overlay_uses_mongodb_theme_tokens():
    html = _html()
    assert "#00ED64" in html  # MongoDB Spring Green
    # ticker + banner + indicator containers exist
    for anchor in ("live-ticker", "live-banner", "live-indicator"):
        assert anchor in html


# --- TC-E-010: ticker routing for dispatch/anomaly ------------------------


def test_overlay_routes_watched_collections_to_ticker_and_counters():
    html = _html()
    # the JS must reference the watched collections it reacts to
    assert "fleet.dispatch_log" in html
    assert "analytics.zone_anomalies" in html
    # counters incremented in JS
    assert "count" in html.lower()


# --- TC-E-011/012: surge + dispatch banners -------------------------------


def test_overlay_defines_surge_and_dispatch_banners():
    html = _html()
    assert "SURGE DETECTED" in html
    assert "AGENT DISPATCHING" in html


# --- TC-E-013 / TC-E-020: connection state incl. OFFLINE ------------------


def test_overlay_defines_live_reconnecting_and_offline_states():
    html = _html()
    assert "LIVE" in html
    assert "RECONNECTING" in html
    assert "OFFLINE" in html
    # onerror handler present so a down sidecar flips to OFFLINE (REQ-E-020)
    assert "onerror" in html


def test_overlay_pulses_between_windows_on_keepalive_ping():
    """The pipeline is windowed: nothing lands in Mongo between window closes,
    so the overlay must show liveness via the SSE keepalive `ping` (no new data
    source). Assert the ping listener, the CSS breathe animation, and the idle
    'listening' hint are all present."""
    html = _html()
    # Listens for the sidecar's keepalive ping event.
    assert "addEventListener('ping'" in html
    # Green dot gently breathes while LIVE (pure CSS).
    assert "@keyframes livepulse" in html
    assert "animation:livepulse" in html
    # Idle hint shown between real change events.
    assert "listening for surges" in html


# --- REQ-NF-SEC: untrusted doc fields must not reach innerHTML (DOM-XSS) --


def test_overlay_renders_dynamic_fields_via_textnode_not_innerhtml():
    """Kafka/Mongo-sourced fields (zone, coll, op) flow into the ticker/banner.
    They MUST be inserted as text nodes / textContent, never concatenated into
    an innerHTML string, or a crafted zone value could inject markup/script."""
    html = _html()
    # The dynamic-row builder uses createTextNode + textContent, not innerHTML
    # string concatenation with the untrusted values.
    assert "createTextNode" in html
    assert "b.textContent=coll" in html
    assert "d2.textContent=text" in html
    # Guard against regressing to the vulnerable concatenation forms.
    assert "row.innerHTML='✓ '+op" not in html
    assert "'<div class=\"b '+kind+'\">'+text" not in html


# --- TC-REG-001: overlay renders without a live sidecar, dashboard intact --


def test_overlay_render_is_pure_string_and_never_raises():
    """INV-001: building the overlay is a pure string op — it does not connect
    to the sidecar at render time, so the dashboard renders even if the sidecar
    is down (the browser reconnects/flips to OFFLINE client-side)."""
    html = dashboard._render_live_overlay("http://127.0.0.1:1")  # unreachable
    assert isinstance(html, str) and len(html) > 200
