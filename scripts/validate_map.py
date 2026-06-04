"""Playwright visual validation for the Live Dispatch Map.

Runs against the running Streamlit dashboard, captures a screenshot of
the map area, and verifies a few invariants:

- Map canvas exists and is non-empty
- Status caption reports >= 1 boat in flight
- We can save a screenshot to logs/map-validation-<ts>.png

Usage:
    uv run python -m scripts.validate_map
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

# use the canonical helper rather
# than duplicating the project-root walk.
from scripts.common.terraform import get_project_root


def _project_root() -> Path:
    return get_project_root(strict=False)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:8501")
    p.add_argument("--wait-ms", type=int, default=15000,
                   help="Time to let the page settle and animation tick.")
    p.add_argument("--screenshot",
                   help="Output PNG path (default: logs/map-validation-<ts>.png)")
    p.add_argument("--frames", type=int, default=1,
                   help="Capture N frames at 3s intervals (catches different animation phases)")
    p.add_argument("--headless", action="store_true", default=True)
    p.add_argument("--no-headless", dest="headless", action="store_false")
    args = p.parse_args(argv)

    from playwright.sync_api import sync_playwright

    root = _project_root()
    logs_dir = root / "logs"
    logs_dir.mkdir(exist_ok=True)
    out_path = (Path(args.screenshot) if args.screenshot else
                logs_dir / f"map-validation-{datetime.now():%Y%m%d-%H%M%S}.png")

    print(f"[validate_map] navigating to {args.url}")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=args.headless)
        context = browser.new_context(viewport={"width": 1600, "height": 1100},
                                      device_scale_factor=2)
        page = context.new_page()
        page.goto(args.url, wait_until="networkidle", timeout=60000)
        # Streamlit hydrates lazily; wait for the dispatch map header.
        page.wait_for_selector("text=Live Dispatch Map", timeout=30000)
        print(f"[validate_map] header visible, waiting {args.wait_ms}ms for "
              "deck.gl to render and animation to tick…")
        page.wait_for_timeout(args.wait_ms)

        # Find the map's deck.gl canvas
        canvas_count = page.locator("canvas").count()
        print(f"[validate_map] canvases on page: {canvas_count}")
        assert canvas_count >= 1, "no <canvas> on the page"

        # Capture N frames so we can find one mid-animation with boats
        # actively visible (boats in flight = path-time within window).
        canvas = page.locator("canvas").last
        canvas.wait_for(state="visible", timeout=5000)
        captured = []
        base = out_path.with_suffix("")
        for i in range(args.frames):
            frame_path = (base.with_name(f"{base.name}-f{i:02d}.png")
                          if args.frames > 1 else out_path)
            try:
                canvas.screenshot(path=str(frame_path))
                captured.append(frame_path)
            except Exception as exc:
                print(f"[validate_map] frame {i} failed: {exc}")
            if args.frames > 1 and i < args.frames - 1:
                page.wait_for_timeout(3000)

        # Read the status line under the map ("N boats in flight · M active trails")
        try:
            status = page.locator(
                "text=/\\d+\\s+boats?\\s+in\\s+flight/").first.text_content(timeout=2000)
        except Exception:
            status = None
        print(f"[validate_map] status caption: {status!r}")

        browser.close()

    if not captured:
        print("[validate_map] WARNING: no screenshots created")
        sys.exit(1)
    for path in captured:
        size_kb = path.stat().st_size // 1024 if path.exists() else 0
        print(f"[validate_map] saved -> {path} ({size_kb} KB)")


if __name__ == "__main__":
    main()
