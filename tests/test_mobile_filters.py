"""Progressively enhanced mobile controls; no database/browser service required."""

from pathlib import Path
import shutil
import subprocess

from bs4 import BeautifulSoup

import server


def test_mobile_filter_markers_keep_unique_live_controls_and_script_order():
    page = BeautifulSoup(server.app.test_client().get("/").data, "html.parser")
    filters = page.select_one("[data-mobile-filters]")
    assert filters is not None
    assert [group.select_one("input, select")["id"] for group in filters.select("[data-secondary-filter]")] == [
        "type-filter", "status-filter", "days-filter", "sort-select", "include-inactive",
    ]
    for field in ["product-filter", "search-input", "type-filter", "status-filter",
                  "days-filter", "sort-select", "include-inactive"]:
        assert len(page.select(f"#{field}")) == 1
    assert filters.select_one("#views-toggle")
    assert filters.select_one("#clear-filters")
    scripts = [script["src"].split("?")[0] for script in page.select("script[src]")]
    assert scripts.index("/static/mobile-filters.js") < scripts.index("/static/app.js")
    assert page.select_one('script[src^="/static/mobile-filters.js"][defer]')


def test_mobile_target_rules_preserve_desktop_and_native_focus():
    root = Path(__file__).resolve().parents[1] / "static"
    css = (root / "styles.css").read_text()
    mobile = css[css.index("@media (max-width: 768px)", css.index(".header-nav .nav-link[aria-current]")):]
    for selector in [".header-nav .nav-link", ".roadmap-results .filter-select",
                     ".mobile-filter-details > summary"]:
        assert selector in mobile
    assert mobile.count("min-height: 44px") >= 4
    assert "summary:focus-visible" in mobile
    assert "grid-template-columns: minmax(0, 1fr)" in mobile
    for asset, selector in [("blog-feedback.css", ".change-article-link"),
                            ("insights.css", ".insights-data > summary")]:
        source = (root / asset).read_text()
        assert selector in source
        assert "min-height: 44px" in source[source.rindex("@media"):]


def test_mobile_disclosure_javascript_behavior():
    node = shutil.which("node")
    assert node, "Install Node.js 22+ to run frontend behavior tests."
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, "--test", str(root / "tests" / "mobile_filters.test.cjs")],
        cwd=root, text=True, capture_output=True, timeout=60, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_home_retry_and_mobile_popover_are_scoped_and_accessible():
    page = BeautifulSoup(server.app.test_client().get("/").data, "html.parser")
    error = page.select_one(".roadmap-results #load-error[hidden]")
    assert error["role"] == "alert"
    assert error.select_one("#load-error-message")
    assert error.select_one('button#retry-load[type="button"]').get_text(strip=True) == "Retry"
    assert page.select_one("#no-results")["tabindex"] == "-1"
    css = (Path(__file__).resolve().parents[1] / "static" / "styles.css").read_text()
    rule = css.split(".roadmap-results .saved-views {", 1)[1].split("}", 1)[0]
    assert "inset: 20vh 16px auto" in rule
    assert "position-area: none" in rule
    assert "position-anchor: auto" in rule
    assert css.index(".roadmap-results .saved-views {") > css.rindex("@media (max-width: 768px)")
    assert "overflow-wrap: anywhere" in css.split(".mobile-filter-applied {", 1)[1].split("}", 1)[0]
