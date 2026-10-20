"""Section-aware navigation without database access or layout-changing styles."""

import re

from bs4 import BeautifulSoup
import pytest

import server


@pytest.mark.parametrize("path, destination, current", [
    ("/", "/", "page"),
    ("/?product_name=Power%20BI", "/", "page"),
    ("/release/example", "/", "location"),
    ("/changelog", "/changelog", "page"),
    ("/changelog/2026-10-19", "/changelog", "location"),
    ("/insights?metric=target", "/insights", "page"),
    ("/endpoints", "/endpoints", "page"),
    ("/endpoints#mcp", "/endpoints", "page"),
    ("/subscribe", "/subscribe", "page"),
    ("/verify-email?token=test", "/subscribe", "location"),
    ("/unsubscribe?token=test", "/subscribe", "location"),
    ("/preferences?token=test", "/subscribe", "location"),
    ("/about", "/about", "page"),
])
def test_navigation_identifies_exact_pages_and_nested_sections(path, destination, current):
    with server.app.test_request_context(path):
        soup = BeautifulSoup(server.render_template("base.html"), "html.parser")
    links = soup.select(".header-nav a")
    assert [link["href"] for link in links] == [
        "/", "/changelog", "/insights", "/endpoints", "/subscribe", "/about",
    ]
    active = soup.select(".header-nav [aria-current]")
    assert len(active) == 1
    assert active[0]["href"] == destination
    assert active[0]["aria-current"] == current
    assert "active" in active[0]["class"]
    assert all("active" not in link["class"] for link in links if link is not active[0])


@pytest.mark.parametrize("path", [
    "/privacy", "/api/releases", "/api/insights", "/mcp", "/unknown",
    "/changelogger", "/release-notes",
])
def test_unrepresented_routes_do_not_default_to_home(path):
    with server.app.test_request_context(path):
        soup = BeautifulSoup(server.render_template("base.html"), "html.parser")
    assert not soup.select(".header-nav [aria-current], .header-nav .active")


def test_active_navigation_uses_aria_and_keeps_focus_and_layout_stable():
    css = server.app.test_client().get("/static/styles.css").get_data(as_text=True)
    rule = re.search(r"\.header-nav \.nav-link\[aria-current\]\s*\{([^}]+)\}", css)
    assert rule is not None
    assert "text-decoration-line: underline" in rule.group(1)
    assert "text-decoration-thickness: 2px" in rule.group(1)
    assert "text-underline-offset:" in rule.group(1)
    assert not any(property_name in rule.group(1) for property_name in (
        "font-weight", "font-size", "padding", "border", "outline",
    ))
    assert "a:focus-visible" in css
