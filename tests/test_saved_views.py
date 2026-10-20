"""Rendered controls and dependency-free JavaScript behavior checks."""

from pathlib import Path
import shutil
import subprocess

from bs4 import BeautifulSoup
import pytest

import server


@pytest.mark.parametrize("path", ["/", "/changelog"])
def test_saved_view_controls_render_and_load_shared_helper_first(path):
    response = server.app.test_client().get(
        path + "?product_name=Data+Engineering&q=%E6%95%B0%E6%8D%AE%26%23&token=private"
    )
    assert response.status_code == 200
    soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")
    for control in [
        "saved-view-select", "view-name", "save-view", "rename-view",
        "delete-view", "copy-view-link", "copy-view-url", "include-inactive",
    ]:
        assert len(soup.select(f"#{control}")) == 1
    assert soup.select_one("#view-name")["maxlength"] == "80"
    assert soup.select_one("#include-inactive").has_attr("checked")
    assert soup.select_one("#copy-view-url").has_attr("readonly")
    assert soup.select_one("#view-error")["role"] == "alert"
    assert soup.select_one("#view-status")["role"] == "status"
    assert soup.select_one("#view-help") is None
    assert "Names and saved views stay in this browser" not in soup.get_text()
    panel = soup.select_one("#saved-views-panel")
    assert panel["popover"] == "auto"
    assert panel.get("role") != "menu"
    assert soup.select_one("#views-toggle")["popovertarget"] == panel["id"]
    assert soup.select_one(".filter-actions #clear-filters") is not None
    assert soup.select_one(".filter-actions #views-toggle") is not None
    assert panel.select_one("#save-view")
    sources = [script.get("src", "") for script in soup.select("script[src]")]
    helper = next(i for i, src in enumerate(sources) if "/saved-views.js" in src)
    page = "app.js" if path == "/" else "changelog.js"
    assert helper < next(i for i, src in enumerate(sources) if f"/{page}" in src)
    assert "private" not in soup.select_one(".saved-views").get_text()


def test_compact_layout_removes_home_label_and_shared_header_gutter():
    soup = BeautifulSoup(server.app.test_client().get("/").data, "html.parser")
    section = soup.select_one(".recent-changes-section")
    assert section["aria-label"] == "Recent roadmap changes"
    assert section.select_one("h2") is None
    css = (Path(__file__).resolve().parents[1] / "static" / "styles.css").read_text()
    import re
    assert "margin-bottom: 0;" in re.search(r"\.header\s*\{([^}]+)", css).group(1)
    assert "margin-top: 0;" in re.search(r"\.recent-changes-content\s*\{([^}]+)", css).group(1)


def test_saved_view_javascript_behavior():
    node = shutil.which("node")
    assert node, "Install Node.js 22+ to run the frontend behavior tests."
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, "--test", str(root / "tests" / "saved_views.test.cjs")],
        cwd=root,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
