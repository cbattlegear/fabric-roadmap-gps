"""Public privacy copy and its footer-only discovery."""

from unittest.mock import patch

from bs4 import BeautifulSoup

import server


def test_privacy_page_preserves_feedback_notice_without_database_access():
    with patch.object(server, "get_engine", side_effect=AssertionError("No database needed")):
        response = server.app.test_client().get("/privacy")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    assert soup.h1.get_text(strip=True) == "Article Feedback & Privacy"
    content = soup.select_one(".privacy-notice").get_text(" ", strip=True)
    for text in (
        "local storage for up to 180 days", "store its hash",
        "your choice, and timestamps", "not use your IP address, email, subscription token",
        "180 days without a changed vote", "hourly refresh job",
        "backups follow the database backup retention policy",
        "not verified people counts", "does not delete previously submitted feedback",
        "operational logging and IP-based abuse limits", "does not set a cookie",
        "No public voter list or score leaderboard",
    ):
        assert text in content


def test_about_removes_privacy_notice_and_keeps_release_notes_in_shared_white_panel():
    with patch.object(server, "release_notes_context", return_value={
        "available": False, "url": "https://github.com/cbattlegear/fabric-roadmap-gps/releases",
    }):
        response = server.app.test_client().get("/about")
    soup = BeautifulSoup(response.data, "html.parser")
    assert soup.select_one("#blog-feedback-privacy") is None
    assert "180 days" not in soup.main.get_text()
    assert not soup.main.select('a[href="/privacy"]')
    notes = soup.select_one(".app-release-notes")
    assert notes is not None
    assert soup.select_one(".coming-soon-section").find_next_sibling() == notes
    css = soup.style.get_text()
    assert ".coming-soon-section,.app-release-notes {background:#fff;border:1px solid #e1e5e9;" in css
    assert "Release notes are temporarily unavailable" in notes.get_text()


def test_privacy_navigation_is_footer_only_and_follows_email_notifications():
    for route in ("/", "/about", "/changelog", "/privacy"):
        soup = BeautifulSoup(server.app.test_client().get(route).data, "html.parser")
        links = soup.select('a[href="/privacy"]')
        assert len(links) == 1
        assert links[0].find_parent("footer") is not None
        assert [link["href"] for link in soup.footer.select("a")] == [
            "/rss", "/endpoints", "/subscribe", "/privacy",
        ]
        assert not soup.header.select('a[href="/privacy"]')
