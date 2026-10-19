"""Shared, read-only rendering for cards, release pages, and email."""

from bs4 import BeautifulSoup
import pytest

from lib.description_format import render_description_html, render_description_text



@pytest.fixture(scope="module")
def rendered():
    descriptions = [
        None,
        "First paragraph.\n\nSecond paragraph.",
        "First line.\nSecond line.",
        "### First heading\n\nText\n\n### Second heading\n\nMore text",
        "- First item\n- Second item\n\n1. One\n2. Two",
        "Use [CREATE VECTOR INDEX](https://learn.microsoft.com/sql) and `VECTOR SEARCH`.",
        "<h2>Generally Available: Vector Search</h2><p>Use "
        '<a href="https://learn.microsoft.com/sql">CREATE VECTOR INDEX</a>.</p>',
        "## Generally Available: Vector Search\n\nVector indexes are now available.",
        "C# and issue #123 are not headings. A & B.",
        "```\nline one\nline two\n```",
    ]
    return [BeautifulSoup(render_description_html(text), "html.parser") for text in descriptions]


def test_empty_description_has_fallback(rendered):
    assert rendered[0].get_text(strip=True) == "No description available."


def test_paragraphs_and_soft_breaks_are_distinct(rendered):
    assert len(rendered[1].find_all("p")) == 2
    assert not rendered[1].find_all("br")
    assert len(rendered[2].find_all("p")) == 1
    assert len(rendered[2].find_all("br")) == 1


def test_all_markdown_heading_markers_are_parsed(rendered):
    assert [h.get_text() for h in rendered[3].find_all("h3")] == [
        "First heading", "Second heading",
    ]
    assert "###" not in rendered[3].get_text()
    assert rendered[7].h2.get_text() == "Generally Available: Vector Search"


def test_lists_keep_their_structure(rendered):
    assert len(rendered[4].ul.find_all("li")) == 2
    assert len(rendered[4].ol.find_all("li")) == 2
    assert not rendered[4].find_all("br")


def test_markdown_links_and_code_are_preserved(rendered):
    assert rendered[5].a["href"] == "https://learn.microsoft.com/sql"
    assert rendered[5].a.get_text() == "CREATE VECTOR INDEX"
    assert rendered[5].code.get_text() == "VECTOR SEARCH"
    assert rendered[9].pre.code.get_text() == "line one\nline two\n"


def test_existing_html_descriptions_are_preserved(rendered):
    assert rendered[6].h2.get_text() == "Generally Available: Vector Search"
    assert rendered[6].p.a["href"] == "https://learn.microsoft.com/sql"


def test_hashes_and_ampersands_in_prose_are_preserved(rendered):
    assert rendered[8].p.get_text() == "C# and issue #123 are not headings. A & B."


@pytest.mark.parametrize("source", [None, "", "   "])
def test_empty_input_is_consistent_in_html_and_text(source):
    assert "No description available." in render_description_html(source)
    assert render_description_text(source) == "No description available."


def test_vector_style_flattened_heading_does_not_enlarge_whole_description():
    source = "# Vector Search " + "Search is generally available. " * 8
    source += "Database.By bringing search into SQL.**Learn more:** [Docs](https://example.com/sql)"
    html = BeautifulSoup(render_description_html(source), "html.parser")
    assert html.h1 is None
    assert len(html.find_all("p")) == 3
    assert "Database.\n\nBy" in render_description_text(source)
    assert html.a["href"] == "https://example.com/sql"
    assert "Docs (https://example.com/sql)" in render_description_text(source)


def test_copilot_style_fused_headings_and_lists():
    source = (
        "# Copilot overview.## One Place for Trusted DataThis integration keeps context."
        "## Administrator ControlsA new setting is off by default."
        "Choose an experience:- If both are enabled, show both."
        "- If only one is enabled, show one."
        "## Initial AvailabilityDuring preview, a license is required."
        "Context includes:- Data through **Fabric IQ**- Work through **Work IQ**"
        "Existing permissions still apply."
    )
    html = BeautifulSoup(render_description_html(source), "html.parser")
    assert [h.get_text() for h in html.find_all("h2")] == [
        "One Place for Trusted Data", "Administrator Controls", "Initial Availability",
    ]
    assert len(html.find_all("li")) == 4
    assert len(html.find_all("ul")) == 2
    assert "##" not in html.get_text()
    assert "**" not in html.get_text()
    assert html.find_all("p")[-1].get_text() == "Existing permissions still apply."


def test_tsql_style_encoded_breaks_and_fused_asterisk_bullets():
    source = (
        "Query improvements.&lt;br/&gt;* **Analytics** Use `MEDIAN` for analysis."
        "* **Syntax** Use `QUALIFY` for simpler queries."
        "* **Productivity** Write less code."
    )
    html = BeautifulSoup(render_description_html(source), "html.parser")
    assert len(html.find_all("li")) == 3
    assert "&lt;br" not in str(html)
    assert html.code.get_text() == "MEDIAN"


def test_deployment_style_colon_bullets_and_fused_continuations():
    source = (
        "**Deployment plan** defines item order.Use the canvas to:"
        "- Group items into deployment groups- Add pre-deploy actions."
        "The result follows the intended order."
    )
    html = BeautifulSoup(render_description_html(source), "html.parser")
    assert [li.get_text() for li in html.find_all("li")] == [
        "Group items into deployment groups", "Add pre-deploy actions.",
    ]
    assert html.find_all("p")[-1].get_text() == "The result follows the intended order."


def test_inline_code_and_links_do_not_break_list_repair_context():
    source = "Options:- Use `SQL` with defaults- Add [Docs](https://example.com) links- Enable previews"
    html = BeautifulSoup(render_description_html(source), "html.parser")
    assert len(html.find_all("li")) == 3
    assert html.code.get_text() == "SQL"
    assert html.a["href"] == "https://example.com"


def test_eventstreams_style_spaced_headings_and_bullets():
    source = (
        "### Large Messages  #### Overview  Process larger events."
        "  #### What's changing  Key aspects:  - **Opt-in** -- existing streams are unchanged."
        " - **Ceiling** -- choose a limit. - **No redesign** -- keep existing pipelines."
    )
    html = BeautifulSoup(render_description_html(source), "html.parser")
    assert [h.get_text() for h in html.find_all(["h3", "h4"])] == [
        "Large Messages", "Overview", "What's changing",
    ]
    assert len(html.find_all("li")) == 3
    assert "-- existing streams" in html.get_text()


@pytest.mark.parametrize("source", [
    "`value.## Keep **literal**`",
    "```\nvalue.## Keep **literal**\n```\n",
    "~~~sql\nvalue.## Keep **literal**\n~~~\n",
    "<pre><code>value.## Keep **literal**</code></pre>",
    "    value.## Keep **literal**\n",
    "``value.## Keep `literal` **literal**``",
])
def test_code_is_not_repaired_as_prose(source):
    html = BeautifulSoup(render_description_html(source), "html.parser")
    expected = "value.## Keep `literal` **literal**" if source.startswith("``value") else "value.## Keep **literal**"
    assert html.code.get_text().strip() == expected


def test_identifiers_product_names_and_url_fragments_are_not_repaired():
    source = (
        "Use OneLake, DataFrame, T-SQL, System.Text.Json, and schema.Table. "
        "[Docs](https://example.com/schema.Table?x=1&y=2#section) "
        "https://example.com/test.This#section"
    )
    text = render_description_text(source)
    assert "OneLake, DataFrame, T-SQL, System.Text.Json, and schema.Table." in text
    assert "https://example.com/schema.Table?x=1&y=2#section" in text
    assert "https://example.com/test.This#section" in text


@pytest.mark.parametrize("source", [
    '<script>alert("bad")</script><p>Safe</p>',
    '<img src="x" onerror="alert(1)" alt="Safe">',
    '<svg><a href="javascript:alert(1)">bad</a></svg><p>Safe</p>',
    '<p style="font-size:99px" onclick="alert(1)">Safe</p>',
    '<iframe src="https://example.com"></iframe><p>Safe</p>',
])
def test_untrusted_html_is_sanitized(source):
    html = BeautifulSoup(render_description_html(source), "html.parser")
    assert not html.find_all(["script", "img", "svg", "iframe"])
    assert all(not tag.attrs for tag in html.find_all(True))
    assert html.get_text(strip=True) == "Safe"


@pytest.mark.parametrize("word", ["Existing", "This", "These"])
@pytest.mark.parametrize("prefix", ["", "Use ", "Settings:", "Use"])
def test_valid_bold_phrases_are_not_split(word, prefix):
    source = f"{prefix}**{word} permissions** to restrict access."
    html = BeautifulSoup(render_description_html(source), "html.parser")
    assert len(html.find_all("p")) == 1
    assert html.strong.get_text() == f"{word} permissions"
    assert "**" not in html.get_text()
    assert render_description_text(source) == f"{prefix}{word} permissions to restrict access."


@pytest.mark.parametrize("source,expected", [
    ("Use &lt;string&gt; and &lt;int&gt; parameters.", "Use <string> and <int> parameters."),
    ("The literal &lt;script&gt; tag is not supported.", "The literal <script> tag is not supported."),
    ("Literal &lt;script&gt;bad&lt;/script&gt; text.", "Literal <script>bad</script> text."),
    ("Write &lt;p&gt; for a paragraph.", "Write <p> for a paragraph."),
])
def test_escaped_angle_brackets_remain_literal_text(source, expected):
    for email in (False, True):
        html = BeautifulSoup(render_description_html(source, email=email), "html.parser")
        assert html.get_text() == expected
        assert html.script is None
        assert html.string is None or html.string.parent.name == "p"
    assert render_description_text(source) == expected


@pytest.mark.parametrize("href", [
    "javascript:alert(1)", "jav&#x09;ascript:alert(1)",
    "data:text/html,bad", "vbscript:bad", "file:///C:/private",
    "http://[bad",
])
def test_unsafe_or_malformed_links_have_no_href(href):
    html = BeautifulSoup(render_description_html(f'<a href="{href}">Link</a>'), "html.parser")
    assert html.a.get("href") is None


def test_email_uses_controlled_inline_styles_only():
    html = BeautifulSoup(
        render_description_html('# Heading\n\nText [Docs](https://example.com)\n\n- One', email=True),
        "html.parser",
    )
    assert "font-size:14px" in html.h1["style"]
    assert "font-size:14px" in html.p["style"]
    assert "color:#19433c" in html.a["style"]
    assert "padding-left:20px" in html.ul["style"]


def test_plain_text_has_lists_and_links_without_markdown_or_html():
    text = render_description_text("## Heading\n\nUse **bold** and [Docs](https://example.com).\n\n- First\n- Second")
    assert text == "Heading\n\nUse bold and Docs (https://example.com).\n\n- First\n- Second"


def test_plain_text_keeps_ordered_list_numbering():
    assert render_description_text("3. First step\n4. Next step") == "3. First step\n4. Next step"


def test_html_comments_are_not_forwarded_to_pages_or_email():
    source = "<!--[if mso]><img src=x onerror=bad><![endif]--><p>Safe</p>"
    assert render_description_html(source) == "<p>Safe</p>"
    assert "<!--" not in render_description_html(source, email=True)
