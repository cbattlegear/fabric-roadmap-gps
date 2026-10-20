"""Read-only presentation of roadmap descriptions, including flattened Markdown.

Repairs use explicit markup and conservative prose boundaries, never product-
specific replacements. Ambiguous missing word boundaries are left untouched.
"""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlsplit

from bs4 import BeautifulSoup, Comment
from markdown_it import MarkdownIt


_FALLBACK = "No description available."
_MARKDOWN = MarkdownIt("commonmark", {"html": True, "breaks": True})
_PROTECTED = re.compile(
    r"(^ {0,3}(?P<fence>`{3,}|~{3,})[^\n]*\n.*?^ {0,3}(?P=fence)[ \t]*$"
    r"|(?P<ticks>`+)(?:(?!(?P=ticks)).)*?(?P=ticks)"
    r"|(?:^ {4}[^\n]*(?:\n|$))+"
    r"|<(?P<code_tag>pre|code)\b[^>]*>.*?</(?P=code_tag)>"
    r"|\[[^\]\n]*\]\((?:[^()\n]|\([^()\n]*\))*\)"
    r"|<https?://[^>\n]+>|https?://[^\s<>]+)",
    re.MULTILINE | re.DOTALL,
)
_ALLOWED_TAGS = {
    "p", "br", "strong", "b", "em", "i", "s", "code", "pre", "a",
    "ul", "ol", "li", "blockquote", "h1", "h2", "h3", "h4", "h5", "h6",
    "hr", "table", "thead", "tbody", "tr", "th", "td",
}
_DROP_TAGS = {
    "script", "style", "iframe", "object", "embed", "svg", "math",
    "form", "input", "button", "textarea", "select", "link", "meta",
}
_EMAIL_STYLES = {
    "p": "margin:0 0 8px 0;font-size:14px;line-height:1.5;",
    "a": "color:#19433c;text-decoration:underline;overflow-wrap:break-word;",
    "ul": "margin:0 0 8px 0;padding-left:20px;",
    "ol": "margin:0 0 8px 0;padding-left:20px;",
    "pre": "margin:0 0 8px 0;white-space:pre-wrap;overflow-wrap:break-word;",
    "blockquote": "margin:0 0 8px 16px;",
    "table": "width:100%;border-collapse:collapse;",
}


def _repair_prose(text: str) -> str:
    text = re.sub(r"&lt;br\s*/?\s*&gt;", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</?(?:p|div)\b[^>]*>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?<!\*)(\*\*[^*\n]+\*\*)(?=\*\*[^*\n]+\*\*(?!\*))",
        r"\1\n\n", text,
    )

    # Flattened exports glue headings onto the preceding sentence or heading.
    text = re.sub(r"(?<=\S)[ \t]*(?<!#)(?=#{2,6}[ \t]+)", "\n\n", text)
    text = re.sub(
        r"(?<=[.!?])(?=(?:By|Our|For|They|The|This|These|During|Existing|Use|"
        r"Built|Learn|No|If|Administrators)\b|\*\*)",
        "\n\n", text,
    )
    text = re.sub(
        r"(?<=[a-z])(?=(?:This|These|During|Existing|Our|A new)\b)",
        "\n\n", text,
    )
    text = re.sub(
        r"(\*\*(?=\S)(?:(?!\*\*|\n).)*?(?<=\S)\*\*)(?=(?:Existing|This|These)\b)",
        r"\1\n\n", text,
    )

    text = re.sub(r"(?<=[.:;])[ \t]*(?=[*-][ \t]+)", "\n", text)
    text = re.sub(r"(?<=\S)[ \t]{2,}(?=-[ \t]+)", "\n", text)
    text = re.sub(r"(?<=\*\*)(?=-[ \t]+)", "\n", text)
    text = re.sub(
        r"^(\*\*(?:Overview|Key Capabilities)\*\*)(?=\S)",
        r"\1\n\n", text, flags=re.MULTILINE,
    )

    lines = []
    for line in text.split("\n"):
        if re.match(r"^#{1,6}[ \t]+", line):
            line = re.sub(r"[ \t]{2,}(?=\S)", "\n\n", line)
        if re.match(r"^[ \t]*[-*][ \t]+", line):
            line = re.sub(
                r"(?<=\S)[ \t]*(?=(?<![-*])[-*](?![-*])[ \t]+(?:\*\*|[A-Z]))",
                "\n", line,
            )
        lines.append(line)
    return "\n".join(lines)


def _normalize(text: str) -> str:
    prefix = "\x00description"
    while prefix in text:
        prefix += "_"
    protected = []

    def protect(match):
        token = f"{prefix}{len(protected)}\x00"
        protected.append((token, match.group()))
        return token

    normalized = _repair_prose(_PROTECTED.sub(protect, text))
    for token, original in protected:
        normalized = normalized.replace(token, original)
    return normalized


def _safe_href(href: str) -> bool:
    # Browsers ignore control characters in schemes; do not let them disguise one.
    href = re.sub(r"[\x00-\x20\x7f]", "", href)
    try:
        return urlsplit(href).scheme.lower() in ("", "http", "https", "mailto")
    except ValueError:
        return False


def render_description_html(
    text: Optional[str], *, email: bool = False, feature_name: Optional[str] = None,
) -> str:
    """Render sanitized HTML without mutating the source or accessing the DB."""
    source = text if text and text.strip() else _FALLBACK
    soup = BeautifulSoup(_MARKDOWN.render(_normalize(source)), "html.parser")
    for comment in soup.find_all(string=lambda value: isinstance(value, Comment)):
        comment.extract()
    for tag in list(soup.find_all(True)):
        if tag.parent is None:
            continue
        name = tag.name
        if name in _DROP_TAGS:
            tag.decompose()
            continue
        if name == "img":
            tag.replace_with(soup.new_string(tag.get("alt", "")))
            continue
        if name not in _ALLOWED_TAGS:
            tag.unwrap()
            continue
        href = tag.get("href") if name == "a" else None
        start = tag.get("start") if name == "ol" else None
        tag.attrs = {}
        if href and _safe_href(href):
            tag["href"] = href
        if start and re.fullmatch(r"\d{1,9}", str(start)):
            tag["start"] = start
        if re.fullmatch(r"h[1-6]", name) and len(tag.get_text()) > 160:
            tag.name = "p"
        if email:
            style = _EMAIL_STYLES.get(tag.name)
            if re.fullmatch(r"h[1-6]", tag.name):
                style = "margin:12px 0 8px 0;font-size:14px;line-height:1.5;font-weight:600;"
            if style:
                tag["style"] = style
    first = soup.find(True, recursive=False)
    if feature_name and first and any(
        sibling.get_text(strip=True) for sibling in first.find_next_siblings()
    ) and (
        re.fullmatch(r"h[1-6]", first.name) or
        (first.name == "p" and len(first.contents) == 1 and first.find(["strong", "b"]))
    ):
        if " ".join(first.get_text().split()) == " ".join(feature_name.split()):
            first.decompose()
    if not soup.get_text(strip=True):
        return f"<p>{_FALLBACK}</p>"
    return str(soup).strip()


def render_description_text(text: Optional[str], *, feature_name: Optional[str] = None) -> str:
    """Readable plain text, retaining list markers and link destinations."""
    soup = BeautifulSoup(render_description_html(text, feature_name=feature_name), "html.parser")
    for whitespace in soup.find_all(string=lambda value: value.isspace()):
        if whitespace.parent.name in ("ul", "ol", "[document]"):
            whitespace.extract()
    for link in soup.find_all("a", href=True):
        label, href = link.get_text(), link["href"]
        link.replace_with(soup.new_string(label if label == href else f"{label} ({href})"))
    for br in soup.find_all("br"):
        br.replace_with("\n")
    for item in soup.find_all("li"):
        parent = item.parent
        marker = "- "
        if parent.name == "ol":
            number = int(parent.get("start", 1)) + len(item.find_previous_siblings("li"))
            marker = f"{number}. "
        item.insert_before("\n")
        item.insert(0, marker)
    for cell in soup.find_all(["th", "td"]):
        cell.insert_after("\t")
    for row in soup.find_all("tr"):
        row.insert_before("\n")
    for block in soup.find_all(["p", "pre", "blockquote", "h1", "h2", "h3", "h4", "h5", "h6"]):
        block.insert_before("\n\n")
        block.insert_after("\n\n")
    return re.sub(r"\n{3,}", "\n\n", soup.get_text()).strip()
