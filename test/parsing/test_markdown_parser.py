# tests/parsing/test_markdown_parser.py

from parsing.parsers import MarkdownStructuredParser, PlainTextParser
from schemas.documents import RawDocument


def make_raw_markdown(raw_text: str, title: str = "Test Doc") -> RawDocument:
    return RawDocument(
        doc_id="doc_123",
        corpus="tech_docs",
        source_type="markdown",
        source_path="data/raw/tech_docs/test.md",
        file_name="test.md",
        title=title,
        raw_text=raw_text,
    )


def test_markdown_parser_extracts_hierarchy() -> None:
    parser = MarkdownStructuredParser()
    doc = make_raw_markdown(
        "# Intro\nHello\n\n## Install\nSteps\n\n## Usage\nHow to use"
    )

    parsed = parser.parse(doc)

    assert len(parsed.sections) == 3
    assert parsed.sections[0].title == "Intro"
    assert parsed.sections[1].title == "Install"
    assert parsed.sections[2].title == "Usage"
    assert parsed.sections[1].parent_section_id == parsed.sections[0].section_id
    assert parsed.sections[2].parent_section_id == parsed.sections[0].section_id


def test_markdown_parser_removes_heading_anchor_suffix() -> None:
    parser = MarkdownStructuredParser()
    doc = make_raw_markdown("## Dependencies { #dependencies }\ntext")

    parsed = parser.parse(doc)

    assert len(parsed.sections) == 1
    assert parsed.sections[0].title == "Dependencies"


def test_markdown_parser_removes_frontmatter() -> None:
    parser = MarkdownStructuredParser()
    doc = make_raw_markdown("---\ntitle: Demo\n---\n\n# Intro\nHello")

    parsed = parser.parse(doc)

    assert len(parsed.sections) == 1
    assert "title: Demo" not in parsed.sections[0].text
    assert parsed.sections[0].title == "Intro"


def test_markdown_parser_fallback_when_no_headings() -> None:
    parser = MarkdownStructuredParser()
    doc = make_raw_markdown("plain markdown without headings", title="Fallback")

    parsed = parser.parse(doc)

    assert len(parsed.sections) == 1
    assert parsed.sections[0].title == "Fallback"
    assert parsed.sections[0].level == 1


def test_plain_text_parser_creates_single_section() -> None:
    parser = PlainTextParser()
    doc = RawDocument(
        doc_id="doc_txt_1",
        corpus="tech_docs",
        source_type="text",
        source_path="data/raw/tech_docs/test.txt",
        file_name="test.txt",
        title="Text Doc",
        raw_text="hello from text file",
    )

    parsed = parser.parse(doc)

    assert len(parsed.sections) == 1
    assert parsed.sections[0].title == "Text Doc"
    assert parsed.sections[0].text == "hello from text file"


def test_markdown_parser_appends_intro_text_to_first_section_and_keeps_empty_section() -> None:
    parser = MarkdownStructuredParser()
    doc = make_raw_markdown(
        "Intro text before first heading.\n\n# Intro\n"
    )

    parsed = parser.parse(doc)

    assert len(parsed.sections) == 1
    assert parsed.sections[0].title == "Intro"
    assert parsed.sections[0].text == "Intro text before first heading."
    assert parsed.sections[0].parent_section_id is None


def test_markdown_parser_allows_duplicate_headings_with_distinct_section_ids() -> None:
    parser = MarkdownStructuredParser()
    doc = make_raw_markdown(
        "# Intro\nFirst intro content\n\n# Intro\nSecond intro content"
    )

    parsed = parser.parse(doc)

    assert len(parsed.sections) == 2
    assert parsed.sections[0].title == "Intro"
    assert parsed.sections[1].title == "Intro"
    assert parsed.sections[0].section_id != parsed.sections[1].section_id
    assert parsed.sections[0].text == "First intro content"
    assert parsed.sections[1].text == "Second intro content"


def test_markdown_parser_keeps_structure_when_heading_levels_jump() -> None:
    parser = MarkdownStructuredParser()
    doc = make_raw_markdown(
        "# Root\nRoot content\n\n## Child\nChild content\n\n#### Deep Node\nDeep content"
    )

    parsed = parser.parse(doc)

    assert len(parsed.sections) == 3
    assert parsed.sections[0].title == "Root"
    assert parsed.sections[1].title == "Child"
    assert parsed.sections[2].title == "Deep Node"
    assert parsed.sections[1].parent_section_id == parsed.sections[0].section_id
    assert parsed.sections[2].parent_section_id == parsed.sections[1].section_id
    assert parsed.sections[2].level == 4


def test_markdown_parser_creates_section_for_single_heading_without_content() -> None:
    parser = MarkdownStructuredParser()
    doc = make_raw_markdown("# Lonely Heading")

    parsed = parser.parse(doc)

    assert len(parsed.sections) == 1
    assert parsed.sections[0].title == "Lonely Heading"
    assert parsed.sections[0].text == ""
    assert parsed.sections[0].level == 1
    assert parsed.sections[0].parent_section_id is None