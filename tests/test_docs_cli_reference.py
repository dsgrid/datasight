from pathlib import Path

from scripts.generate_cli_reference import generate_markdown


def test_cli_reference_is_current():
    docs_path = Path("docs/reference/cli.md")
    assert docs_path.read_text(encoding="utf-8") == generate_markdown()
