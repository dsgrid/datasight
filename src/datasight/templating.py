"""
Mustache template loading and rendering for datasight.

Provides utilities for loading and rendering Mustache templates
from the templates directory.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import chevron


_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates" / "html"


@lru_cache(maxsize=16)
def _load_template(name: str) -> str:
    """Load a template file by name (cached).

    Parameters
    ----------
    name:
        Template name without extension (e.g., "chart" for "chart.mustache").

    Returns
    -------
    The template content as a string.

    Raises
    ------
    FileNotFoundError:
        If the template file does not exist.
    """
    path = _TEMPLATES_DIR / f"{name}.mustache"
    if not path.exists():
        raise FileNotFoundError(f"Template not found: {path}")
    return path.read_text()


def render_template(name: str, data: dict[str, Any]) -> str:
    """Render a Mustache template with the given data.

    Parameters
    ----------
    name:
        Template name without extension (e.g., "chart" for "chart.mustache").
    data:
        Dictionary of data to pass to the template.

    Returns
    -------
    The rendered HTML string.

    Raises
    ------
    FileNotFoundError:
        If the template file does not exist.
    """
    template = _load_template(name)
    return chevron.render(template, data)


def escape_html(text: str) -> str:
    """Escape HTML special characters.

    Parameters
    ----------
    text:
        The text to escape.

    Returns
    -------
    The escaped text.
    """
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def escape_html_attr(text: str) -> str:
    """Escape text for use in HTML attributes.

    Parameters
    ----------
    text:
        The text to escape.

    Returns
    -------
    The escaped text suitable for HTML attributes.
    """
    return escape_html(text).replace("'", "&#x27;")
