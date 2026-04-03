"""Sphinx configuration for datasight documentation."""

project = "datasight"
copyright = "2025, dthom"
author = "dthom"
release = "0.1.0"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx_click",
    "sphinxcontrib.mermaid",
    "sphinx_design",
    "sphinx_copybutton",
]

myst_enable_extensions = [
    "colon_fence",
    "deflist",
]
myst_fence_as_directive = ["mermaid"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

templates_path = ["_templates"]
exclude_patterns = ["_build"]

html_theme = "furo"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_title = "datasight"
html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "#4a86c8",
        "color-brand-content": "#023d60",
    },
    "dark_css_variables": {
        "color-brand-primary": "#6ba3e0",
        "color-brand-content": "#5ec4e8",
    },
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

autodoc_member_order = "bysource"
napoleon_google_docstring = True
