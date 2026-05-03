"""Generate a static Markdown CLI reference from the Click command tree."""

from __future__ import annotations

from pathlib import Path
import re

import click

from datasight.cli import cli


OUTPUT_PATH = Path("docs/reference/cli.md")


def _param_label(param: click.Parameter) -> str:
    if isinstance(param, click.Argument):
        name = param.human_readable_name.upper()
        return f"`{name}`"

    pieces = []
    for opt in param.opts:
        pieces.append(f"`{opt}`")
    for opt in param.secondary_opts:
        pieces.append(f"`{opt}`")
    return ", ".join(pieces) or f"`{param.name}`"


def _param_detail(param: click.Parameter) -> str:
    detail = _clean_text(getattr(param, "help", "") or "")
    if isinstance(param, click.Option):
        default_text = _clean_default(param.default)
        if default_text:
            detail = f"{detail} Default: `{default_text}`.".strip()
        if getattr(param, "required", False):
            detail = f"{detail} Required.".strip()
    return detail or " "


def _clean_default(value: object) -> str:
    if value in (None, (), [], False):
        return ""
    if callable(value):
        return ""
    text = str(value)
    if text == "Sentinel.UNSET":
        return ""
    return text


def _clean_text(text: str) -> str:
    cleaned = text.replace("\b", "")
    cleaned = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", cleaned)
    cleaned = cleaned.replace(" Default: `Sentinel.UNSET`.", ".")
    cleaned = cleaned.replace(" Default: Sentinel.UNSET.", ".")
    cleaned = cleaned.strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned


def _clean_epilog(text: str) -> str:
    cleaned = _clean_text(text)
    lines = [line.rstrip() for line in cleaned.splitlines() if line.strip()]
    out: list[str] = []
    code: list[str] = []

    def flush() -> None:
        if not code:
            return
        common = min(len(line) - len(line.lstrip()) for line in code)
        if out and out[-1] != "":
            out.append("")
        out.append("```")
        out.extend(line[common:] for line in code)
        out.append("```")
        out.append("")
        code.clear()

    for line in lines:
        if line.startswith((" ", "\t")):
            code.append(line)
            continue
        flush()
        if out and out[-1] != "":
            out.append("")
        out.append(line)
        if line.endswith(":"):
            out.append("")
    flush()

    while out and out[-1] == "":
        out.pop()
    return "\n".join(out)


def _first_paragraph(text: str) -> str:
    paragraphs = [part.strip() for part in _clean_text(text).split("\n\n") if part.strip()]
    return paragraphs[0] if paragraphs else ""


def _usage_line(command_path: str, command: click.Command) -> str:
    ctx = click.Context(command, info_name=command_path)
    usage = command.get_usage(ctx).strip()
    return usage.replace("Usage: ", "", 1)


def _command_header(level: int, command_path: str) -> str:
    hashes = "#" * level
    return f"{hashes} `{command_path}`"


def _render_command(level: int, command_path: str, command: click.Command) -> list[str]:
    lines = [_command_header(level, command_path), ""]

    help_text = _clean_text(command.help or command.short_help or "")
    if help_text:
        lines.extend([help_text, ""])

    epilog = _clean_epilog(getattr(command, "epilog", "") or "")
    if epilog:
        lines.extend([epilog, ""])

    lines.extend(["```bash", _usage_line(command_path, command), "```", ""])

    params = [param for param in command.params if not getattr(param, "hidden", False)]
    if params:
        lines.extend(["**Parameters**", ""])
        lines.extend(["| Name | Details |", "| --- | --- |"])
        for param in params:
            lines.append(f"| {_param_label(param)} | {_param_detail(param)} |")
        lines.append("")

    if isinstance(command, click.Group):
        subcommands = list(command.commands.items())
        if subcommands:
            lines.extend(["**Subcommands**", ""])
            for name, subcommand in subcommands:
                summary = _first_paragraph(subcommand.short_help or subcommand.help or "")
                lines.append(f"- `{name}`: {summary}")
            lines.append("")
            for name, subcommand in subcommands:
                lines.extend(_render_command(level + 1, f"{command_path} {name}", subcommand))

    return lines


def generate_markdown() -> str:
    lines = [
        "# CLI reference",
        "",
        "This page is generated from the Click command tree in `datasight.cli`.",
        "Update it with `python scripts/generate_cli_reference.py`.",
        "",
        "## Common workflows",
        "",
        "### Run batch questions",
        "",
        "```bash",
        "datasight ask --file questions.txt --output-dir batch-output",
        "datasight ask --file questions.yaml --output-dir batch-output",
        "datasight ask --file questions.jsonl --output-dir batch-output",
        "```",
        "",
        "### Inspect a project without the LLM",
        "",
        "```bash",
        "datasight profile",
        "datasight profile --table generation_fuel",
        "datasight profile --column generation_fuel.report_date",
        "```",
        "",
        "### Run deterministic audits and suggestions",
        "",
        "```bash",
        "datasight quality --table generation_fuel",
        "datasight dimensions --table generation_fuel",
        "datasight trends --table generation_fuel",
        "datasight recipes list --table generation_fuel",
        "```",
        "",
        "### Check project health",
        "",
        "```bash",
        "datasight doctor",
        "datasight doctor --format markdown -o doctor.md",
        "```",
        "",
    ]

    lines.extend(_render_command(2, "datasight", cli))
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    OUTPUT_PATH.write_text(generate_markdown(), encoding="utf-8")


if __name__ == "__main__":
    main()
