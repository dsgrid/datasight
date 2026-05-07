"""``datasight run`` — start the FastAPI web UI."""

import os
from pathlib import Path

import rich_click as click

from datasight import cli
from datasight.cli_helpers import format_epilog


@click.command(
    epilog=format_epilog(
        """
        Examples:

            datasight run
            datasight run --project-dir eia-demo
            datasight run --port 9000 --model gpt-4o
            datasight run --unix-socket /tmp/datasight.sock
        """
    )
)
@click.option("--port", type=int, default=None, help="Web UI port (default: 8084).")
@click.option("--host", default="127.0.0.1", help="Bind address for TCP mode.")
@click.option(
    "--unix-socket",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Listen on this UNIX domain socket instead of TCP.",
)
@click.option("--model", default=None, help="LLM model name (overrides .env).")
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=None,
    help="Auto-load this project on startup (optional).",
)
def run(
    port,
    host,
    unix_socket,
    model,
    project_dir,
):
    """Start the datasight web UI.

    If the current directory contains schema_description.md, it will be
    auto-loaded as the project. Otherwise, use the UI to select a project,
    or pass --project-dir to specify one explicitly.
    """
    _, resolved_model, resolved_port = cli.prepare_web_runtime(
        port=port,
        model=model,
        project_dir=project_dir,
    )

    if project_dir:
        project_dir = str(Path(project_dir).resolve())

    click.echo(f"datasight v{cli.__version__}")
    click.echo(f"  Model:    {resolved_model}")
    if project_dir:
        click.echo(f"  Project:  {project_dir} (auto-load)")
    else:
        click.echo("  Project:  (none — select in UI)")
    if unix_socket:
        click.echo(f"  Socket:   {unix_socket}")
    else:
        click.echo(f"  Address:  http://{host}:{resolved_port}")
    click.echo()

    import uvicorn

    if unix_socket:
        if port is not None:
            raise click.UsageError("--port cannot be used with --unix-socket")
        os.environ["DATASIGHT_UNIX_SOCKET"] = str(unix_socket)
        click.echo(f"Starting web UI on UNIX socket {unix_socket} ...")
        uvicorn.run(
            "datasight.web.app:app",
            uds=str(unix_socket),
            log_level="warning",
        )
        return

    os.environ.pop("DATASIGHT_UNIX_SOCKET", None)
    click.echo(f"Starting web UI at http://{host}:{resolved_port} ...")
    uvicorn.run(
        "datasight.web.app:app",
        host=host,
        port=resolved_port,
        log_level="warning",
    )
