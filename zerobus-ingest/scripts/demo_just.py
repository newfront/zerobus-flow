"""Helpers for demo-focused just recipes."""

from __future__ import annotations

import argparse
import time
from textwrap import wrap
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dotenv import load_dotenv

from zerobus_ingest.config import Config
from zerobus_ingest.main import _load_descriptor_from_binary
from zerobus_ingest.utils import TableUtils


def select_warehouse_id(client: WorkspaceClient) -> str:
    """Return the first available SQL warehouse id."""
    warehouses = [w for w in client.warehouses.list() if getattr(w, "id", None)]
    if not warehouses:
        raise RuntimeError("No SQL warehouses available in this workspace.")
    return warehouses[0].id


def execute_sql(client: WorkspaceClient, warehouse_id: str, statement: str):
    """Execute SQL and poll until completion."""
    stmt = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
    )
    for _ in range(60):
        if stmt.status.state not in (StatementState.PENDING, StatementState.RUNNING):
            break
        time.sleep(2)
        stmt = client.statement_execution.get_statement(stmt.statement_id)

    if stmt.status.state != StatementState.SUCCEEDED:
        err = stmt.status.error
        detail = f"{err.error_code}: {err.message}" if err else "unknown error"
        raise RuntimeError(f"SQL failed ({stmt.status.state}): {detail}")
    return stmt


def extract_rows(stmt) -> tuple[list[str], list[list[Any]]]:
    """Get column names and row values from statement response."""
    result = getattr(stmt, "result", None)
    if result is None:
        return [], []

    schema = None
    if getattr(stmt, "manifest", None) and getattr(stmt.manifest, "schema", None):
        schema = stmt.manifest.schema
    elif getattr(result, "manifest", None) and getattr(result.manifest, "schema", None):
        schema = result.manifest.schema
    headers = [col.name for col in schema.columns] if schema and schema.columns else []

    rows = [list(row) for row in (getattr(result, "data_array", None) or [])]
    if not headers and rows:
        headers = [f"col_{index + 1}" for index in range(len(rows[0]))]
    return headers, rows


def print_table(headers: list[str], rows: list[list[Any]]) -> None:
    """Render a plain-text table."""
    if not headers:
        print("(no result set)")
        return

    max_col_width = 72
    widths = [len(str(header)) for header in headers]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = min(
                max_col_width,
                max(widths[idx], len("" if value is None else str(value))),
            )

    def _cell_lines(value: Any, width: int) -> list[str]:
        text = "" if value is None else str(value)
        if not text:
            return [""]
        wrapped: list[str] = []
        for segment in text.splitlines() or [""]:
            # Preserve readability for nested/long type strings.
            wrapped.extend(
                wrap(
                    segment,
                    width=width,
                    break_long_words=True,
                    break_on_hyphens=False,
                )
                or [""]
            )
        return wrapped

    print(" | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers)))
    print("-+-".join("-" * widths[i] for i in range(len(widths))))
    for row in rows:
        wrapped_cells = [_cell_lines(value, widths[i]) for i, value in enumerate(row)]
        row_height = max(len(lines) for lines in wrapped_cells)
        for line_idx in range(row_height):
            print(
                " | ".join(
                    (
                        wrapped_cells[col_idx][line_idx]
                        if line_idx < len(wrapped_cells[col_idx])
                        else ""
                    ).ljust(widths[col_idx])
                    for col_idx in range(len(headers))
                )
            )


def bootstrap(args: argparse.Namespace) -> None:
    """Create demo table if needed, then print table metadata."""
    load_dotenv()
    config = Config.databricks()
    client = WorkspaceClient(host=config["host"], token=config["token"])

    if not TableUtils.table_exists(client, args.catalog, args.schema, args.table):
        print(
            f"{args.table_name} doesn't exist. "
            f"Creating zerobus table {args.table_name}"
        )
        descriptor = _load_descriptor_from_binary(
            Path(args.descriptor_path), args.message_name
        )
        columns = TableUtils.descriptor_to_columns(descriptor)
        TableUtils.create_managed_table(
            client,
            args.catalog,
            args.schema,
            args.table,
            columns,
        )

    warehouse_id = select_warehouse_id(client)
    stmt = execute_sql(
        client, warehouse_id, f"DESCRIBE TABLE EXTENDED {args.table_name}"
    )
    headers, rows = extract_rows(stmt)
    print_table(headers, rows)


def teardown(args: argparse.Namespace) -> None:
    """Drop the demo table from Unity Catalog if it exists."""
    load_dotenv()
    config = Config.databricks()
    client = WorkspaceClient(host=config["host"], token=config["token"])
    warehouse_id = select_warehouse_id(client)
    execute_sql(client, warehouse_id, f"DROP TABLE IF EXISTS {args.table_name}")
    print(f"Dropped table (if it existed): {args.table_name}")


def query(args: argparse.Namespace) -> None:
    """Print sampled rows from the demo table."""
    if args.limit <= 0:
        raise ValueError("limit must be > 0")

    print(f"Generate mode: {args.generate}")

    load_dotenv()
    config = Config.databricks()
    client = WorkspaceClient(host=config["host"], token=config["token"])

    warehouse_id = select_warehouse_id(client)
    stmt = execute_sql(
        client,
        warehouse_id,
        f"select * from {args.table_name} limit {args.limit}",
    )
    headers, rows = extract_rows(stmt)
    print_table(headers, rows)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Justfile helper commands for demo.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap_parser = subparsers.add_parser("bootstrap")
    bootstrap_parser.add_argument("--catalog", required=True)
    bootstrap_parser.add_argument("--schema", required=True)
    bootstrap_parser.add_argument("--table", required=True)
    bootstrap_parser.add_argument("--table-name", required=True)
    bootstrap_parser.add_argument("--descriptor-path", required=True)
    bootstrap_parser.add_argument("--message-name", required=True)
    bootstrap_parser.set_defaults(func=bootstrap)

    teardown_parser = subparsers.add_parser("teardown")
    teardown_parser.add_argument("--table-name", required=True)
    teardown_parser.set_defaults(func=teardown)

    query_parser = subparsers.add_parser("query")
    query_parser.add_argument("--table-name", required=True)
    query_parser.add_argument("--limit", type=int, default=100)
    query_parser.add_argument("--generate", default="orders")
    query_parser.set_defaults(func=query)

    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    arguments.func(arguments)
