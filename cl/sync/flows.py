"""Prefect flow that syncs warehouse staged data into CourtListener.

Runs inside a Django process via the ``sync_worker`` management command.
Reads from ``courtlistener.staged_*`` tables in the warehouse DB and
saves objects via the Django ORM, then writes ``courtlistener_id`` back
to the warehouse.

Usage:
    Triggered automatically by the ``sync.prepared`` Prefect event
    after SQLMesh transforms complete.  Can also be triggered manually::

        prefect deployment run sync-warehouse/sync-pool
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone

from django.db import connections
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.logging import get_run_logger

from cl.sync.mappers import MAPPER_REGISTRY, TABLE_KEYS

logger = logging.getLogger(__name__)


def _get_logger():
    """Get the Prefect run logger if available, else fall back to module logger."""
    try:
        return get_run_logger()
    except Exception:
        return logger

# Tables in dependency order (dockets first, then dependents).
DEFAULT_TABLES = [
    "dockets",
    "originating_court_information",
    "opinion_clusters",
    "opinions",
    "docket_entries",
    "audio",
]

CHUNK_SIZE = 500


def _get_warehouse_cursor():
    """Get a database cursor for the warehouse connection."""
    return connections["warehouse"].cursor()


def _read_sync_state(table_name: str) -> int:
    """Read the last-synced provenance high-water mark for a table."""
    with _get_warehouse_cursor() as cur:
        cur.execute(
            "SELECT last_provenance FROM warehouse.sync_state "
            "WHERE table_name = %s",
            [table_name],
        )
        row = cur.fetchone()
        return row[0] if row else 0


def _write_sync_state(table_name: str, last_provenance: int) -> None:
    """Update the sync state high-water mark for a table."""
    with _get_warehouse_cursor() as cur:
        cur.execute(
            "INSERT INTO warehouse.sync_state (table_name, last_provenance, updated_at) "
            "VALUES (%s, %s, NOW()) "
            "ON CONFLICT (table_name) DO UPDATE "
            "SET last_provenance = EXCLUDED.last_provenance, "
            "    updated_at = EXCLUDED.updated_at",
            [table_name, last_provenance],
        )


def _get_pending_count(table_name: str, last_prov: int) -> int:
    """Count rows in a staged table newer than the high-water mark."""
    with _get_warehouse_cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM courtlistener.staged_{table_name} "
            "WHERE version_provenance > %s",
            [last_prov],
        )
        return cur.fetchone()[0]


def _fetch_chunk(
    table_name: str, last_prov: int, chunk_size: int, offset: int
) -> list[dict]:
    """Fetch a chunk of staged rows newer than the high-water mark.

    ORDER BY must be fully deterministic (include the natural key)
    so that OFFSET pagination doesn't skip or duplicate rows when
    concurrent UPDATEs (writebacks) create new tuple versions.
    """
    key_cols = TABLE_KEYS[table_name]
    order_cols = ", ".join(["version_provenance"] + key_cols)
    with _get_warehouse_cursor() as cur:
        cur.execute(
            f"SELECT * FROM courtlistener.staged_{table_name} "
            f"WHERE version_provenance > %s "
            f"ORDER BY {order_cols} "
            f"LIMIT %s OFFSET %s",
            [last_prov, chunk_size, offset],
        )
        columns = [col.name for col in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def _writeback_cl_id(
    table_name: str, row: dict, cl_id: int
) -> None:
    """Write the courtlistener_id back to the staged table."""
    key_cols = TABLE_KEYS[table_name]
    where_clause = " AND ".join(f"{col} = %s" for col in key_cols)
    params = [cl_id] + [row[col] for col in key_cols]

    with _get_warehouse_cursor() as cur:
        cur.execute(
            f"UPDATE courtlistener.staged_{table_name} "
            f"SET courtlistener_id = %s "
            f"WHERE {where_clause}",
            params,
        )


@task(
    retries=1,
    retry_delay_seconds=30,
    task_run_name="sync-{table_name}",
)
def sync_table(table_name: str) -> dict:
    """Sync all pending rows for a single staged table.

    Reads the high-water mark, fetches changed rows in chunks,
    saves each via Django ORM, writes back courtlistener_id, and
    updates the high-water mark.

    Returns a summary dict with counts.
    """
    log = _get_logger()
    mapper = MAPPER_REGISTRY[table_name]

    last_prov = _read_sync_state(table_name)
    pending = _get_pending_count(table_name, last_prov)

    if pending == 0:
        log.info("%s: no pending rows (hwm=%d)", table_name, last_prov)
        return {
            "table": table_name,
            "pending": 0,
            "synced": 0,
            "errors": 0,
            "error_types": {},
            "hwm_before": last_prov,
            "hwm_after": last_prov,
        }

    num_chunks = (pending + CHUNK_SIZE - 1) // CHUNK_SIZE
    log.info(
        "%s: %d pending rows in %d chunks (hwm=%d)",
        table_name,
        pending,
        num_chunks,
        last_prov,
    )

    synced = 0
    errors = 0
    chunk_errors = 0
    max_prov = last_prov
    # Track error types: {error_type_str: {"count": int, "example": str}}
    error_types: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "example": ""}
    )

    for chunk_num in range(num_chunks):
        offset = chunk_num * CHUNK_SIZE
        rows = _fetch_chunk(table_name, last_prov, CHUNK_SIZE, offset)

        if not rows:
            break

        chunk_errors = 0
        for row in rows:
            try:
                cl_id = mapper(row)
                _writeback_cl_id(table_name, row, cl_id)
                synced += 1

                row_prov = row.get("version_provenance", 0)
                if row_prov > max_prov:
                    max_prov = row_prov

            except Exception as exc:
                log.exception(
                    "%s: failed to sync row %s",
                    table_name,
                    {k: row.get(k) for k in TABLE_KEYS.get(table_name, [])},
                )
                errors += 1
                chunk_errors += 1

                err_type = type(exc).__name__
                entry = error_types[err_type]
                entry["count"] += 1
                if not entry["example"]:
                    entry["example"] = str(exc)[:200]

        # Only advance the high-water mark if the chunk had zero errors.
        # This ensures failed rows are retried on the next sync run.
        if chunk_errors == 0 and max_prov > last_prov:
            _write_sync_state(table_name, max_prov)
        elif chunk_errors > 0:
            log.warning(
                "%s: NOT advancing high-water mark due to %d errors "
                "(hwm stays at %d). Failed rows will be retried.",
                table_name,
                chunk_errors,
                last_prov,
            )

        log.info(
            "%s: chunk %d/%d — synced=%d errors=%d",
            table_name,
            chunk_num + 1,
            num_chunks,
            synced,
            errors,
        )

    hwm_after = max_prov if errors == 0 else last_prov
    log.info(
        "%s: completed — synced=%d errors=%d hwm=%d→%d",
        table_name,
        synced,
        errors,
        last_prov,
        hwm_after,
    )

    return {
        "table": table_name,
        "pending": pending,
        "synced": synced,
        "errors": errors,
        "error_types": dict(error_types),
        "hwm_before": last_prov,
        "hwm_after": hwm_after,
    }


@flow(name="sync-warehouse", log_prints=True)
def sync_warehouse(
    tables: list[str] | None = None,
) -> list[dict]:
    """Sync pending warehouse rows to CourtListener.

    Processes tables in dependency order (dockets first).
    Triggered by the ``sync.prepared`` Prefect event after SQLMesh
    transforms complete.

    Args:
        tables: Override the list of tables to sync. Defaults to all 6.

    Returns:
        List of per-table summary dicts.
    """
    log = _get_logger()

    if tables is None:
        tables = DEFAULT_TABLES

    log.info("Starting warehouse sync for tables: %s", tables)

    results = []
    docket_errors = 0
    for table_name in tables:
        if table_name not in MAPPER_REGISTRY:
            log.warning("Unknown table: %s, skipping", table_name)
            continue

        if table_name != "dockets" and docket_errors > 0:
            log.warning(
                "%s: docket sync had %d errors — dependent table "
                "will likely have matching errors for the same rows.",
                table_name,
                docket_errors,
            )

        result = sync_table(table_name)
        results.append(result)

        if table_name == "dockets":
            docket_errors = result.get("errors", 0)

    total_synced = sum(r["synced"] for r in results)
    total_errors = sum(r["errors"] for r in results)
    total_pending = sum(r.get("pending", 0) for r in results)
    log.info(
        "Warehouse sync complete: %d synced, %d errors across %d tables",
        total_synced,
        total_errors,
        len(results),
    )

    # Build and publish a markdown artifact summarising the run.
    markdown = _build_sync_summary(results, total_pending, total_synced, total_errors)
    create_markdown_artifact(
        key="sync-summary",
        markdown=markdown,
        description="Warehouse → CourtListener sync results",
    )

    return results


# ---------------------------------------------------------------------------
# Markdown artifact
# ---------------------------------------------------------------------------

def _build_sync_summary(
    results: list[dict],
    total_pending: int,
    total_synced: int,
    total_errors: int,
) -> str:
    """Build a markdown summary of the sync run."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if total_errors == 0:
        status = "Completed"
    elif total_synced == 0:
        status = "Failed"
    else:
        status = "Completed with errors"

    lines: list[str] = []
    lines.append("# Warehouse Sync Summary")
    lines.append("")
    lines.append(f"**Run time:** {now}")
    lines.append("")

    # -- overview table --
    lines.append("## Overview")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Status | {status} |")
    lines.append(f"| Tables processed | {len(results)} |")
    lines.append(f"| Total pending | {total_pending} |")
    lines.append(f"| Total synced | {total_synced} |")
    lines.append(f"| Total errors | {total_errors} |")
    lines.append("")

    # -- per-table results --
    lines.append("## Results by Table")
    lines.append("")
    lines.append("| Table | Pending | Synced | Errors | HWM |")
    lines.append("|-------|---------|--------|--------|-----|")
    for r in results:
        table = r["table"]
        pending = r.get("pending", "—")
        synced = r["synced"]
        errs = r["errors"]
        hwm_before = r.get("hwm_before", "?")
        hwm_after = r.get("hwm_after", "?")
        err_badge = f" **{errs}**" if errs > 0 else str(errs)
        lines.append(
            f"| {table} | {pending} | {synced} | {err_badge} | "
            f"{hwm_before} → {hwm_after} |"
        )
    lines.append("")

    # -- error breakdown (only tables with errors) --
    tables_with_errors = [r for r in results if r.get("error_types")]
    if tables_with_errors:
        lines.append("## Errors")
        lines.append("")
        for r in tables_with_errors:
            table = r["table"]
            err_count = r["errors"]
            lines.append(f"### {table} ({err_count} error{'s' if err_count != 1 else ''})")
            lines.append("")
            lines.append("| Error Type | Count | Example |")
            lines.append("|------------|-------|---------|")
            for err_type, info in sorted(
                r["error_types"].items(), key=lambda x: -x[1]["count"]
            ):
                example = info["example"].replace("|", "\\|").replace("\n", " ")
                lines.append(
                    f"| `{err_type}` | {info['count']} | {example} |"
                )
            lines.append("")
    else:
        lines.append("No errors.")
        lines.append("")

    return "\n".join(lines)
