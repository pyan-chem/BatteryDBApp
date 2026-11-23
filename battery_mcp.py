"""Read the configured BatteryDBApp library over local MCP stdio."""

import argparse
from contextlib import closing
from datetime import date, datetime, time
from decimal import Decimal
import math
from pathlib import Path
import sqlite3
from typing import Annotated, Any, Literal

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
import pandas as pd
from pydantic import Field

from analysis.data_loading import load_and_process_cell
from analysis.plot_data import PLOTS, analyze_plot
from data_import.importer import DataImporter
from database import BatteryDatabase


Plot = Literal[
    "capacity", "overvoltage", "accumulated_energy", "endpoint_slippage",
    "voltage_time", "voltage_capacity", "dqdv", "dvdq",
]
PLOT_NAMES: dict[Plot, str] = {
    "capacity": "Cycling vs. Capacity",
    "overvoltage": "Cycling vs. Overvoltage",
    "accumulated_energy": "Cycling vs. Accumulated Energy",
    "endpoint_slippage": "Cycling vs. Endpointslippage",
    "voltage_time": "Voltage vs. Time",
    "voltage_capacity": "Voltage vs. Capacity cyclic",
    "dqdv": "dQ/dV",
    "dvdq": "dV/dQ",
}
COLUMNS = ", ".join(sorted(BatteryDatabase.ALLOWED_COLUMNS))
SEARCH_COLUMNS = (
    "id", "instrument", "date", "positive_electrode", "negative_electrode",
    "electrolyte", "format", "voltage_range", "status", "comment", "file_path",
)
READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
Row = dict[str, Any]
Offset = Annotated[int, Field(ge=0, le=2**63 - 1)]
Limit = Annotated[int, Field(ge=1, le=1000)]
Cycle = Annotated[int, Field(ge=0)]
INTERPOLATION_BUDGET = 100_000
InterpolationPoints = Annotated[int, Field(ge=2, le=INTERPOLATION_BUDGET)]


def json_scalar(value):
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else None
    if pd.isna(value) or (isinstance(value, float) and not math.isfinite(value)):
        return None
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return value


def frame_page(frame, offset, limit):
    page = frame.iloc[offset:offset + limit]
    return {
        "columns": frame.columns.tolist(),
        "rows": [
            {column: json_scalar(value) for column, value in row.items()}
            for row in page.to_dict(orient="records")
        ],
        "total_rows": len(frame),
        "next_offset": offset + len(page) if offset + len(page) < len(frame) else None,
    }


def select_cycles(sample, cycles, cycle_start, cycle_end, *, default_start):
    if cycle_start is not None and cycle_end is not None and cycle_start > cycle_end:
        raise ValueError("cycle_start must be less than or equal to cycle_end.")
    if (cycle_start is not None or cycle_end is not None) and cycles is not None:
        raise ValueError("Use either cycles or a cycle range.")
    available = sample["cycle_number"].dropna().unique()
    if cycles is not None:
        requested = set(cycles)
        return [cycle for cycle in available if cycle in requested]
    start = default_start if cycle_start is None else cycle_start
    return [cycle for cycle in available if cycle >= start and (cycle_end is None or cycle <= cycle_end)]


def read_only_uri(database_path):
    uri = database_path.as_uri()
    if database_path.drive.startswith("\\\\"):
        # SQLite expects the UNC hostname in the path, with an empty URI authority.
        uri = "file:////" + uri.removeprefix("file://")
    return uri + "?mode=ro"


def create_server(database_path: Path, data_root: Path | None = None) -> FastMCP:
    database_path = database_path.expanduser().resolve()
    data_root = data_root.expanduser().resolve() if data_root is not None else database_path.parent
    database_uri = read_only_uri(database_path)

    def connect():
        connection = sqlite3.connect(database_uri, uri=True)
        connection.row_factory = sqlite3.Row
        return connection

    with closing(connect()) as connection:
        fields = {row["name"] for row in connection.execute("PRAGMA table_info(cells)")}
        missing = BatteryDatabase.ALLOWED_COLUMNS - fields
        if missing:
            raise sqlite3.DatabaseError(f"Missing cells fields: {', '.join(sorted(missing))}")

    server = FastMCP(
        "BatteryDBApp",
        instructions=(
            "Search the configured read-only battery library, then use the exact battery ID. "
            "Use get_plot_data for cycling metrics and graph points. Use get_measurements for "
            "standardized point data, and get_instrument_rows for instrument-specific columns. "
            "Follow next_offset with the same arguments to read subsequent pages."
        ),
    )

    @server.tool(annotations=READ_ONLY)
    def search_batteries(
        query: str = "", instrument: str | None = None, status: str | None = None,
        offset: Offset = 0, limit: Limit = 25,
    ) -> Row:
        """Search battery records by literal text. Optionally filter by instrument or status."""
        with closing(connect()) as connection:
            connection.create_function("casefold", 1, lambda value: str(value or "").casefold())
            conditions, parameters = [], []
            if query.strip():
                contains = " OR ".join(f"instr(casefold({column}), ?) > 0" for column in SEARCH_COLUMNS)
                conditions.append(f"({contains})")
                parameters.extend([query.strip().casefold()] * len(SEARCH_COLUMNS))
            for column, value in (("instrument", instrument), ("status", status)):
                if value is not None:
                    conditions.append(f"casefold({column}) = ?")
                    parameters.append(value.strip().casefold())
            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            records = connection.execute(
                f"SELECT {COLUMNS} FROM cells {where} ORDER BY date DESC, id LIMIT ? OFFSET ?",
                (*parameters, limit + 1, offset),
            ).fetchall()
        return {
            "rows": [{key: json_scalar(value) for key, value in dict(row).items()} for row in records[:limit]],
            "next_offset": offset + limit if len(records) > limit else None,
        }

    @server.tool(annotations=READ_ONLY)
    def get_battery(battery_id: str) -> Row:
        """Read all metadata fields for one exact battery ID."""
        with closing(connect()) as connection:
            record = connection.execute(
                f"SELECT {COLUMNS} FROM cells WHERE id = ?", (battery_id,),
            ).fetchone()
        if record is None:
            raise ValueError(f"Battery {battery_id!r} was not found.")
        return {key: json_scalar(value) for key, value in dict(record).items()}

    def linked_file(record):
        stored_path = record["file_path"]
        if not isinstance(stored_path, str) or not stored_path.strip():
            raise ValueError(f"Battery {record['id']!r} has no linked instrument file.")
        path = data_root / Path(stored_path).expanduser()
        if not path.is_file():
            raise ValueError(f"The linked instrument file is unavailable for battery {record['id']!r}.")
        return str(path)

    def measurements(battery_id):
        record = get_battery(battery_id)
        path = linked_file(record)
        try:
            mass = float(record["p_active_mass"])
        except (ValueError, TypeError):
            raise ValueError(f"Set a valid positive active mass in grams for battery {battery_id!r}.") from None
        if not math.isfinite(mass) or mass <= 0:
            raise ValueError(f"Set a valid positive active mass in grams for battery {battery_id!r}.")
        return load_and_process_cell(path, record["instrument"], mass)

    @server.tool(annotations=READ_ONLY)
    def get_available_cycles(battery_id: str) -> Row:
        """List cycle numbers in the processed measurements. Cycle 0 is the initial rest, when present."""
        sample = measurements(battery_id)
        return {"battery_id": battery_id, "cycles": sorted(int(c) for c in sample["cycle_number"].dropna().unique())}

    @server.tool(annotations=READ_ONLY)
    def get_measurements(
        battery_id: str, cycles: list[Cycle] | None = None, cycle_start: Cycle | None = None,
        cycle_end: Cycle | None = None, offset: Offset = 0, limit: Limit = 100,
    ) -> Row:
        """Read standardized measurement rows with time, voltage, current, capacity and energy.

        Pass explicit cycles or an inclusive range. The default includes cycle 0.
        """
        sample = measurements(battery_id)
        selected = select_cycles(sample, cycles, cycle_start, cycle_end, default_start=0)
        return {"battery_id": battery_id, **frame_page(sample[sample["cycle_number"].isin(selected)], offset, limit)}

    @server.tool(annotations=READ_ONLY)
    def get_instrument_rows(battery_id: str, offset: Offset = 0, limit: Limit = 100) -> Row:
        """Read the instrument reader's output before cleaning or standardization.

        Columns depend on the instrument. No active mass is needed.
        """
        record = get_battery(battery_id)
        frame = DataImporter.import_data(linked_file(record), record["instrument"])
        return {"battery_id": battery_id, **frame_page(frame, offset, limit)}

    @server.tool(annotations=READ_ONLY)
    def get_plot_data(
        battery_id: str, plot: Plot, cycles: list[Cycle] | None = None,
        cycle_start: Cycle | None = None, cycle_end: Cycle | None = None,
        interpolation_points: InterpolationPoints | None = None, offset: Offset = 0, limit: Limit = 100,
    ) -> Row:
        """Read graph points or per-cycle metrics using BatteryDBApp's plot calculations.

        Pass explicit cycles or an inclusive range. Cycle 0 is excluded by default.
        Supply interpolation_points only for dqdv to enable the app's interpolation.
        """
        if interpolation_points is not None and plot != "dqdv":
            raise ValueError("Interpolation is only available for dqdv.")
        sample = measurements(battery_id)
        selected = select_cycles(sample, cycles, cycle_start, cycle_end, default_start=1)
        if interpolation_points is not None and interpolation_points * len(selected) > INTERPOLATION_BUDGET:
            raise ValueError("Select fewer cycles or reduce interpolation_points (100,000 per request across cycles).")
        name = PLOT_NAMES[plot]
        definition = PLOTS[name]
        frame = analyze_plot(sample, selected, name, definition, interpolation_points=interpolation_points)
        columns = ["cycle_number", *(column for column in definition.export_columns if column != "cycle_number")]
        if plot == "dqdv":
            columns.append("type")
        return {
            "battery_id": battery_id,
            "plot": plot,
            "x_column": definition.x,
            "x_label": definition.x_label,
            "y_label": definition.y_label,
            "series": [{"column": column, "label": label} for column, label in definition.series],
            **frame_page(frame.reindex(columns=columns), offset, limit),
        }

    return server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path, help="Existing BatteryDBApp SQLite database")
    parser.add_argument("--data-root", type=Path, help="Base for relative instrument paths (default: database folder)")
    args = parser.parse_args()
    try:
        server = create_server(args.db, args.data_root)
    except (OSError, sqlite3.Error) as error:
        parser.exit(2, f"Could not open BatteryDBApp database {args.db}: {error}\n")
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
