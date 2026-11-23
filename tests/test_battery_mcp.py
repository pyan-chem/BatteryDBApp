import asyncio
from datetime import timedelta
import os
from pathlib import Path, PureWindowsPath
import sqlite3
import subprocess
import sys

import pytest

pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from battery_mcp import read_only_uri
from database import BatteryDatabase


SERVER = Path(__file__).resolve().parents[1] / "battery_mcp.py"


@pytest.fixture
def library(tmp_path):
    path = tmp_path / "battery # 测试.db"
    data = tmp_path / "test.001"
    data.write_text(
        "Maccor\n"
        "Test (Sec)\tAmp-hr\tAmps\tVolts\tWatt-hr\tCyc#\tMD\n"
        "0\t0\t0\t3.0\t0\t1\tR\n"
        "1\t0\t0.002\t3.1\t0\t1\tC\n"
        "2\t0.001\t0.002\t3.5\t0\t1\tC\n"
        "3\t0\t0.002\t3.4\t0\t1\tD\n"
        "4\t0.0008\t0.002\t3.0\t0.003\t1\tD\n"
        "5\t0\t0.002\t3.1\t0\t2\tC\n"
        "6\t0.0003\t0.002\t3.2\t0\t2\tC\n"
        "7\t0.0006\t0.002\t3.4\t0\t2\tC\n"
        "8\t0.001\t0.002\t3.5\t0\t2\tC\n"
        "9\t0\t0.002\t3.4\t0\t2\tD\n"
        "10\t0.0002\t0.002\t3.3\t0.001\t2\tD\n"
        "11\t0.0005\t0.002\t3.2\t0.002\t2\tD\n"
        "12\t0.0008\t0.002\t3.0\t0.003\t2\tD\n",
        encoding="utf-8",
    )
    with BatteryDatabase(str(path)) as db:
        db.add_cell({"id": "LFP_01", "instrument": "Maccor", "date": "2025-01-02",
                     "positive_electrode": "LFP", "file_path": data.name, "p_active_mass": 0.01})
        db.add_cell({"id": "LFP-02", "instrument": "Maccor", "date": "2025-01-01",
                     "positive_electrode": "LFP", "file_path": "missing.txt"})
        db.add_cell({"id": "NO-MASS", "instrument": "Maccor", "file_path": data.name})
    return path


async def call(session, name, **arguments):
    result = await session.call_tool(name, arguments)
    assert not result.isError, result.content
    assert result.structuredContent is not None
    return result.structuredContent


def test_codex_can_read_battery_records_metrics_and_points(library):
    original = library.read_bytes()

    async def exercise():
        server = StdioServerParameters(
            command=sys.executable, args=[str(SERVER), "--db", str(library)],
            env=dict(os.environ), cwd=str(library.parent.parent),
        )
        async with (
            stdio_client(server) as (read, write),
            ClientSession(read, write, read_timeout_seconds=timedelta(seconds=20)) as session,
        ):
            await session.initialize()
            tools = (await session.list_tools()).tools
            assert len(tools) == 6
            assert all(tool.annotations.readOnlyHint for tool in tools)

            first = await call(session, "search_batteries", query="lfp", limit=1)
            assert [row["id"] for row in first["rows"]] == ["LFP_01"]
            second = await call(session, "search_batteries", query="lfp", offset=first["next_offset"])
            assert [row["id"] for row in second["rows"]] == ["LFP-02"]
            literal = await call(session, "search_batteries", query="_", instrument="maccor")
            assert [row["id"] for row in literal["rows"]] == ["LFP_01"]
            battery = await call(session, "get_battery", battery_id="LFP_01")
            assert battery["file_path"] == "test.001"

            assert (await call(session, "get_available_cycles", battery_id="LFP_01"))["cycles"] == [0, 1, 2]
            cases = {
                "capacity": ("discharge_cap_mAh_g", 80.0),
                "overvoltage": ("Overvoltage", 0.1),
                "accumulated_energy": ("Accu_Energy_Wh", 0.003),
                "endpoint_slippage": ("charge_endpoint", 100.0),
                "voltage_time": ("voltage_V", 3.1),
                "voltage_capacity": ("(Q-Qo)_mAh_g", 0.0),
                "dqdv": ("type", "charge"),
                "dvdq": ("dVdQ", 0.005),
            }
            for plot, (column, expected) in cases.items():
                page = await call(session, "get_plot_data", battery_id="LFP_01", plot=plot,
                                  cycle_start=1, cycle_end=1, limit=1)
                assert page["rows"][0]["cycle_number"] == 1
                value = page["rows"][0][column]
                assert value == (pytest.approx(expected) if isinstance(expected, float) else expected)

            measure = await call(session, "get_measurements", battery_id="LFP_01", limit=2)
            assert [row["cycle_number"] for row in measure["rows"]] == [0, 1]
            following = await call(session, "get_measurements", battery_id="LFP_01",
                                   offset=measure["next_offset"], limit=2)
            assert [row["voltage_V"] for row in following["rows"]] == [3.5, 3.4]
            source = await call(session, "get_instrument_rows", battery_id="NO-MASS", limit=1)
            assert source["rows"][0]["Echem_Mode"] == "R"
            default = await call(session, "get_plot_data", battery_id="LFP_01", plot="capacity")
            assert [row["cycle_number"] for row in default["rows"]] == [1, 2]
            rest = await call(session, "get_plot_data", battery_id="LFP_01", plot="voltage_time",
                              cycle_start=0, cycle_end=0)
            assert [(row["cycle_number"], row["voltage_V"]) for row in rest["rows"]] == [(0, 3.0)]
            empty = await call(session, "get_plot_data", battery_id="LFP_01", plot="dqdv",
                               cycle_start=99)
            assert empty["rows"] == []
            assert empty["next_offset"] is None
            interpolated = await call(session, "get_plot_data", battery_id="LFP_01", plot="dqdv",
                                      cycles=[2], interpolation_points=5)
            assert [row["type"] for row in interpolated["rows"]] == ["charge"] * 5 + ["discharge"] * 5
            assert interpolated["rows"][0]["dQdV_discharge"] is None
            slippage = await call(session, "get_plot_data", battery_id="LFP_01", plot="endpoint_slippage",
                                  cycles=[2])
            assert slippage["rows"][0]["discharge_endpoint"] == pytest.approx(20.0)
            for name, params in [
                ("get_battery", {"battery_id": "' OR 1=1 --"}),
                ("get_instrument_rows", {"battery_id": "LFP-02"}),
                ("get_measurements", {"battery_id": "NO-MASS"}),
                ("get_measurements", {"battery_id": "LFP_01", "limit": 0}),
                ("get_plot_data", {"battery_id": "LFP_01", "plot": "capacity", "cycle_start": 2, "cycle_end": 1}),
                ("get_plot_data", {"battery_id": "LFP_01", "plot": "dqdv",
                                   "cycles": [0, 1], "interpolation_points": 100_000}),
            ]:
                assert (await session.call_tool(name, params)).isError

    asyncio.run(exercise())
    assert library.read_bytes() == original
    with sqlite3.connect(library) as db:
        assert db.execute("SELECT count(*) FROM cells").fetchone()[0] == 3


def test_missing_library_does_not_create_a_database(tmp_path):
    missing = tmp_path / "absent.db"
    result = subprocess.run(
        [sys.executable, str(SERVER), "--db", str(missing)], input="", encoding="utf-8",
        capture_output=True, timeout=20,
    )
    assert result.returncode != 0
    assert "BatteryDBApp database" in result.stderr
    assert not missing.exists()


def test_unc_database_uses_an_authority_free_read_only_uri():
    path = PureWindowsPath("//lab-server/shared folder/battery#1.db")
    assert read_only_uri(path) == "file:////lab-server/shared%20folder/battery%231.db?mode=ro"
