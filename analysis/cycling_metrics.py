"""Cycle metrics calculated from measured time, current and cell voltage."""

import numpy as np
import pandas as pd


def _sample_intervals(frame):
    time, current, voltage = (
        pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        for column in ("time_h", "current_mA", "voltage_V")
    )
    current = current / 1000.0
    cycle = frame["cycle_number"].to_numpy()
    valid = np.isfinite(time) & np.isfinite(current) & np.isfinite(voltage)
    duration = np.diff(time)
    usable = valid[1:] & valid[:-1] & (duration > 0) & (cycle[1:] == cycle[:-1])
    return duration, current, voltage, usable


def _branch_integrals(duration, current, voltage, usable, direction):
    """Use adjacent samples within one cycle and direction, with no boundary extrapolation."""
    amps = current * direction
    active = amps > 0
    mask = usable & active[1:] & active[:-1]
    left = amps[:-1][mask]
    right = amps[1:][mask]
    half_interval = duration[mask] / 2
    charge = np.zeros(len(current), dtype=float)
    energy = np.zeros(len(current), dtype=float)
    charge[1:][mask] = (left + right) * half_interval
    energy[1:][mask] = (left * voltage[:-1][mask] + right * voltage[1:][mask]) * half_interval
    return charge, energy, mask.any()


def cumulative_discharge_energy(frame):
    """Cumulative delivered energy in Wh in acquisition order."""
    _, energy, has_discharge = _branch_integrals(*_sample_intervals(frame), direction=-1)
    if not has_discharge:
        return np.full(len(frame), np.nan)
    return np.cumsum(energy)


def average_voltage_gap(frame):
    intervals = _sample_intervals(frame)
    charge_ah, charge_wh, _ = _branch_integrals(*intervals, direction=1)
    discharge_ah, discharge_wh, _ = _branch_integrals(*intervals, direction=-1)
    totals = pd.DataFrame({
        "cycle_number": frame["cycle_number"].to_numpy(),
        "charge_Ah": charge_ah, "charge_Wh": charge_wh,
        "discharge_Ah": discharge_ah, "discharge_Wh": discharge_wh,
    }).groupby("cycle_number", as_index=False).sum()
    charge = totals["charge_Wh"].div(totals["charge_Ah"].where(totals["charge_Ah"] > 0))
    discharge = totals["discharge_Wh"].div(totals["discharge_Ah"].where(totals["discharge_Ah"] > 0))
    # Keep the existing output key for plot-data clients.
    return pd.DataFrame({"cycle_number": totals["cycle_number"], "Overvoltage": charge - discharge})
