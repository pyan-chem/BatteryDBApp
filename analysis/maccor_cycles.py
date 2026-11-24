"""Maccor cycle numbering and last-cycle status from measured records."""

import numpy as np
import pandas as pd


def assign_maccor_cycles(frame, raw, charging, discharging):
    """Use recorded Cycle C, or reconstruct it from charge/discharge transitions."""
    direction = np.select([charging, discharging], [1, -1], default=0)
    if "Echem_Mode" not in raw:
        current = frame["current_mA"].to_numpy(dtype=float)
        direction = np.select([current > 0, current < 0], [1, -1], default=0)
    active = np.flatnonzero(direction)
    if not active.size:
        frame["cycle_number"] = np.zeros(len(frame), dtype=np.int64)
        frame.attrs["cycle_number_source"] = "Cycle C" if "Echem_Cycle_C" in raw else "inferred"
        return

    first = active[0]
    numbers = None
    if "Echem_Cycle_C" in raw:
        values = raw["Echem_Cycle_C"]
        if not pd.api.types.is_numeric_dtype(values):
            values = values.astype("string").str.replace(",", "", regex=False)
        values = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        cycling = values[first:]
        if (np.isfinite(cycling).all() and (cycling >= 1).all()
                and (cycling == np.floor(cycling)).all() and (np.diff(cycling) >= 0).all()
                and (cycling < 2**63).all()):
            numbers = np.zeros(len(frame), dtype=np.int64)
            numbers[first:] = cycling
    source = "Cycle C" if numbers is not None else "inferred"
    if numbers is None:
        transitions = (direction[active[:-1]] < 0) & (direction[active[1:]] > 0)
        starts = np.zeros(len(frame), dtype=bool)
        starts[first] = True
        starts[active[1:][transitions]] = True
        numbers = np.cumsum(starts)
    frame["cycle_number"] = numbers

    last = active[-1]
    last_number = int(numbers[last])
    last_active = active[numbers[active] == last_number]
    status = "incomplete"
    if direction[last] < 0 and (direction[last_active] > 0).any():
        status = "unconfirmed"
        if "Echem_Status" in raw:
            end_code = pd.to_numeric(raw["Echem_Status"].iat[last], errors="coerce")
            # Maccor's end-step code range as used by the BEEP Maccor reader.
            if pd.notna(end_code) and 128 <= end_code <= 255 and end_code != 192:
                status = "complete"
    frame.attrs.update(cycle_number_source=source, last_cycle_number=last_number, last_cycle_status=status)
