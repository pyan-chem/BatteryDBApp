"""Identify constant-current intervals and calculate derivatives within each interval."""

import numpy as np
import pandas as pd
from scipy.interpolate import interp1d


CURRENT_TOLERANCE = 0.02
STARTUP_POINTS = 5


def _boundary_changes(values):
    codes, _ = pd.factorize(values)
    return codes[1:] != codes[:-1]


def _infer_plateau(current, voltage):
    """Find one leading current plateau, allowing a short startup transient."""
    size = len(current)
    width = min(3, size)
    early = np.lib.stride_tricks.sliding_window_view(current[:STARTUP_POINTS + width - 1], width)
    candidates = np.flatnonzero(np.ptp(early, axis=1) <= CURRENT_TOLERANCE * early[:, 0])
    if not candidates.size:
        return slice(0, 0)
    start = int(candidates[0])
    target = np.median(current[start:start + width])
    # A flat voltage together with substantial current taper is evidence of CV.
    # A flat voltage with stable current can still belong to a legitimate CC plateau.
    if current[-1] < (1 - CURRENT_TOLERANCE) * target:
        tail = voltage[max(start, size // 2):]
        level = np.median(voltage[start:start + width])
        if np.ptp(tail) <= 0.005 and abs(np.median(tail) - level) <= 0.005:
            return slice(0, 0)
    outside = np.flatnonzero(np.abs(current[start + width:] - target) > CURRENT_TOLERANCE * target)
    stop = start + width + int(outside[0]) if outside.size else size
    return slice(start, stop)


def classify_cc(frame, raw=None, instrument=None):
    """Return segment IDs and whether each point's mode comes from the instrument."""
    length = len(frame)
    segments = np.full(length, -1, dtype=np.int32)
    recorded = np.zeros(length, dtype=bool)
    if not length:
        return segments, recorded
    current = frame["current_mA"].to_numpy(dtype=float)
    voltage = frame["voltage_V"].to_numpy(dtype=float)
    direction = np.sign(current)
    direction[~np.isfinite(current) | ~np.isfinite(voltage)] = 0
    mode = np.zeros(length, dtype=np.int8)  # Unknown 0, recorded CC 1, non-CC 2.
    if "control_mode" in frame:
        explicit = frame["control_mode"].astype("string").str.strip().str.upper()
        mode[explicit.isin(["CC", "GALVANOSTATIC"]).to_numpy()] = 1
        mode[explicit.isin(["CV", "POTENTIOSTATIC", "REST"]).to_numpy()] = 2
    if raw is not None and instrument == "Biologic" and "mode" in raw:
        values = raw["mode"].reindex(frame.index)
        mode[values.eq(1).fillna(False).to_numpy(dtype=bool)] = 1
        mode[values.isin([2, 3]).to_numpy(dtype=bool)] = 2

    boundaries = _boundary_changes(frame["cycle_number"]) | (direction[1:] != direction[:-1])
    boundaries |= mode[1:] != mode[:-1]
    if "step_index" in frame:
        boundaries |= _boundary_changes(frame["step_index"])
    if raw is not None:
        for name in ("Echem_Full Step #", "Echem_Step", "Step_Index", "Ns"):
            if name in raw:
                boundaries |= _boundary_changes(raw[name].reindex(frame.index))
                break
        for name in ("Echem_Step_Time_h", "Step_Time"):
            if name in raw:
                elapsed = pd.to_numeric(raw[name].reindex(frame.index), errors="coerce").to_numpy(dtype=float)
                boundaries |= elapsed[1:] < elapsed[:-1]
                break

    points = np.r_[0, np.flatnonzero(boundaries) + 1, length]
    magnitude = np.abs(current)
    for segment, (start, stop) in enumerate(zip(points[:-1], points[1:])):
        if stop - start < 2 or not direction[start] or mode[start] == 2:
            continue
        if mode[start] == 1:
            segments[start:stop] = segment
            recorded[start:stop] = True
        else:
            selection = _infer_plateau(magnitude[start:stop], voltage[start:stop])
            segments[start + selection.start:start + selection.stop] = segment
    return segments, recorded


def derivative_curves(frame, *, differential_voltage=False, interpolation_points=None):
    """Calculate the requested derivative separately for each retained CC interval."""
    if "cc_segment" in frame:
        segments = frame["cc_segment"].to_numpy()
        recorded = frame["cc_recorded"].to_numpy()
    else:
        segments, recorded = classify_cc(frame)
    fields = ["cycle_number", "voltage_V", "dQdV_charge", "dQdV_discharge", "type", "segment", "cc_source"]
    if differential_voltage:
        fields = ["cycle_number", "discharge_cap_mAh_g", "dVdQ", "segment", "cc_source"]
    results = []
    current = frame["current_mA"].to_numpy(dtype=float)
    cycles = frame["cycle_number"].to_numpy()
    v = frame["voltage_V"].to_numpy(dtype=float)
    charge = frame["charge_cap_mAh_g"].to_numpy(dtype=float) if not differential_voltage else None
    discharge = frame["discharge_cap_mAh_g"].to_numpy(dtype=float)
    points = np.r_[0, np.flatnonzero(segments[1:] != segments[:-1]) + 1, len(frame)]
    for start, stop in zip(points[:-1], points[1:]):
        if stop - start < 2 or segments[start] < 0:
            continue
        charging = current[start] > 0
        if differential_voltage and charging:
            continue
        capacity = (charge if charging else discharge)[start:stop]
        voltage = v[start:stop]
        metadata = {"cycle_number": cycles[start], "segment": segments[start],
                    "cc_source": "recorded" if recorded[start] else "inferred"}
        if interpolation_points is not None and len(voltage) > 3:
            original = np.linspace(0, 1, len(voltage))
            requested = np.linspace(0, 1, interpolation_points)
            voltage = interp1d(original, voltage, kind="cubic")(requested)
            capacity = interp1d(original, capacity, kind="cubic")(requested)
        dq, dv = np.gradient(capacity), np.gradient(voltage)
        if differential_voltage:
            derivative = np.abs(np.divide(dv, dq, out=np.zeros_like(dv), where=np.abs(dq) > 1e-10))
            results.append(pd.DataFrame({**metadata, "discharge_cap_mAh_g": capacity, "dVdQ": derivative}))
        else:
            derivative = np.divide(dq, dv, out=np.zeros_like(dq), where=np.abs(dv) > 1e-10)
            branch = "charge" if charging else "discharge"
            results.append(pd.DataFrame({**metadata, "voltage_V": voltage, f"dQdV_{branch}": derivative, "type": branch}))
    return pd.concat(results, ignore_index=True).reindex(columns=fields) if results else pd.DataFrame(columns=fields)
