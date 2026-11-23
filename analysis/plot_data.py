"""BatteryDBApp plot definitions and calculations independent of the GUI."""

from dataclasses import dataclass

from analysis.data_processor import DataProcessor


@dataclass(frozen=True)
class PlotDefinition:
    x: str
    x_label: str
    y_label: str
    series: tuple
    export_columns: tuple
    processor: object = None
    per_cycle: bool = False


PLOTS = {
    "Voltage vs. Time": PlotDefinition(
        "time_h", "Time (h)", "Voltage (V)", (("voltage_V", "Voltage"),),
        ("time_h", "voltage_V"), per_cycle=True,
    ),
    "Cycling vs. Capacity": PlotDefinition(
        "cycle_number", "Cycle number", "Capacity (mAh/g)",
        (("charge_cap_mAh_g", "Charge"), ("discharge_cap_mAh_g", "Discharge")),
        ("cycle_number", "charge_cap_mAh_g", "discharge_cap_mAh_g"), DataProcessor.cycling_vs_capacity,
    ),
    "Cycling vs. Accumulated Energy": PlotDefinition(
        "cycle_number", "Cycle number", "Accumulated energy (Wh)", (("Accu_Energy_Wh", "Energy"),),
        ("cycle_number", "Accu_Energy_Wh"), DataProcessor.cycling_vs_accumulated_energy,
    ),
    "dQ/dV": PlotDefinition(
        "voltage_V", "Voltage (V)", "dQ/dV (mAh/g/V)",
        (("dQdV_charge", "Charge"), ("dQdV_discharge", "Discharge")),
        ("voltage_V", "dQdV_charge", "dQdV_discharge"), per_cycle=True,
    ),
    "dV/dQ": PlotDefinition(
        "discharge_cap_mAh_g", "Discharge capacity (mAh/g)", "dV/dQ (V g/mAh)",
        (("dVdQ", "Discharge"),), ("cycle_number", "discharge_cap_mAh_g", "dVdQ"),
        DataProcessor.dvdq, per_cycle=True,
    ),
    "Voltage vs. Capacity cyclic": PlotDefinition(
        "(Q-Qo)_mAh_g", "Capacity (Q − Q₀) (mAh/g)", "Voltage (V)", (("voltage_V", "Voltage"),),
        ("cycle_number", "voltage_V", "(Q-Qo)_mAh_g"), DataProcessor.voltage_vs_capacity_cyclic,
        per_cycle=True,
    ),
    "Cycling vs. Overvoltage": PlotDefinition(
        "cycle_number", "Cycle number", "Overvoltage (V)", (("Overvoltage", "Overvoltage"),),
        ("cycle_number", "Overvoltage"), DataProcessor.cycling_vs_overvoltage,
    ),
    "Cycling vs. Endpointslippage": PlotDefinition(
        "cycle_number", "Cycle number", "Endpoint slippage (mAh/g)",
        (("discharge_endpoint", "Discharge"), ("charge_endpoint", "Charge")),
        ("cycle_number", "discharge_endpoint", "charge_endpoint"), DataProcessor.cycling_vs_endpointslippage,
    ),
}


def analyze_plot(sample, cycles, analysis_type, definition, *, interpolation_points=None):
    selected = sample[sample["cycle_number"].isin(cycles)]
    if selected.empty:
        return selected
    if analysis_type == "dQ/dV":
        return DataProcessor.dqdv(
            selected, interpolate_flag=interpolation_points is not None,
            interp_points=interpolation_points if interpolation_points is not None else 1000,
        )
    if definition.processor:
        # Endpoint slippage needs the history of all preceding cycles.
        data = definition.processor(sample)
        return data[data["cycle_number"].isin(cycles)]
    return selected
