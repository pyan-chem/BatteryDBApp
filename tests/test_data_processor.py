import pandas as pd
import numpy as np
from analysis.data_processor import DataProcessor


def test_process_arbin_data_basic():
    # Construct a small DataFrame with minimal necessary columns
    df = pd.DataFrame({
        'Data_Point': [1, 2, 3],
        'Test_Time': [0, 3600, 7200],
        'Cycle_Index': [1, 1, 2],
        'Current': [0.5, -0.2, 0.1],
        'Voltage': [3.7, 3.6, 3.8],
        'Charge_Capacity': [0.1, 0.15, 0.2],
        'Discharge_Capacity': [0.05, 0.1, 0.12],
        'Charge_Energy': [0.01, 0.02, 0.03],
        'Discharge_Energy': [0.005, 0.015, 0.02]
    })

    processed = DataProcessor.process_arbin_data(df, p_active_mass=50.0)

    # Basic shape checks
    assert 'time_h' in processed.columns
    assert 'cycle_number' in processed.columns
    assert len(processed) == len(df)

    # Check numeric conversions
    assert processed['time_h'].iloc[1] == 1.0  # 3600 seconds -> 1 hour
    assert processed['current_mA'].iloc[0] == 500.0


def test_process_maccor_data_basic():
    df = pd.DataFrame({
        'Echem_Test_Time_h': [0.0, 0.5, 1.0],
        'Echem_Voltage_V': [3.7, 3.8, 3.85],
        'Echem_Current_A': [0.1, -0.05, 0.0],
        'Echem_Capacity_Ah': [0.05, 0.06, 0.07],
        'Echem_Mode': ['C', 'D', 'C'],
        'Echem_Energy_Wh': [0.01, 0.02, 0.03]
    })

    processed = DataProcessor.process_maccor_data(df, p_active_mass=10.0)

    assert 'time_h' in processed.columns
    assert 'current_mA' in processed.columns
    assert processed['current_mA'].iloc[0] > 0
    assert processed['charge_cap_mAh_g'].sum() >= 0
def test_maccor_trajectory_keeps_cycle_endpoints_and_writes_in_bulk(monkeypatch):
    frame = pd.DataFrame(
        {
            "Echem_Test_Time_h": [0, 2, 1, 4, 3, 3.5, 6, 5, 7, 8],
            "Echem_Cycle": [1, 1, 1, 1, 1, 1, 2, 2, 3, 4],
            "Echem_Mode": ["R", "C", "C", "D", "D", "R", "C", "C", "D", "C"],
            "Echem_Capacity_Ah": [0, .004, .002, .003, .001, None, None, .002, .001, .001],
            "Echem_Current_A": [0, .001, .001, .001, .001, 0, .001, .001, .001, .001],
            "Echem_Voltage_V": [3.5] * 10,
            "Echem_Energy_Wh": [0] * 10,
        },
        index=[10, 11, 14, 18, 20, 25, 31, 43, 52, 60],
    )
    loc_type = type(frame.loc)
    original_setitem = loc_type.__setitem__
    trajectory_writes = []

    def track_trajectory_write(indexer, key, value):
        if isinstance(key, tuple) and len(key) == 2 and key[1] == "(Q-Qo)_mAh_g":
            trajectory_writes.append(key[1])
        return original_setitem(indexer, key, value)

    monkeypatch.setattr(loc_type, "__setitem__", track_trajectory_write)
    result = DataProcessor.process_maccor_data(frame, p_active_mass=1.0)

    assert result.index.tolist() == [10, 14, 11, 20, 25, 18, 43, 31, 52, 60]
    assert result["cycle_number"].tolist() == [0, 1, 1, 1, 1, 1, 2, 2, 3, 4]
    assert result["(Q-Qo)_mAh_g"].tolist() == [0, 2, 4, 3, 0, 1, 3, 1, 2, 3]
    assert trajectory_writes == []


