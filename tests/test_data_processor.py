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
