import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from analysis.data_loading import clean_data, load_and_process_cell
from analysis.data_processor import DataProcessor
from data_import.importer import DataImporter


@pytest.mark.parametrize("records, expected_reads", [
    ("1\t1\t0\t0\t0\t.001\t3.1\tC\t20\n2\t1\t1\t.002\t0\t.001\t3.5\tC\t21\n", [True]),
    ("1\t1\t0\t0\t0\t.001\t3.1\tC\t20\n1\t1\t0\t0\t0\t.001\t3.1\tC\t21\n", [True, False]),
    ("\t1\t0\t0\t0\t.001\t3.1\tC\t20\n\t1\t0\t0\t0\t.001\t3.1\tC\t20\n", [True, False]),
])
def test_maccor_analysis_projection_preserves_full_import_results(tmp_path, monkeypatch, records, expected_reads):
    path = tmp_path / "test.025"
    path.write_text(
        "Export metadata\n"
        "Rec#\tCycle P\tTest Time (sec)\tCapacity (AHr)\tEnergy (WHr)\tCurrent (A)\tVoltage (V)\tMD\tAux #1\n"
        + records, encoding="utf-8",
    )
    raw = DataImporter.import_data(path, "Maccor")
    assert "Echem_Aux #1" in raw
    expected = DataProcessor.process_maccor_data(clean_data(raw), 0.01)
    reads = []
    read_csv = pd.read_csv

    def observe_read(*args, **kwargs):
        reads.append("usecols" in kwargs)
        return read_csv(*args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", observe_read)
    actual = load_and_process_cell(path, "Maccor", 0.01)
    assert_frame_equal(actual, expected, check_exact=True)
    assert reads == expected_reads
