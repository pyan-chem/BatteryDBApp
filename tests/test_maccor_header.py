import pytest

from analysis.data_loading import load_and_process_cell
from data_import.importer import DataImporter


@pytest.mark.parametrize(("preamble", "header"), [
    ("", "Filename\tRec#\tCycle P\tTest Time (sec)\tCapacity (AHr)\tEnergy (WHr)\tCurrent (A)\tVoltage (V)\tMD"),
    ("Maccor export\n", "Filename\tRec#\tCyc#\tTest (Sec)\tAmp-hr\tWatt-hr\tAmps\tVolts\tState"),
])
def test_maccor_reads_first_measurement_with_or_without_preamble(tmp_path, preamble, header):
    path = tmp_path / "test.025"
    path.write_text(
        preamble + header + "\n"
        "test.025\t1\t0\t0\t0\t0\t0\t0.11757\tR\n"
        "test.025\t2\t1\t3600\t0.002\t0.004\t0.001\t3.5\tC\n",
        encoding="utf-8",
    )

    imported = DataImporter.import_data(str(path), "Maccor")
    assert imported["Echem_Rec#"].tolist() == [1, 2]
    processed = load_and_process_cell(str(path), "Maccor", 0.01)
    assert processed["cycle_number"].tolist() == [0, 1]
    assert processed["time_h"].tolist() == [0.0, 1.0]
    assert processed["voltage_V"].tolist() == [0.11757, 3.5]
    assert processed["charge_cap_mAh_g"].tolist() == [0.0, 200.0]


def test_unrecognized_maccor_header_reports_an_import_error(tmp_path):
    path = tmp_path / "invalid.025"
    path.write_text("Description\nunknown\tcolumns\n10\t20\n", encoding="utf-8")

    with pytest.raises(Exception, match="Maccor column header"):
        DataImporter.import_data(str(path), "Maccor")
