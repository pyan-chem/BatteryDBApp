"""Shared instrument import, cleaning, and processing for desktop and MCP."""

import logging
import time

from analysis.data_processor import DataProcessor
from data_import.importer import DataImporter


logger = logging.getLogger(__name__)
PROCESSORS = {
    "Biologic": DataProcessor.process_biologic_data,
    "Arbin": DataProcessor.process_arbin_data,
    "Maccor": DataProcessor.process_maccor_data,
}


def clean_data(frame):
    """Remove blank and duplicate rows and reset the row index."""
    return frame.dropna(how="all").drop_duplicates().reset_index(drop=True)


def load_and_process_cell(file_path, instrument_type, p_active_mass):
    """Return the app's standardized measurements using positive active mass in grams."""
    if p_active_mass <= 0:
        raise ValueError("P active mass must be greater than 0")
    if instrument_type not in PROCESSORS:
        raise ValueError(f"Unknown instrument type: {instrument_type}")

    started = time.perf_counter()
    if instrument_type in {"Maccor", "Biologic"}:
        raw = DataImporter.import_data(file_path, instrument_type, for_analysis=True)
    else:
        raw = DataImporter.import_data(file_path, instrument_type)
    imported = time.perf_counter()
    cleaned = clean_data(raw)
    cleaned_at = time.perf_counter()
    processed = PROCESSORS[instrument_type](cleaned, p_active_mass)
    logger.info(
        "%s data: import %.3fs (%d rows), clean %.3fs (%d rows), process %.3fs (%d rows)",
        instrument_type, imported - started, len(raw), cleaned_at - imported, len(cleaned),
        time.perf_counter() - cleaned_at, len(processed),
    )
    return processed
