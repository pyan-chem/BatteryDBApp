from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from statistics import mean

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_import.importer import DataImporter
from analysis.data_processor import DataProcessor


DEFAULT_FILES = {
    "Biologic": Path(r"xxx.mpr"),
    "Arbin": None,
    "Maccor": Path(r"xxx.xyz"),
}

PROCESSOR_MAP = {
    "Biologic": DataProcessor.process_biologic_data,
    "Arbin": DataProcessor.process_arbin_data,
    "Maccor": DataProcessor.process_maccor_data,
}


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    df_clean = df_clean.dropna(how="all")
    df_clean = df_clean.drop_duplicates()
    df_clean = df_clean.reset_index(drop=True)
    return df_clean


def resolve_path(value: str | None, instrument: str) -> Path | None:
    if value:
        return Path(value)
    env_key = f"BATTERY_BENCHMARK_{instrument.upper()}_FILE"
    env_value = os.environ.get(env_key)
    if env_value:
        return Path(env_value)
    default_value = DEFAULT_FILES[instrument]
    if isinstance(default_value, Path):
        return default_value
    return None


def benchmark_file(instrument: str, file_path: Path, p_active_mass: float, runs: int = 3) -> dict:
    import_times = []
    clean_times = []
    process_times = []
    total_times = []
    raw_rows = []
    cleaned_rows = []
    processed_rows = []

    for _ in range(runs):
        start_total = time.perf_counter()

        start_import = time.perf_counter()
        raw_df = DataImporter.import_data(str(file_path), instrument)
        import_times.append(time.perf_counter() - start_import)
        raw_rows.append(len(raw_df))

        start_clean = time.perf_counter()
        cleaned_df = clean_data(raw_df)
        clean_times.append(time.perf_counter() - start_clean)
        cleaned_rows.append(len(cleaned_df))

        start_process = time.perf_counter()
        processed_df = PROCESSOR_MAP[instrument](cleaned_df, p_active_mass)
        process_times.append(time.perf_counter() - start_process)
        processed_rows.append(len(processed_df))

        total_times.append(time.perf_counter() - start_total)

    return {
        "instrument": instrument,
        "file": file_path.name,
        "path": str(file_path),
        "raw_rows": int(mean(raw_rows)),
        "cleaned_rows": int(mean(cleaned_rows)),
        "processed_rows": int(mean(processed_rows)),
        "import_s": mean(import_times),
        "clean_s": mean(clean_times),
        "process_s": mean(process_times),
        "total_s": mean(total_times),
    }


def format_table(rows: list[dict]) -> str:
    headers = [
        "Instrument",
        "File",
        "Raw Rows",
        "Clean Rows",
        "Processed Rows",
        "Import (s)",
        "Clean (s)",
        "Process (s)",
        "Total (s)",
    ]

    table_rows = [
        [
            row["instrument"],
            row["file"],
            f"{row['raw_rows']:,}",
            f"{row['cleaned_rows']:,}",
            f"{row['processed_rows']:,}",
            f"{row['import_s']:.4f}",
            f"{row['clean_s']:.4f}",
            f"{row['process_s']:.4f}",
            f"{row['total_s']:.4f}",
        ]
        for row in rows
    ]

    widths = [len(h) for h in headers]
    for row in table_rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def render_row(values: list[str]) -> str:
        return " | ".join(value.ljust(widths[index]) for index, value in enumerate(values))

    separator = "-+-".join("-" * width for width in widths)
    lines = [render_row(headers), separator]
    lines.extend(render_row(row) for row in table_rows)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark battery data processing stages.")
    parser.add_argument("--biologic", help="Path to a Biologic .mpr file")
    parser.add_argument("--arbin", help="Path to an Arbin .res file")
    parser.add_argument("--maccor", help="Path to a Maccor file")
    parser.add_argument("--p-active-mass", type=float, default=50.0, help="Positive active mass in mg")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs per file")
    args = parser.parse_args()

    file_specs = [
        ("Biologic", resolve_path(args.biologic, "Biologic")),
        ("Arbin", resolve_path(args.arbin, "Arbin")),
        ("Maccor", resolve_path(args.maccor, "Maccor")),
    ]

    results = []
    missing = []
    for instrument, file_path in file_specs:
        if file_path is None:
            missing.append(instrument)
            continue
        if not file_path.exists():
            missing.append(f"{instrument} ({file_path})")
            continue
        results.append(benchmark_file(instrument, file_path, args.p_active_mass, args.runs))

    if missing:
        print("Skipped missing inputs:")
        for item in missing:
            print(f"- {item}")
        print()

    if not results:
        print("No benchmarkable files were found.")
        return 1

    print(format_table(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
