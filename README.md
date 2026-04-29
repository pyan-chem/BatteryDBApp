# BatteryDBApp

A desktop application for processing, analyzing, and visualizing electrochemical test data from battery cycling equipment.

## Features

### Multi-Instrument Support
- **Biologic** (.mpr binary files) – via galvani library
- **Arbin** (.res MDB files) – via pyodbc or galvani fallback
- **Maccor** (.txt, .xyz, numeric extensions) – tab-separated format

### Data Processing
- Data cleaning and processing
- Visualization and analysis
  - Overvoltage (charge mean voltage – discharge mean voltage)
  - Accumulated discharge energy
  - dQ/dV
  - dV/dQ
  - Cycle-to-cycle endpointslippage
  - 
### Database
- SQLite3 backend (`battery_data.db`)
- Cell metadata storage (electrode materials, active masses, test parameters)
- Tight integration with PySide6 GUI for data persistence

### Visualization
- **Analysis Window**: Interactive matplotlib plots for cycle filtering
- **Supported Plots**: Voltage vs. capacity, overvoltage vs. cycle, capacity fade tracking
- **Real-Time Filtering**: Filter by cycle range within the analysis dialog

### Performance Monitoring
- Included benchmark script (`scripts/benchmark_data_processing.py`)
- Measures import, clean, and process stages independently
- Multi-run averaging with configurable file paths

## Project Structure

```
BatteryDBApp/
├── main.py                    # Application entry point
├── database.py                # SQLite database handler
├── requirements.txt           # Python dependencies + versions
├── battery_manager.log        # Runtime logs
├── analysis/
│   ├── data_processor.py      # Core processing: 3 instrument processors
│   └── plotter.py             # Matplotlib utilities
├── data_import/
│   └── importer.py            # File readers (Biologic, Arbin, Maccor)
├── ui/
│   ├── main_window.py         # Main Qt6 window, threading, file dialogs
│   └── analysis_window.py     # Analysis/plotting dialog
├── scripts/
│   └── benchmark_data_processing.py  # Standalone performance measurement tool
└── tests/
    └── [test files]
```

## Installation

1. **Create virtual environment**:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate      # Windows

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Run

```bash
python BatteryDBApp/main.py

