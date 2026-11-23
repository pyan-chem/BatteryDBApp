# BatteryDBApp

BatteryDBApp is a PySide6 desktop application for organizing battery test records and analyzing cycling data from Arbin, BioLogic, and Maccor instruments. It stores battery metadata and links to the original instrument files in SQLite.

## Installation

```bash
git clone https://github.com/pyan-chem/BatteryDBApp.git
cd BatteryDBApp
conda create -n BatteryDBApp python=3.11
conda activate BatteryDBApp
python -m pip install -r requirements.txt
```

Run the application from the project directory.

```bash
python main.py
```

The default database is `battery_data.db`. Use **File → Open database…** to open an existing library or **File → New database…** to create one.

## Battery library and analysis

1. Use **New battery** to enter a battery ID, instrument, electrode details, and the path to the instrument file. Enter the positive active mass in grams for capacity normalization. Search by ID, material, date, or file path, and filter by instrument or status. Select a row to see its full details in the right panel.
2. Select a battery and choose **Open analysis**. Hold `Ctrl` or `Shift` to select up to 10 batteries for comparison. The analysis page also has a searchable battery list. Once you have drawn a plot, changing the selected batteries repeats the analysis with the current plot type, cycle selection, and interpolation settings.
3. Choose a plot type and select cycles such as `1,3,5–10`. **All test cycles** skips the pre-cycle or rest segment. Enter `0` explicitly to include that segment if the file contains it.
4. Hover near a plotted point to see its battery ID, X and Y values, and the cycle or charge/discharge branch where applicable. The legend identifies each battery by color. Solid and dashed lines distinguish charge and discharge when both are plotted. The chart toolbar supports zooming, panning, and saving an image.
5. Use **Copy plotted data** for tab-separated plot data. Comparisons include a `Battery ID` column. Select one battery to use **Export processed CSV…** for its processed instrument data.

Available plots include voltage versus time, voltage versus capacity for selected cycles, capacity versus cycle, overvoltage versus cycle, accumulated discharge energy, cycle-to-cycle endpoint slippage, dQ/dV with optional interpolation, and dV/dQ. Overvoltage is the mean charge voltage minus the mean discharge voltage. The accumulated discharge energy calculation is pending validation.

## Read the library from Codex

BatteryDBApp can run as a local, read-only MCP server over standard input/output. It works with the desktop program open or closed and reads the SQLite library and instrument files from your computer.

In the BatteryDBApp Python environment, install the MCP dependencies and register the server with Codex. Replace the example paths with your checkout and database paths. If Codex starts Python outside your BatteryDBApp environment, use that environment's full Python executable path instead of `python`.

```powershell
python -m pip install -r requirements-mcp.txt
codex mcp add batterydb -- python "C:/path/to/BatteryDBApp/battery_mcp.py" --db "D:/lab/battery_data.db"
codex mcp list
```

In Codex desktop, you can also add a **STDIO** server under **Settings → MCP servers**. Use the Python executable as the command and supply the script path, `--db`, and your database path as arguments. Restart the client after adding the server. See the [Codex MCP setup guide](https://learn.chatgpt.com/docs/extend/mcp?surface=cli).

The database must already exist. Instrument files must be accessible from the computer running the server. Relative file paths in the database are resolved against the SQLite library's folder. For libraries whose paths were entered relative to a different working directory, add `--data-root "D:/old/working-directory"` to the server arguments. If you switch libraries in the GUI, update the server's `--db` argument to switch Codex to that library as well.

| Tool | Returns |
| --- | --- |
| `search_batteries` | Battery records matching text, instrument, and status, newest first |
| `get_battery` | All metadata for one exact battery ID |
| `get_available_cycles` | Cycle numbers found in the processed measurements |
| `get_measurements` | Standardized point-by-point time, voltage, current, capacity, and energy |
| `get_instrument_rows` | Instrument-specific rows produced by the existing reader before cleaning and processing |
| `get_plot_data` | Axis and series information with the app's calculated plot data |

For all eight plots, BatteryDBApp reads the linked instrument file and calculates the plot data locally using the same code as the desktop analysis view. The MCP server returns the calculated values, axis labels, and series names so Codex can draw the chart when asked. For example, ask Codex to plot the dQ/dV curve for cycle 10 of a specific battery.

For `get_plot_data`, use `capacity`, `overvoltage`, `accumulated_energy`, `endpoint_slippage`, `voltage_time`, `voltage_capacity`, `dqdv`, or `dvdq`. For example, ask Codex to find LFP cells, compare their first 50 cycles of discharge capacity, and retrieve the voltage–capacity points for cycle 10. The accumulated discharge energy calculation is pending validation.

Measurement and plot tools accept a list of `cycles` or an inclusive `cycle_start`/`cycle_end` range. Plots omit the initial rest (cycle 0) by default, as in the GUI. Measurements include it by default. For dQ/dV, pass `interpolation_points` to enable the same interpolation as in the app. The number of selected cycles times the requested interpolation points must be at most 100,000 per call. Split large requests across cycle ranges.

Follow `next_offset` with the same filters to read the next page. A null offset marks the last page. The server does not downsample measurements. Each request reads the current files, so a running test that updates its instrument file can change the result across pages. For analysis and standardized measurements, enter the positive active mass in grams in the battery record. Instrument-reader rows can be retrieved without a mass.

Local file reading and calculations use no model tokens. Data returned to Codex uses model context. Request per-cycle metrics for comparisons and select the cycles you need when reading individual points. Reading every page still sends the full result to Codex.

## Automatic file status

On startup and when opening a database, BatteryDBApp checks the linked instrument files in the background. Each writable database stores its own baseline.

| Status | Meaning |
| --- | --- |
| `Update` | The file size or modification time changed since the previous check, or a previously missing file is now available. |
| `Stopped` | The check established the first baseline, found the same metadata as before, or could not access the file. |

The check reads file metadata once per distinct path. Its cost depends mainly on the number of linked files and filesystem latency. Edits that preserve both size and modification time are treated as unchanged. For a temporarily unavailable file, the previous successful observation is retained. A read-only database can display the current scan result for the session. Saving a new baseline requires a writable database.

## Supported instruments

| Instrument | Input | Reader |
| --- | --- | --- |
| BioLogic | `.mpr` | yadg with a galvani fallback |
| Arbin | `.res` | galvani with mdbtools or a pyodbc fallback with a Microsoft Access ODBC driver |
| Maccor | `.txt`, `.xyz`, or a numeric extension such as `.001` | Tab-separated text |

## Development

- `main.py` launches the application. `database.py` manages SQLite records and file status baselines.
- `data_import/` reads instrument data and checks linked file metadata.
- `analysis/` processes instrument data and calculates cycling metrics.
- `ui/` contains the battery library, analysis page, and point inspection.
- `tests/` covers data processing, SQLite operations, and application workflows.

Install pytest to run the full test suite.

```bash
python -m pip install pytest
python -m pytest
```

The benchmark reports import, cleaning, and processing times separately. Run `python scripts/benchmark_data_processing.py --help` for instrument-file arguments and repeat counts.
