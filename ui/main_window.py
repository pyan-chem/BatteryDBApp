from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QTableWidget, QTableWidgetItem, QLabel, QLineEdit,
                             QPushButton, QFormLayout, QComboBox, QDateEdit,
                             QHeaderView, QSplitter, QGroupBox, QFileDialog,
                             QCheckBox, QMessageBox, QMenu, QDialog,
                             QDialogButtonBox)
from PySide6.QtCore import Qt, QDate, QSettings, QObject, QThread, Signal
from PySide6.QtGui import QAction
from database import BatteryDatabase
import sys
import os
import logging
import time

# Get logger for this module
logger = logging.getLogger(__name__)

# Add parent directory to path to import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_import.importer import DataImporter
from analysis.data_processor import DataProcessor
from ui.analysis_window import AnalysisWindow


class TableColumns:
    """Table column indices for database cells table."""
    ID = 0
    INSTRUMENT = 1
    DATE = 2
    NEG_ELECTRODE = 3
    POS_ELECTRODE = 4
    N_MASS = 5
    P_MASS = 6
    ELECTROLYTE = 7
    FORMAT = 8
    VOLTAGE_RANGE = 9
    COMMENT = 10
    STATUS = 11
    FILE_PATH = 12


class DataLoadWorker(QObject):
    finished = Signal(object)
    failed_detailed = Signal(dict)  # Detailed error info: {"type", "message", "suggestion"}

    def __init__(self, load_func, file_path, instrument_type, p_active_mass, cell_id, action):
        super().__init__()
        self._load_func = load_func
        self.file_path = file_path
        self.instrument_type = instrument_type
        self.p_active_mass = p_active_mass
        self.cell_id = cell_id
        self.action = action

    def run(self):
        try:
            processed_data = self._load_func(
                self.file_path,
                self.instrument_type,
                self.p_active_mass,
            )
            self.finished.emit({
                "action": self.action,
                "cell_id": self.cell_id,
                "instrument_type": self.instrument_type,
                "file_path": self.file_path,
                "p_active_mass": self.p_active_mass,
                "processed_data": processed_data,
            })
        except FileNotFoundError as e:
            # Missing file error
            logger.error(
                f"Background data load failed for cell {self.cell_id}: {e}",
                exc_info=True,
            )
            error_info = {
                "type": "FileNotFoundError",
                "message": f"File not found: {self.file_path}",
                "suggestion": "Please check that the file path is correct and the file exists."
            }
            self.failed_detailed.emit(error_info)
        except ValueError as e:
            # File format or instrument type error
            logger.error(
                f"Background data load failed for cell {self.cell_id}: {e}",
                exc_info=True,
            )
            error_str = str(e)
            if "format" in error_str.lower() or "extension" in error_str.lower():
                error_info = {
                    "type": "FileFormatError",
                    "message": error_str,
                    "suggestion": f"Please check that {os.path.basename(self.file_path)} is a valid {self.instrument_type} file format."
                }
            elif "instrument" in error_str.lower():
                error_info = {
                    "type": "InstrumentTypeError",
                    "message": f"Unsupported instrument type: {self.instrument_type}",
                    "suggestion": "Please select from: Arbin, Biologic, or Maccor."
                }
            else:
                error_info = {
                    "type": "ValueError",
                    "message": error_str,
                    "suggestion": None
                }
            self.failed_detailed.emit(error_info)
        except Exception as e:
            # Generic error
            logger.error(
                f"Background data load failed for cell {self.cell_id}: {e}",
                exc_info=True,
            )
            error_info = {
                "type": type(e).__name__,
                "message": str(e),
                "suggestion": "An unexpected error occurred. Please check the application logs for details."
            }
            self.failed_detailed.emit(error_info)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        logger.info("Initializing MainWindow")
        self.setWindowTitle("Battery Data Manager")
        self.resize(1400, 800)
        
        try:
            self.db = BatteryDatabase()
            self.settings = QSettings("BatteryDBApp", "BatteryDataManager")
            self.processed_data = None  # Current processed data
            self.current_cell_id = None  # Current cell ID
            self.preload_cache = {}  # Dictionary to cache preloaded data: {cell_id: processed_data}
            self.preload_cache_limit = 10  # Max number of cells to cache
            # Load last file directory from persistent settings
            self.last_dir = self.settings.value("last_file_directory", "", type=str)
            
            self.central_widget = QWidget()
            self.setCentralWidget(self.central_widget)
            self.main_layout = QHBoxLayout(self.central_widget)
            self.main_layout.setContentsMargins(8, 8, 8, 8)
            self.main_layout.setSpacing(8)
            
            # Initialize status bar
            self.status_bar = self.statusBar()
            self.status_bar.showMessage("Ready", 0)
            
            self.init_ui()
            logger.debug("UI initialized")
            self.init_menu()
            logger.debug("Menu initialized")
            self._set_window_title()
            self.restore_last_database()
            self.load_data()
            # Restore window geometry (size and position)
            self.restore_window_geometry()
            logger.info("MainWindow initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing MainWindow: {e}", exc_info=True)
            raise

    def restore_last_database(self):
        """Restore last opened database on startup."""
        try:
            last_db = self.settings.value("last_database_path", "", type=str)
            if last_db and os.path.exists(last_db):
                try:
                    self.db.switch_database(last_db)
                    self.last_dir = os.path.dirname(last_db)
                    logger.info(f"Restored last database: {last_db}")
                except Exception as e:
                    logger.warning(f"Failed to restore last database: {e}")
            elif last_db:
                self.settings.remove("last_database_path")
                logger.warning(f"Last database no longer exists: {last_db}")
        except Exception as e:
            logger.error(f"Error restoring last database: {e}")

    def set_last_database_path(self, path):
        """Persist last opened database path."""
        if path:
            try:
                abs_path = os.path.abspath(path)
                self.settings.setValue("last_database_path", abs_path)
                self.last_dir = os.path.dirname(abs_path)
                self.settings.setValue("last_file_directory", self.last_dir)
                logger.debug(f"Set last database path: {abs_path}")
            except Exception as e:
                logger.error(f"Error setting last database path: {e}")

    def restore_window_geometry(self):
        """Restore window size and position from settings."""
        try:
            geometry = self.settings.value("window_geometry", None, type=bytes)
            state = self.settings.value("window_state", None, type=bytes)
            
            if geometry:
                self.restoreGeometry(geometry)
                logger.debug("Restored window geometry from settings")
            
            if state:
                self.restoreState(state)
                logger.debug("Restored window state from settings")
        except Exception as e:
            logger.warning(f"Could not restore window geometry: {e}")
            # Window keeps the default size (1400, 800) from __init__

    def save_window_geometry(self):
        """Save window size and position to settings."""
        try:
            self.settings.setValue("window_geometry", self.saveGeometry())
            self.settings.setValue("window_state", self.saveState())
            logger.debug("Saved window geometry to settings")
        except Exception as e:
            logger.warning(f"Could not save window geometry: {e}")

    def _set_window_title(self):
        """Update the window title to include the current loaded database."""
        base_title = "Battery Data Manager"
        if hasattr(self, 'db') and self.db.db_name:
            db_filename = os.path.basename(self.db.db_name)
            self.setWindowTitle(f"{base_title} - {db_filename}")
        else:
            self.setWindowTitle(base_title)

    def _set_status_message(self, message, duration_ms=5000):
        """Display a status message in the status bar with optional timeout."""
        self.status_bar.showMessage(message, duration_ms)

    def show_error(self, message):
        """Show an error dialog to the user."""
        QMessageBox.critical(self, "Error", message)

    def show_warning(self, message):
        """Show a warning dialog to the user."""
        QMessageBox.warning(self, "Warning", message)

    def show_info(self, message):
        """Show an info dialog to the user."""
        QMessageBox.information(self, "Success", message)

    def log_and_show_error(self, message, exception=None):
        """Log an error and show an error dialog to the user."""
        if exception:
            logger.error(f"{message}: {exception}", exc_info=True)
            self.show_error(f"{message}: {exception}")
        else:
            logger.error(message)
            self.show_error(message)

    def _set_data_loading_state(self, is_loading):
        """Enable or disable controls affected by background data loading."""
        self.preload_btn.setEnabled(not is_loading)
        self.plot_btn.setEnabled(not is_loading)
        self.save_processed_btn.setEnabled(not is_loading and self.processed_data is not None)

    def _start_data_load_worker(self, action, cell_id, file_path, instrument_type, p_active_mass):
        """Start background loading and processing for the selected cell."""
        existing_thread = getattr(self, "_data_load_thread", None)
        if existing_thread is not None:
            if existing_thread.isRunning():
                logger.warning("A data load is already in progress")
                return

        logger.info(
            f"Starting background load for cell {cell_id} using {instrument_type} from {file_path}"
        )
        self._set_data_loading_state(True)
        
        # Show loading status with file name
        file_name = os.path.basename(file_path)
        self._set_status_message(f"Loading {file_name}... ({cell_id})", 0)

        self._data_load_thread = QThread(self)
        self._data_load_worker = DataLoadWorker(
            self._load_and_process_cell,
            file_path,
            instrument_type,
            p_active_mass,
            cell_id,
            action,
        )
        self._data_load_worker.moveToThread(self._data_load_thread)

        self._data_load_thread.started.connect(self._data_load_worker.run)
        self._data_load_worker.finished.connect(self._handle_data_load_success)
        self._data_load_worker.failed_detailed.connect(self._handle_data_load_failure_detailed)
        self._data_load_worker.finished.connect(self._data_load_thread.quit)
        self._data_load_worker.failed_detailed.connect(self._data_load_thread.quit)
        self._data_load_thread.finished.connect(self._data_load_worker.deleteLater)
        self._data_load_thread.finished.connect(self._data_load_thread.deleteLater)
        self._data_load_thread.finished.connect(self._clear_data_load_worker_refs)

        self._data_load_thread.start()

    def _format_error_message(self, error_info):
        """Format error information into a user-friendly message."""
        error_type = error_info.get("type", "Error")
        message = error_info.get("message", "An unknown error occurred")
        suggestion = error_info.get("suggestion")
        
        formatted = f"{message}"
        if suggestion:
            formatted += f"\n\nSuggestion: {suggestion}"
        
        return formatted

    def _clear_data_load_worker_refs(self):
        self._data_load_thread = None
        self._data_load_worker = None

    def _handle_data_load_success(self, payload):
        """Apply loaded data on the main thread."""
        self._set_data_loading_state(False)

        action = payload["action"]
        cell_id = payload["cell_id"]
        instrument_type = payload["instrument_type"]
        processed_data = payload["processed_data"]
        file_name = os.path.basename(payload.get("file_path", "<unknown>"))

        self.processed_data = processed_data
        self.current_cell_id = cell_id
        
        # Show success status message
        self._set_status_message(f"✓ Loaded {file_name} ({len(processed_data)} rows)", 5000)

        if action == "preload":
            self.preload_cache[cell_id] = processed_data

            if len(self.preload_cache) > self.preload_cache_limit:
                oldest_cell_id = next(iter(self.preload_cache))
                del self.preload_cache[oldest_cell_id]
                logger.debug(f"Cache limit exceeded, removed oldest cell: {oldest_cell_id}")

            self.save_processed_btn.setEnabled(True)
            self.update_preload_checkbox()

            logger.info(
                f"Data preloaded successfully for cell: {cell_id} ({len(processed_data)} rows)"
            )
            msg = (f"Data preloaded successfully!\\n"
                   f"Cell: {cell_id}\\n"
                   f"Instrument: {instrument_type}\\n"
                   f"Processed rows: {len(processed_data)}\\n"
                   f"Cached cells: {list(self.preload_cache.keys())}")
            self.show_info(msg)
            return

        if action == "analysis":
            self.save_processed_btn.setEnabled(True)
            self.update_preload_checkbox()

            analysis_window = AnalysisWindow(self, processed_data, cell_id)
            analysis_window.exec()
            logger.info(f"Analysis window closed for cell: {cell_id}")
            return

        logger.warning(f"Unknown data load action: {action}")

    def _handle_data_load_failure_detailed(self, error_info):
        """Restore UI state after a background load failure and show detailed error message."""
        self._set_data_loading_state(False)
        
        # Format error message with user-friendly details
        formatted_message = self._format_error_message(error_info)
        error_type = error_info.get("type", "Error")
        
        # Show failure status message
        self._set_status_message(f"✗ Failed to load data", 5000)
        
        # Show detailed error dialog
        self.show_error(formatted_message)

    def init_ui(self):
        # Splitter to separate table and inputs
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setHandleWidth(6)
        self.main_layout.addWidget(splitter)
        
        # --- Left Side: Data Table ---
        self.table_widget = QTableWidget()
        self.table_widget.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_widget.customContextMenuRequested.connect(self.show_table_context_menu)
        # Set selection mode to select entire rows
        self.table_widget.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table_widget.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        # Connect selection change to update preload checkbox
        self.table_widget.selectionModel().selectionChanged.connect(self.update_preload_checkbox)
        self.table_widget.setAlternatingRowColors(True)
        self.table_widget.setWordWrap(True)
        splitter.addWidget(self.table_widget)
        
        # --- Right Side: Input Form ---
        right_panel = QGroupBox("New cell")
        right_layout = QVBoxLayout()
        right_layout.setSpacing(8)
        right_panel.setLayout(right_layout)
        right_panel.setMinimumWidth(320)
        right_panel.setMaximumWidth(420)
        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([1000, 360])
        
        # Form inputs
        self.form_layout = QFormLayout()
        
        self.id_input = QLineEdit()
        self.instrument_input = QComboBox()
        self.instrument_input.addItems(["Arbin", "Biologic", "Maccor"])
        self.date_input = QDateEdit(QDate.currentDate())
        self.neg_electrode_input = QLineEdit()
        
        self.pos_electrode_input = QLineEdit()
        
        self.n_mass_input = QLineEdit()
        self.p_mass_input = QLineEdit()
        
        self.electrolyte_input = QLineEdit()
        
        self.voltage_range_input = QLineEdit()
        self.file_path_input = QLineEdit()
        self.browse_btn = QPushButton("...")
        self.browse_btn.clicked.connect(self.browse_file)
        file_layout = QHBoxLayout()
        file_layout.addWidget(self.file_path_input)
        file_layout.addWidget(self.browse_btn)

        self.format_input = QComboBox()
        self.format_input.addItems(["Coin", "Pouch", "Swagelok", "Other"])
        
        self.status_chk = QCheckBox("Active")
        self.status_chk.setChecked(True)
        
        self.comment_input = QLineEdit()

        # Add rows to form
        self.form_layout.addRow("ID:", self.id_input)
        self.form_layout.addRow("Instrument:", self.instrument_input)
        self.form_layout.addRow("Date:", self.date_input)
        self.form_layout.addRow("Negative electrode:", self.neg_electrode_input)
        self.form_layout.addRow("Positive electrode:", self.pos_electrode_input)
        self.form_layout.addRow("N active mass (mg):", self.n_mass_input)
        self.form_layout.addRow("P active mass (mg):", self.p_mass_input)
        self.form_layout.addRow("Electrolyte:", self.electrolyte_input)
        self.form_layout.addRow("Voltage range:", self.voltage_range_input)
        self.form_layout.addRow("Format:", self.format_input)
        self.form_layout.addRow("File:", file_layout)
        self.form_layout.addRow("Status:", self.status_chk)
        self.form_layout.addRow("Comment:", self.comment_input)
        
        right_layout.addLayout(self.form_layout)
        
        # Buttons
        btn_layout = QHBoxLayout()
        self.create_btn = QPushButton("Create cell")
        self.create_btn.clicked.connect(self.create_cell_from_form)
        self.clear_btn = QPushButton("Clear fields")
        self.clear_btn.clicked.connect(self.clear_form)
        
        btn_layout.addWidget(self.create_btn)
        btn_layout.addWidget(self.clear_btn)
        right_layout.addLayout(btn_layout)
        
        # Data Actions
        action_group = QGroupBox("Data")
        action_layout = QVBoxLayout()
        action_group.setLayout(action_layout)
        
        # Preload with checkbox
        preload_layout = QHBoxLayout()
        self.preload_btn = QPushButton("Preload")
        self.preload_btn.clicked.connect(self.preload_data)
        self.preload_chk = QCheckBox("Preloaded")
        self.preload_chk.setEnabled(False)  # Read-only indicator
        preload_layout.addWidget(self.preload_btn)
        preload_layout.addWidget(self.preload_chk)
        preload_layout.addStretch()
        action_layout.addLayout(preload_layout)
        
        self.plot_btn = QPushButton("Data Analysis / Plot")
        self.plot_btn.clicked.connect(self.open_analysis)
        action_layout.addWidget(self.plot_btn)
        
        self.save_processed_btn = QPushButton("Save Processed Data")
        self.save_processed_btn.clicked.connect(self.save_processed_data)
        self.save_processed_btn.setEnabled(False)  # Disabled until data is processed
        action_layout.addWidget(self.save_processed_btn)
        
        right_layout.addWidget(action_group)
        
        right_layout.addStretch()

    def init_menu(self):
        menu_bar = self.menuBar()

        file_menu = menu_bar.addMenu("File")
        edit_menu = menu_bar.addMenu("Edit")
        help_menu = menu_bar.addMenu("Help")

        new_action = QAction("New", self)
        new_action.triggered.connect(self.new_database)
        file_menu.addAction(new_action)

        open_action = QAction("Open", self)
        open_action.triggered.connect(self.open_database)
        file_menu.addAction(open_action)

        save_action = QAction("Save", self)
        save_action.triggered.connect(self.save_database)
        file_menu.addAction(save_action)

        save_as_action = QAction("Save As", self)
        save_as_action.triggered.connect(self.save_as_database)
        file_menu.addAction(save_as_action)

        create_cell_action = QAction("Create Cell", self)
        create_cell_action.triggered.connect(self.create_cell_dialog)
        edit_menu.addAction(create_cell_action)

        edit_cell_action = QAction("Edit Cell", self)
        edit_cell_action.triggered.connect(self.edit_selected_cell)
        edit_menu.addAction(edit_cell_action)

        delete_action = QAction("Delete", self)
        delete_action.triggered.connect(self.delete_selected_cell)
        edit_menu.addAction(delete_action)

        about_action = QAction("About", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)

    def browse_file(self):
        start_dir = self.last_dir if self.last_dir else ""
        filename, _ = QFileDialog.getOpenFileName(self, "Open Data File", start_dir, "All Files (*)")
        if filename:
            self.file_path_input.setText(filename)
            self.last_dir = os.path.dirname(filename)
            # Persist the new file directory
            self.settings.setValue("last_file_directory", self.last_dir)

    def create_cell_dialog(self):
        """Create cell from dialog (menu)"""
        dialog = EditCellDialog(self, {})
        if dialog.exec() == QDialog.DialogCode.Accepted:
            data = dialog.get_data()
            try:
                if not data["id"]:
                    self.show_warning("ID is required!")
                    return
                self.db.add_cell(data)
                self.load_data()
                self.show_info("Cell created successfully.")
            except Exception as e:
                self.log_and_show_error("Database error", e)

    def create_cell_from_form(self):
        """Create cell from right-side form"""
        try:
            data = {
                "id": self.id_input.text(),
                "instrument": self.instrument_input.currentText(),
                "date": self.date_input.date().toString("yyyy-MM-dd"),
                "negative_electrode": self.neg_electrode_input.text(),
                "positive_electrode": self.pos_electrode_input.text(),
                "n_active_mass": float(self.n_mass_input.text()) if self.n_mass_input.text() else 0.0,
                "p_active_mass": float(self.p_mass_input.text()) if self.p_mass_input.text() else 0.0,
                "electrolyte": self.electrolyte_input.text(),
                "format": self.format_input.currentText(),
                "voltage_range": self.voltage_range_input.text(),
                "comment": self.comment_input.text(),
                "status": "Active" if self.status_chk.isChecked() else "Stopped",
                "file_path": self.file_path_input.text()
            }
            
            if not data["id"]:
                logger.warning("Attempted to create cell without ID")
                self.show_warning("ID is required!")
                return
            
            logger.info(f"Creating cell: {data['id']} ({data['instrument']})")
            self.db.add_cell(data)
            self.load_data()
            logger.info(f"Cell created successfully: {data['id']}")
            self.show_info("Cell created successfully.")
            
        except ValueError as e:
            logger.error(f"Invalid input when creating cell: {e}")
            self.show_error(f"Invalid input: {e}")
        except Exception as e:
            self.log_and_show_error("Error creating cell", e)

    def clear_form(self):
        self.id_input.clear()
        self.neg_electrode_input.clear()
        self.pos_electrode_input.clear()
        self.n_mass_input.clear()
        self.p_mass_input.clear()
        self.electrolyte_input.clear()
        self.voltage_range_input.clear()
        self.file_path_input.clear()
        self.comment_input.clear()
        self.instrument_input.setCurrentIndex(0)
        self.date_input.setDate(QDate.currentDate())
        self.format_input.setCurrentIndex(0)
        self.status_chk.setChecked(True)

    def load_data(self):
        """Load all cells from database into table."""
        try:
            df = self.db.get_all_cells()
            
            # Configure table
            self.table_widget.setColumnCount(len(df.columns))
            self.table_widget.setHorizontalHeaderLabels(df.columns)
            self.table_widget.setRowCount(len(df))
            
            for i, row in df.iterrows():
                for j, val in enumerate(row):
                    self.table_widget.setItem(i, j, QTableWidgetItem(str(val)))
            
            self.table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
            self.table_widget.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
            self.table_widget.horizontalHeader().setStretchLastSection(True)
            
            logger.debug(f"Loaded {len(df)} cells into table")
            self._set_window_title()
        except Exception as e:
            logger.error(f"Error loading data from database: {e}", exc_info=True)

    def get_selected_row_data(self):
        selected_rows = self.table_widget.selectionModel().selectedRows()
        if not selected_rows:
            return None

        row_idx = selected_rows[0].row()
        data = {}
        for col in range(self.table_widget.columnCount()):
            header_item = self.table_widget.horizontalHeaderItem(col)
            key = header_item.text() if header_item else f"col_{col}"
            item = self.table_widget.item(row_idx, col)
            data[key] = item.text() if item else ""

        return data

    def _load_and_process_cell(self, file_path, instrument_type, p_active_mass):
        """
        Unified data loading and processing pipeline.
        
        Args:
            file_path: Path to raw data file
            instrument_type: "Biologic", "Arbin", or "Maccor"
            p_active_mass: Positive electrode mass in mg
        
        Returns:
            Processed DataFrame
        
        Raises:
            ValueError: If inputs invalid
            Exception: If data loading/processing fails
        """
        try:
            logger.debug(f"Loading and processing {instrument_type} data: {file_path}, p_active_mass={p_active_mass}")
            
            if p_active_mass <= 0:
                logger.error(f"Invalid p_active_mass: {p_active_mass}")
                raise ValueError("P active mass must be greater than 0")
            
            # Import data
            logger.debug(f"Importing data from {file_path}")
            import_start = time.perf_counter()
            df = DataImporter.import_data(file_path, instrument_type)
            import_elapsed = time.perf_counter() - import_start
            imported_rows = len(df)
            logger.info(
                f"import_data completed in {import_elapsed:.3f}s for {file_path} ({imported_rows} rows)"
            )
            
            # Basic data cleaning
            clean_start = time.perf_counter()
            df_cleaned = self.clean_data(df)
            clean_elapsed = time.perf_counter() - clean_start
            cleaned_rows = len(df_cleaned)
            logger.info(
                f"clean_data completed in {clean_elapsed:.3f}s for {file_path} ({imported_rows} -> {cleaned_rows} rows)"
            )
            
            # Process data based on instrument type
            processor_map = {
                "Biologic": DataProcessor.process_biologic_data,
                "Arbin": DataProcessor.process_arbin_data,
                "Maccor": DataProcessor.process_maccor_data,
            }
            
            if instrument_type not in processor_map:
                logger.error(f"Unknown instrument type: {instrument_type}")
                raise ValueError(f"Unknown instrument type: {instrument_type}")
            
            logger.debug(f"Processing data with {instrument_type} processor")
            process_start = time.perf_counter()
            processed = processor_map[instrument_type](df_cleaned, p_active_mass)
            process_elapsed = time.perf_counter() - process_start
            processed_rows = len(processed)
            logger.info(
                f"process_{instrument_type.lower()}_data completed in {process_elapsed:.3f}s for {file_path} ({cleaned_rows} -> {processed_rows} rows)"
            )
            return processed
        except Exception as e:
            logger.error(f"Error in data loading/processing pipeline: {e}", exc_info=True)
            raise

    def show_table_context_menu(self, position):
        if self.table_widget.itemAt(position) is None:
            return

        menu = QMenu(self)
        edit_action = menu.addAction("Edit Cell")
        delete_action = menu.addAction("Delete")

        action = menu.exec(self.table_widget.viewport().mapToGlobal(position))
        if action == edit_action:
            self.edit_selected_cell()
        elif action == delete_action:
            self.delete_selected_cell()

    def edit_selected_cell(self):
        data = self.get_selected_row_data()
        if not data:
            self.show_warning("Please select a cell from the table first.")
            return

        old_id = data.get("id", "")
        dialog = EditCellDialog(self, data)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            updated_data = dialog.get_data()
            try:
                self.db.update_cell(old_id, updated_data)
                self.load_data()
                self.show_info("Cell updated successfully.")
            except Exception as e:
                self.log_and_show_error("Database error", e)

    def delete_selected_cell(self):
        data = self.get_selected_row_data()
        if not data:
            self.show_warning("Please select a cell from the table first.")
            return

        cell_id = data.get("id", "")
        if not cell_id:
            self.show_warning("Cannot delete row without ID.")
            return

        confirm = QMessageBox.question(self, "Confirm Delete", f"Delete cell {cell_id}?",
                                       QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if confirm == QMessageBox.StandardButton.Yes:
            try:
                self.db.delete_cell(cell_id)
                self.load_data()
            except Exception as e:
                self.log_and_show_error("Database error", e)

    def new_database(self):
        filename, _ = QFileDialog.getSaveFileName(self, "Create New Database", "", "SQLite Database (*.db)")
        if not filename:
            logger.debug("New database dialog canceled")
            return

        try:
            logger.info(f"Creating new database: {filename}")
            self.db.switch_database(filename)
            self.set_last_database_path(filename)
            self.load_data()
            logger.info(f"New database created successfully: {filename}")
        except Exception as e:
            self.log_and_show_error("Failed to create database", e)

    def open_database(self):
        filename, _ = QFileDialog.getOpenFileName(self, "Open Database", "", "SQLite Database (*.db)")
        if not filename:
            logger.debug("Open database dialog canceled")
            return

        try:
            logger.info(f"Opening database: {filename}")
            self.db.switch_database(filename)
            self.set_last_database_path(filename)
            self.load_data()
            logger.info(f"Database opened successfully: {filename}")
        except Exception as e:
            self.log_and_show_error("Failed to open database", e)

    def save_database(self):
        try:
            logger.info(f"Saving database: {self.db.db_name}")
            self.db.save()
            logger.info("Database saved successfully")
            self.show_info("Database saved successfully.")
        except Exception as e:
            self.log_and_show_error("Failed to save database", e)

    def save_as_database(self):
        filename, _ = QFileDialog.getSaveFileName(self, "Save Database As", "", "SQLite Database (*.db)")
        if not filename:
            logger.debug("Save As dialog canceled")
            return

        try:
            logger.info(f"Saving database as: {filename}")
            self.db.save_as(filename)
            self.set_last_database_path(filename)
            logger.info(f"Database saved as: {filename}")
            self.show_info("Database saved successfully.")
        except Exception as e:
            self.log_and_show_error("Failed to save database", e)

    def closeEvent(self, event):
        """Persist user preferences on close."""
        try:
            self.set_last_database_path(self.db.db_name)
            self.save_window_geometry()
        finally:
            super().closeEvent(event)

    def show_about(self):
        QMessageBox.information(self, "About", "Battery Data Manager\nManage, analyze, and plot battery data.")
    
    def update_preload_checkbox(self):
        """Update preload checkbox based on selected cell"""
        selected_rows = self.table_widget.selectionModel().selectedRows()
        if not selected_rows:
            self.preload_chk.setChecked(False)
            return
        
        row_idx = selected_rows[0].row()
        cell_id_item = self.table_widget.item(row_idx, TableColumns.ID)
        selected_cell_id = cell_id_item.text() if cell_id_item else None
        
        # Check if selected cell is in preload cache
        is_preloaded = selected_cell_id in self.preload_cache
        self.preload_chk.setChecked(is_preloaded)
    
    def preload_data(self):
        """Preload and process data without saving to CSV"""
        try:
            selected_rows = self.table_widget.selectionModel().selectedRows()
            if not selected_rows:
                logger.warning("Preload attempted without cell selection")
                self.show_warning("Please select a cell from the table first.")
                return

            row_idx = selected_rows[0].row()
            
            # Get file path, instrument type, and p_active_mass from the selected row
            file_path_item = self.table_widget.item(row_idx, TableColumns.FILE_PATH)
            cell_id_item = self.table_widget.item(row_idx, TableColumns.ID)
            instrument_item = self.table_widget.item(row_idx, TableColumns.INSTRUMENT)
            p_mass_item = self.table_widget.item(row_idx, TableColumns.P_MASS)
            
            if not file_path_item or not file_path_item.text():
                logger.warning("Preload attempted without file path")
                self.show_warning("No file path associated with this cell.")
                return
                
            file_path = file_path_item.text()
            cell_id = cell_id_item.text() if cell_id_item else "Unknown Cell"
            instrument_type = instrument_item.text() if instrument_item else "Unknown"
            
            # Get p_active_mass value
            try:
                p_active_mass = float(p_mass_item.text()) if p_mass_item and p_mass_item.text() else 0.0
            except ValueError:
                p_active_mass = 0.0
            
            if p_active_mass <= 0:
                logger.warning(f"Preload attempted with invalid p_active_mass: {p_active_mass}")
                self.show_warning("P active mass must be greater than 0 for data processing.")
                return

            self._start_data_load_worker(
                "preload",
                cell_id,
                file_path,
                instrument_type,
                p_active_mass,
            )
            
        except Exception as e:
            logger.error(f"Error preloading data: {e}", exc_info=True)
            self.show_error(f"Failed to preload data: {str(e)}")

    def open_analysis(self):
        """Open analysis window for selected cell"""
        try:
            selected_rows = self.table_widget.selectionModel().selectedRows()
            if not selected_rows:
                logger.warning("Analysis attempted without cell selection")
                self.show_warning("Please select a cell from the table first.")
                return

            row_idx = selected_rows[0].row()
            
            # Get file path, instrument type, and p_active_mass from the selected row
            file_path_item = self.table_widget.item(row_idx, TableColumns.FILE_PATH)
            cell_id_item = self.table_widget.item(row_idx, TableColumns.ID)
            instrument_item = self.table_widget.item(row_idx, TableColumns.INSTRUMENT)
            p_mass_item = self.table_widget.item(row_idx, TableColumns.P_MASS)
            
            cell_id = cell_id_item.text() if cell_id_item else "Unknown Cell"
            
            # Check if data is already cached for THIS cell
            if cell_id in self.preload_cache:
                logger.debug(f"Using cached data for cell: {cell_id}")
                # Use cached data
                self.processed_data = self.preload_cache[cell_id]
                self.current_cell_id = cell_id
                self.save_processed_btn.setEnabled(True)
                analysis_window = AnalysisWindow(self, self.processed_data, self.current_cell_id)
                analysis_window.exec()
                return
            
            # Otherwise, load data for the selected cell
            if not file_path_item or not file_path_item.text():
                logger.warning("Analysis attempted without file path")
                self.show_warning("No file path associated with this cell.")
                return
                
            file_path = file_path_item.text()
            instrument_type = instrument_item.text() if instrument_item else "Unknown"
            
            # Get p_active_mass value
            try:
                p_active_mass = float(p_mass_item.text()) if p_mass_item and p_mass_item.text() else 0.0
            except ValueError:
                p_active_mass = 0.0
            
            if p_active_mass <= 0:
                logger.warning(f"Analysis attempted with invalid p_active_mass: {p_active_mass}")
                self.show_warning("P active mass must be greater than 0 for data processing.")
                return

            logger.info(f"Opening analysis for cell: {cell_id}")
            self._start_data_load_worker(
                "analysis",
                cell_id,
                file_path,
                instrument_type,
                p_active_mass,
            )
            
        except Exception as e:
            logger.error(f"Error opening analysis: {e}", exc_info=True)
            self.show_error(f"Failed to analyze data: {str(e)}")

    def save_processed_data(self):
        """Save processed_raw_data to a user-selected location"""
        if self.processed_data is None:
            logger.warning("Save processed data attempted without data")
            self.show_warning("No processed data available. Please run data analysis first.")
            return
        
        # Default filename
        default_filename = f"{self.current_cell_id}_processed_raw_data.csv" if self.current_cell_id else "processed_raw_data.csv"
        
        filename, _ = QFileDialog.getSaveFileName(
            self, 
            "Save Processed Data", 
            default_filename, 
            "CSV Files (*.csv)"
        )
        
        if not filename:
            logger.debug("Save processed data dialog canceled")
            return
        
        try:
            logger.info(f"Saving processed data for cell: {self.current_cell_id} to {filename}")
            self.processed_data.to_csv(filename, index=False)
            logger.info(f"Processed data saved successfully: {filename} ({len(self.processed_data)} rows)")
            self.show_info(f"Processed data saved successfully to:\n{filename}")
        except Exception as e:
            logger.error(f"Error saving processed data: {e}", exc_info=True)
            self.show_error(f"Failed to save processed data: {str(e)}")

    def clean_data(self, df):
        """Clean battery data - remove rows with invalid values, etc."""
        try:
            df_clean = df.copy()
            
            original_rows = len(df_clean)
            
            # Remove rows where all values are 0 or NaN
            df_clean = df_clean.dropna(how='all')
            
            # Remove duplicate rows
            df_clean = df_clean.drop_duplicates()
            
            # Reset index
            df_clean = df_clean.reset_index(drop=True)
            
            final_rows = len(df_clean)
            removed_rows = original_rows - final_rows
            logger.info(f"Data cleaned: {original_rows} -> {final_rows} rows (removed {removed_rows} rows)")
            print(f"Data cleaned: {original_rows} -> {final_rows} rows")
            return df_clean
        except Exception as e:
            logger.error(f"Error during data cleaning: {e}", exc_info=True)
            raise


class EditCellDialog(QDialog):
    def __init__(self, parent, data):
        super().__init__(parent)
        self.setWindowTitle("Edit Cell")
        self.resize(600, 400)

        self.data = data

        layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        self.id_input = QLineEdit(data.get("id", ""))
        self.instrument_input = QComboBox()
        self.instrument_input.addItems(["Arbin", "Biologic", "Maccor"])
        self.instrument_input.setCurrentText(data.get("instrument", "Arbin"))

        self.date_input = QDateEdit()
        date_str = data.get("date", "")
        if date_str:
            self.date_input.setDate(QDate.fromString(date_str, "yyyy-MM-dd"))
        else:
            self.date_input.setDate(QDate.currentDate())

        self.neg_electrode_input = QComboBox()
        self.neg_electrode_input.addItems(["150um Li", "Graphite", "Silicon", "Other"])
        self.neg_electrode_input.setEditable(True)
        self.neg_electrode_input.setCurrentText(data.get("negative_electrode", ""))

        self.pos_electrode_input = QComboBox()
        self.pos_electrode_input.addItems(["NMC83", "NMC811", "LFP", "LCO", "Other"])
        self.pos_electrode_input.setEditable(True)
        self.pos_electrode_input.setCurrentText(data.get("positive_electrode", ""))

        self.n_mass_input = QLineEdit(data.get("n_active_mass", ""))
        self.p_mass_input = QLineEdit(data.get("p_active_mass", ""))

        self.electrolyte_input = QComboBox()
        self.electrolyte_input.addItems(["1M LiPF6 in FEC/DMC 1/4", "Gen 2", "Other"])
        self.electrolyte_input.setEditable(True)
        self.electrolyte_input.setCurrentText(data.get("electrolyte", ""))

        self.voltage_range_input = QLineEdit(data.get("voltage_range", ""))

        self.format_input = QComboBox()
        self.format_input.addItems(["Coin", "Pouch", "Swagelok", "Other"])
        self.format_input.setCurrentText(data.get("format", ""))

        self.file_path_input = QLineEdit(data.get("file_path", ""))
        self.browse_btn = QPushButton("...")
        self.browse_btn.clicked.connect(self.browse_file)
        file_layout = QHBoxLayout()
        file_layout.addWidget(self.file_path_input)
        file_layout.addWidget(self.browse_btn)

        self.status_chk = QCheckBox("Active")
        self.status_chk.setChecked(data.get("status", "Active") == "Active")

        self.comment_input = QLineEdit(data.get("comment", ""))

        form_layout.addRow("ID:", self.id_input)
        form_layout.addRow("Instrument:", self.instrument_input)
        form_layout.addRow("Date:", self.date_input)
        form_layout.addRow("Negative electrode:", self.neg_electrode_input)
        form_layout.addRow("Positive electrode:", self.pos_electrode_input)
        form_layout.addRow("N active mass (mg):", self.n_mass_input)
        form_layout.addRow("P active mass (mg):", self.p_mass_input)
        form_layout.addRow("Electrolyte:", self.electrolyte_input)
        form_layout.addRow("Voltage range:", self.voltage_range_input)
        form_layout.addRow("Format:", self.format_input)
        form_layout.addRow("File:", file_layout)
        form_layout.addRow("Status:", self.status_chk)
        form_layout.addRow("Comment:", self.comment_input)

        layout.addLayout(form_layout)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def browse_file(self):
        filename, _ = QFileDialog.getOpenFileName(self, "Open Data File", "", "Data Files (*.mpr *.res *.csv *.txt)")
        if filename:
            self.file_path_input.setText(filename)

    def get_data(self):
        return {
            "id": self.id_input.text(),
            "instrument": self.instrument_input.currentText(),
            "date": self.date_input.date().toString("yyyy-MM-dd"),
            "negative_electrode": self.neg_electrode_input.currentText(),
            "positive_electrode": self.pos_electrode_input.currentText(),
            "n_active_mass": float(self.n_mass_input.text()) if self.n_mass_input.text() else 0.0,
            "p_active_mass": float(self.p_mass_input.text()) if self.p_mass_input.text() else 0.0,
            "electrolyte": self.electrolyte_input.currentText(),
            "format": self.format_input.currentText(),
            "voltage_range": self.voltage_range_input.text(),
            "comment": self.comment_input.text(),
            "status": "Active" if self.status_chk.isChecked() else "Stopped",
            "file_path": self.file_path_input.text()
        }
