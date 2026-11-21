import logging
import math
import os
import sqlite3
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from html import escape

from PySide6.QtCore import QDate, QItemSelectionModel, QSettings, QSignalBlocker, QTimer, Qt
from PySide6.QtGui import QAction, QKeySequence
from PySide6.QtWidgets import (
    QComboBox, QDateEdit, QDialog, QDialogButtonBox, QFileDialog, QFormLayout,
    QFrame, QHBoxLayout, QHeaderView, QLabel, QLineEdit, QMainWindow, QMenu,
    QMessageBox, QPlainTextEdit, QProgressBar, QPushButton, QScrollArea,
    QSizePolicy, QSplitter, QStackedWidget, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget,
)

from analysis.data_processor import DataProcessor
from data_import.file_status import FILE_STOPPED, FILE_UPDATED, FileStatusScan
from data_import.importer import DataImporter
from database import BatteryDatabase
from ui.analysis_window import AnalysisWindow, MAX_ANALYSIS_BATTERIES


logger = logging.getLogger(__name__)

INSTRUMENTS = ("Arbin", "Biologic", "Maccor")
STATUSES = ("Active", FILE_UPDATED, FILE_STOPPED)
DATABASE_FILTER = "SQLite databases (*.db *.sqlite *.sqlite3)"
DATA_FILE_FILTERS = {
    "Arbin": "Arbin data (*.res);;All files (*)",
    "Biologic": "Biologic data (*.mpr);;All files (*)",
    "Maccor": "Maccor data (*.txt *.xyz *.0* *.1* *.2* *.3* *.4* *.5* *.6* *.7* *.8* *.9*);;All files (*)",
}
LIBRARY_COLUMNS = (
    ("id", "Battery ID"), ("date", "Date"), ("instrument", "Instrument"),
    ("positive_electrode", "Positive"), ("negative_electrode", "Negative"),
    ("status", "Status"),
)
STATUS_COLUMN = next(index for index, (field, _) in enumerate(LIBRARY_COLUMNS) if field == "status")
DETAIL_FIELDS = (
    ("date", "Test date"), ("positive_electrode", "Positive electrode"),
    ("negative_electrode", "Negative electrode"), ("p_active_mass", "Positive mass (g)"),
    ("n_active_mass", "Negative mass (g)"), ("electrolyte", "Electrolyte"),
    ("format", "Cell format"), ("voltage_range", "Voltage range"),
    ("file_path", "Data file"), ("comment", "Notes"),
)

WINDOW_STYLE = """
QLabel#pageTitle { font-size: 24px; font-weight: 650; }
QLabel#detailTitle { font-size: 19px; font-weight: 650; }
QFrame#detailPanel, QFrame#analysisControls {
    background: palette(base); border: 1px solid palette(mid); border-radius: 8px;
}
QScrollArea#detailScroll, QWidget#detailContents { background: palette(base); }
QTableWidget { border: 1px solid palette(mid); }
QHeaderView::section { padding: 7px 8px; }
QLineEdit, QComboBox { min-height: 25px; }
QPushButton { padding: 5px 10px; }
QPushButton[primary="true"] {
    background: #146b69; border: 1px solid #146b69; border-radius: 5px;
    color: white; font-weight: 600;
}
QPushButton[primary="true"]:hover { background: #0e5b59; }
QPushButton[primary="true"]:focus { border: 2px solid palette(highlight); }
QPushButton[primary="true"]:disabled {
    color: palette(mid); border: 1px solid palette(mid); background: palette(button);
}
"""


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.resize(1250, 790)
        self.setStyleSheet(WINDOW_STYLE)
        self.db = BatteryDatabase()
        self.settings = QSettings("BatteryDBApp", "BatteryDataManager")
        self.last_dir = self.settings.value("last_file_directory", "", type=str)
        self._cells = {}
        self.preload_cache = OrderedDict()
        self._load_errors = {}
        self._data_loader = ThreadPoolExecutor(max_workers=1, thread_name_prefix="battery-loader")
        self._data_load_future = None
        self._load_timer = QTimer(self)
        self._load_timer.setInterval(30)
        self._load_timer.timeout.connect(self._finish_data_load)
        self._file_scan = None
        self._pending_file_scan = None
        self._file_scan_retry_until = None
        self._file_scan_timer = QTimer(self)
        self._file_scan_timer.setInterval(50)
        self._file_scan_timer.timeout.connect(self._finish_file_scan)
        self._load_request = None
        self._close_after_load = False
        self.analysis_page = None

        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)
        self.init_ui()
        self.init_menu()
        self.restore_last_database()
        self.load_data()
        geometry = self.settings.value("window_geometry")
        if geometry:
            self.restoreGeometry(geometry)
        splitter_state = self.settings.value("library_splitter")
        if splitter_state:
            self.library_splitter.restoreState(splitter_state)
        self.statusBar().showMessage("Ready")
        self._queue_file_scan()

    def init_ui(self):
        self.library_page = QWidget()
        self.stack.addWidget(self.library_page)
        layout = QVBoxLayout(self.library_page)
        layout.setContentsMargins(22, 18, 22, 14)
        layout.setSpacing(13)

        heading_row = QHBoxLayout()
        heading = QVBoxLayout()
        heading.setSpacing(3)
        title = QLabel("Battery library")
        title.setObjectName("pageTitle")
        self.database_label = QLabel()
        self.database_label.setTextFormat(Qt.TextFormat.PlainText)
        heading.addWidget(title)
        heading.addWidget(self.database_label)
        heading_row.addLayout(heading, 1)
        self.new_cell_btn = QPushButton("New battery")
        self.new_cell_btn.setProperty("primary", True)
        self.new_cell_btn.clicked.connect(self.create_cell_dialog)
        heading_row.addWidget(self.new_cell_btn)
        layout.addLayout(heading_row)

        filters = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search batteries by ID, material, date or file…")
        self.search_input.setAccessibleName("Search batteries")
        self.search_input.setClearButtonEnabled(True)
        self.instrument_filter = QComboBox()
        self.instrument_filter.addItems(["All instruments", *INSTRUMENTS])
        self.instrument_filter.setAccessibleName("Filter by instrument")
        self.status_filter = QComboBox()
        self.status_filter.addItems(["All statuses", *STATUSES])
        self.status_filter.setAccessibleName("Filter by status")
        filters.addWidget(self.search_input, 1)
        filters.addWidget(self.instrument_filter)
        filters.addWidget(self.status_filter)
        layout.addLayout(filters)
        for signal in (self.search_input.textChanged, self.instrument_filter.currentIndexChanged,
                       self.status_filter.currentIndexChanged):
            signal.connect(self.apply_filters)

        self.library_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.library_splitter.setChildrenCollapsible(False)
        self.library_splitter.setHandleWidth(12)
        layout.addWidget(self.library_splitter, 1)

        list_panel = QWidget()
        list_layout = QVBoxLayout(list_panel)
        list_layout.setContentsMargins(0, 0, 0, 0)
        list_layout.setSpacing(7)
        self.table_widget = QTableWidget(0, len(LIBRARY_COLUMNS))
        self.table_widget.setAccessibleName("Battery records")
        self.table_widget.setHorizontalHeaderLabels([label for _, label in LIBRARY_COLUMNS])
        self.table_widget.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table_widget.setSelectionMode(QTableWidget.SelectionMode.ExtendedSelection)
        self.table_widget.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table_widget.setAlternatingRowColors(True)
        self.table_widget.setWordWrap(False)
        self.table_widget.verticalHeader().hide()
        self.table_widget.verticalHeader().setDefaultSectionSize(32)
        header = self.table_widget.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        self.table_widget.setSortingEnabled(True)
        self.table_widget.sortItems(0, Qt.SortOrder.AscendingOrder)
        header.sortIndicatorChanged.connect(self.apply_filters)
        self.table_widget.itemSelectionChanged.connect(self.update_selected_cell)
        self.table_widget.itemDoubleClicked.connect(self.open_analysis)
        self.table_widget.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_widget.customContextMenuRequested.connect(self.show_table_context_menu)
        list_layout.addWidget(self.table_widget, 1)
        self.result_count = QLabel()
        list_layout.addWidget(self.result_count)
        self.library_splitter.addWidget(list_panel)
        self.library_splitter.addWidget(self.create_details_panel())
        self.library_splitter.setStretchFactor(0, 3)
        self.library_splitter.setStretchFactor(1, 1)
        self.library_splitter.setSizes([845, 330])

    def create_details_panel(self):
        panel = QFrame()
        panel.setObjectName("detailPanel")
        panel.setMinimumWidth(300)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(9)
        self.detail_title = self.detail_label()
        self.detail_title.setObjectName("detailTitle")
        self.detail_summary = self.detail_label()
        layout.addWidget(self.detail_title)
        layout.addWidget(self.detail_summary)

        detail_scroll = QScrollArea()
        detail_scroll.setObjectName("detailScroll")
        detail_scroll.setFrameShape(QFrame.Shape.NoFrame)
        detail_scroll.setWidgetResizable(True)
        detail_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.detail_contents = QWidget()
        self.detail_contents.setObjectName("detailContents")
        form = QFormLayout(self.detail_contents)
        form.setContentsMargins(0, 5, 1, 4)
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        form.setVerticalSpacing(10)
        self.detail_values = {}
        for key, label in DETAIL_FIELDS:
            value = self.detail_label()
            value.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
            value.setMinimumWidth(60)
            value.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            self.detail_values[key] = value
            if key in ("file_path", "comment"):
                form.addRow(QLabel(label))
                form.addRow(value)
            else:
                form.addRow(label, value)
        self.details_text = self.detail_values["file_path"]
        detail_scroll.setWidget(self.detail_contents)
        layout.addWidget(detail_scroll, 1)

        self.data_status = self.detail_label()
        layout.addWidget(self.data_status)
        self.loading_indicator = QProgressBar()
        self.loading_indicator.setRange(0, 0)
        self.loading_indicator.setTextVisible(False)
        self.loading_indicator.setFixedHeight(5)
        self.loading_indicator.setAccessibleName("Loading battery data")
        layout.addWidget(self.loading_indicator)
        self.plot_btn = QPushButton("Open analysis")
        self.plot_btn.setProperty("primary", True)
        self.plot_btn.clicked.connect(self.open_analysis)
        layout.addWidget(self.plot_btn)
        self.save_processed_btn = QPushButton("Export processed CSV…")
        self.save_processed_btn.clicked.connect(self.save_processed_data)
        layout.addWidget(self.save_processed_btn)
        row = QHBoxLayout()
        self.edit_btn = QPushButton("Edit battery…")
        self.edit_btn.clicked.connect(self.edit_selected_cell)
        self.preload_btn = QPushButton("Preload data")
        self.preload_btn.clicked.connect(self.preload_data)
        row.addWidget(self.edit_btn)
        row.addWidget(self.preload_btn)
        layout.addLayout(row)
        return panel

    @staticmethod
    def detail_label():
        label = QLabel()
        label.setTextFormat(Qt.TextFormat.PlainText)
        label.setWordWrap(True)
        label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        return label

    def init_menu(self):
        menu_bar = self.menuBar()
        file_menu = menu_bar.addMenu("File")
        edit_menu = menu_bar.addMenu("Edit")
        help_menu = menu_bar.addMenu("Help")
        for title, slot, shortcut in (
            ("New database…", self.new_database, QKeySequence.StandardKey.New),
            ("Open database…", self.open_database, QKeySequence.StandardKey.Open),
            ("Save database", self.save_database, QKeySequence.StandardKey.Save),
            ("Save database as…", self.save_as_database, QKeySequence.StandardKey.SaveAs),
        ):
            action = file_menu.addAction(title)
            action.setShortcut(shortcut)
            action.triggered.connect(slot)
        edit_menu.addAction("New battery…", self.create_cell_dialog)
        self.edit_action = edit_menu.addAction("Edit battery…", self.edit_selected_cell)
        self.delete_action = edit_menu.addAction("Delete battery…", self.delete_selected_cell)
        search_action = QAction("Find battery", self)
        search_action.setShortcut(QKeySequence.StandardKey.Find)
        search_action.triggered.connect(self.find_battery)
        edit_menu.addAction(search_action)
        help_menu.addAction("About", self.show_about)

    def find_battery(self):
        self.show_library()
        self.search_input.setFocus()
        self.search_input.selectAll()

    def load_data(self, select_id=None, *, preserve_selection=True):
        if select_id is not None:
            selected_ids = {select_id}
        else:
            selected_ids = {cell["id"] for cell in self.selected_cells()} if preserve_selection else set()
        try:
            frame = self.db.get_all_cells()
        except Exception as error:
            self.log_and_show_error("Could not read battery records", error)
            return
        records = frame.astype(object).where(frame.notna(), None).to_dict("records")
        self._cells = {record["id"]: record for record in records}
        table = self.table_widget
        header = table.horizontalHeader()
        table.blockSignals(True)
        header.blockSignals(True)
        try:
            table.setSortingEnabled(False)
            table.clearSelection()
            table.setRowCount(0)
            table.setRowCount(len(records))
            for row, record in enumerate(records):
                for column, (key, _) in enumerate(LIBRARY_COLUMNS):
                    value = record[key]
                    text = str(value) if value is not None and value != "" else "—"
                    item = QTableWidgetItem(text)
                    item.setToolTip(text)
                    if column == 0:
                        item.setData(Qt.ItemDataRole.UserRole, value)
                    table.setItem(row, column, item)
            table.setSortingEnabled(True)
        finally:
            header.blockSignals(False)
            table.blockSignals(False)
        self._set_window_title()
        self.apply_filters()
        if selected_ids:
            selection = table.selectionModel()
            for row in range(table.rowCount()):
                if not table.isRowHidden(row) and self._cell_id_at(row) in selected_ids:
                    selection.select(table.model().index(row, 0),
                                     QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)
        if self.analysis_page is not None:
            self.analysis_page.set_batteries(self._cells, preserve_selection=preserve_selection)
            self._refresh_analysis()

    def _cell_id_at(self, row):
        return self.table_widget.item(row, 0).data(Qt.ItemDataRole.UserRole)

    def apply_filters(self):
        terms = self.search_input.text().casefold().split()
        instrument = self.instrument_filter.currentText() if self.instrument_filter.currentIndex() else None
        status = self.status_filter.currentText() if self.status_filter.currentIndex() else None
        table = self.table_widget
        selected = table.selectionModel().selectedRows()
        visible = 0
        for row in range(table.rowCount()):
            record = self._cells[self._cell_id_at(row)]
            values = " ".join(str(value) for value in record.values() if value is not None).casefold() if terms else ""
            matches = (
                (instrument is None or record["instrument"] == instrument)
                and (status is None or record["status"] == status)
                and all(term in values for term in terms)
            )
            table.setRowHidden(row, not matches)
            visible += matches
        for index in selected:
            if table.isRowHidden(index.row()):
                table.selectionModel().select(index, QItemSelectionModel.SelectionFlag.Deselect |
                                              QItemSelectionModel.SelectionFlag.Rows)
        total = table.rowCount()
        if not total:
            text = "No batteries in this database. Use New battery to add a record."
        elif not visible:
            text = "No batteries match the current search and filters."
        else:
            text = f"{visible} of {total} batteries"
        self.result_count.setText(text)
        self.update_selected_cell()

    def get_selected_row_data(self):
        selected = self.selected_cells()
        return selected[0] if len(selected) == 1 else None

    def selected_cells(self):
        table = self.table_widget
        rows = sorted(index.row() for index in table.selectionModel().selectedRows())
        return [self._cells[self._cell_id_at(row)] for row in rows if not table.isRowHidden(row)]

    @staticmethod
    def _source_problem(record):
        if not record.get("file_path"):
            return "Link an instrument data file in Edit battery."
        if record.get("instrument") not in INSTRUMENTS:
            return "Choose a supported instrument in Edit battery."
        mass = record.get("p_active_mass")
        if not isinstance(mass, (float, int)) or not math.isfinite(mass) or mass <= 0:
            return "Enter a positive active mass greater than zero in Edit battery."
        return ""

    def update_selected_cell(self):
        selected = self.selected_cells()
        record = selected[0] if len(selected) == 1 else None
        cell_id = record.get("id") if record else None
        count = len(selected)
        busy = self._data_load_future is not None
        loading = self._load_request is not None and self._load_request == cell_id
        cached = cell_id in self.preload_cache and bool(cell_id)
        problem = self._source_problem(record) if record else ""

        if count > 1:
            self.detail_title.setText(f"{count} batteries selected")
            self.detail_summary.setText(", ".join(str(cell["id"]) for cell in selected[:MAX_ANALYSIS_BATTERIES]))
        else:
            self.detail_title.setText(str(cell_id) if cell_id else "Battery details")
            self.detail_summary.setText(" · ".join(
                str(record[key]) for key in ("instrument", "status") if record.get(key)
            ) if record else "Select a battery to view its details.")
        self.detail_contents.setVisible(record is not None)
        if record:
            for key, label in self.detail_values.items():
                value = record.get(key)
                text = str(value) if value is not None and value != "" else "—"
                label.setText(text)
                if key in ("file_path", "comment"):
                    label.setToolTip(f"<qt>{escape(text)}</qt>")

        note = ""
        if count > MAX_ANALYSIS_BATTERIES:
            note = f"Select up to {MAX_ANALYSIS_BATTERIES} batteries for a comparison."
        elif loading:
            note = "Reading and processing the instrument file…"
        elif cached:
            note = f"Data ready · {len(self.preload_cache[cell_id]):,} rows"
        elif cell_id:
            note = self._load_errors.get(cell_id) or problem
        self.data_status.setText(note)
        self.data_status.setVisible(bool(note))
        self.loading_indicator.setVisible(loading)
        can_analyze = any(
            cell["id"] and (cell["id"] in self.preload_cache or not self._source_problem(cell))
            for cell in selected
        )
        self.plot_btn.setText(f"Compare {count} batteries" if count > 1 else "Open analysis")
        self.plot_btn.setEnabled(0 < count <= MAX_ANALYSIS_BATTERIES and can_analyze)
        self.preload_btn.setEnabled(bool(cell_id and not busy and not problem))
        self.preload_btn.setText("Reload data" if cached else "Preload data")
        self.save_processed_btn.setEnabled(cached and not loading)
        self.edit_btn.setEnabled(bool(cell_id))
        can_edit = bool(cell_id) and self.stack.currentWidget() is self.library_page
        self.edit_action.setEnabled(can_edit)
        self.delete_action.setEnabled(can_edit)

    def show_table_context_menu(self, position):
        item = self.table_widget.itemAt(position)
        if item is None:
            return
        if not item.isSelected():
            self.table_widget.selectionModel().select(
                self.table_widget.model().index(item.row(), 0),
                QItemSelectionModel.SelectionFlag.ClearAndSelect | QItemSelectionModel.SelectionFlag.Rows,
            )
        menu = QMenu(self)
        menu.addAction(self.edit_action)
        menu.addAction(self.delete_action)
        menu.exec(self.table_widget.viewport().mapToGlobal(position))

    def create_cell_dialog(self):
        self._edit_cell()

    def edit_selected_cell(self):
        record = self.get_selected_row_data()
        if record:
            self._edit_cell(record)

    def _edit_cell(self, record=None):
        old_id = record["id"] if record else None

        def save(data):
            if old_id is None:
                self.db.add_cell(data)
            else:
                self.db.update_cell(old_id, data)
                self._invalidate_cell(old_id)

        dialog = EditCellDialog(self, record, self.last_dir, save)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.show_library()
            if dialog.last_dir:
                self.last_dir = dialog.last_dir
                self.settings.setValue("last_file_directory", self.last_dir)
            self.load_data(select_id=dialog.saved_id)
            self.statusBar().showMessage(f"Saved battery {dialog.saved_id}.", 5000)

    def delete_selected_cell(self):
        record = self.get_selected_row_data()
        if not record:
            return
        cell_id = record["id"]
        confirm = QMessageBox.question(
            self, "Delete battery", f"Delete {cell_id} from this database?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if confirm == QMessageBox.StandardButton.Yes:
            try:
                self.db.delete_cell(cell_id)
            except sqlite3.DatabaseError as error:
                self.log_and_show_error("Could not delete battery", error)
                return
            self._invalidate_cell(cell_id)
            self.show_library()
            self.load_data()
            self.statusBar().showMessage(f"Deleted battery {cell_id}.", 5000)

    def _invalidate_cell(self, cell_id):
        self.preload_cache.pop(cell_id, None)
        self._load_errors.pop(cell_id, None)
        if self._load_request == cell_id:
            self._load_request = None

    def restore_last_database(self):
        last_db = self.settings.value("last_database_path", "", type=str)
        if last_db and os.path.isfile(last_db):
            try:
                self.db.switch_database(last_db)
            except sqlite3.DatabaseError:
                logger.exception("Could not reopen %s", last_db)
        elif last_db:
            self.settings.remove("last_database_path")

    def _set_window_title(self):
        path = os.path.abspath(self.db.db_name)
        name = os.path.basename(path)
        self.database_label.setText(name)
        self.database_label.setToolTip(path)
        self.setWindowTitle(f"Battery Data Manager · {name}")

    def _change_database(self, path, operation):
        if not path:
            return
        try:
            operation(path)
        except (OSError, sqlite3.DatabaseError) as error:
            self.log_and_show_error("Could not open database", error)
            return
        if self._file_scan is not None:
            self._file_scan.cancelled.set()
        self._load_request = None
        self.preload_cache.clear()
        self._load_errors.clear()
        self.settings.setValue("last_database_path", os.path.abspath(self.db.db_name))
        self.show_library()
        self.load_data(preserve_selection=False)
        self.statusBar().showMessage(f"Current database: {os.path.basename(self.db.db_name)}", 5000)
        self._queue_file_scan()

    def _queue_file_scan(self):
        self._pending_file_scan = {cell_id: row.get("file_path") for cell_id, row in self._cells.items()}
        if self._file_scan is not None:
            self._file_scan.cancelled.set()
        else:
            self._start_file_scan()

    def _start_file_scan(self):
        references = self._pending_file_scan
        self._pending_file_scan = None
        if not references:
            self._file_scan_timer.stop()
            return
        self._file_scan = FileStatusScan(references)
        self._file_scan_retry_until = None
        self._file_scan_timer.start(50)
        if self._data_load_future is None:
            self.statusBar().showMessage("Checking linked data files…")

    def _finish_file_scan(self):
        scan = self._file_scan
        if scan is None or not scan.future.done():
            return
        if not scan.cancelled.is_set():
            try:
                metadata = scan.future.result()
                statuses, updated, persisted = self.db.apply_file_scan(scan.references, metadata)
            except Exception as error:
                code = getattr(error, "sqlite_errorcode", 0) & 0xff
                if code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    now = time.monotonic()
                    if self._file_scan_retry_until is None:
                        self._file_scan_retry_until = now + 5
                    if now < self._file_scan_retry_until:
                        self._file_scan_timer.start(150)
                        return
                logger.exception("Could not check data file status")
                self.statusBar().showMessage("Could not check data file status. See the log for details.", 7000)
            else:
                self._show_file_status_changes(statuses)
                if self._data_load_future is None:
                    unavailable = sum(value is None for value in metadata.values())
                    message = f"Data file check: {updated:,} updated batter{'y' if updated == 1 else 'ies'}"
                    if unavailable:
                        message += f" · {unavailable:,} unavailable file(s)"
                    if not persisted:
                        message += " · Read-only database, status not saved"
                    self.statusBar().showMessage(message, 7000)
        self._file_scan = None
        self._file_scan_timer.stop()
        if self._pending_file_scan is not None:
            self._start_file_scan()

    def _show_file_status_changes(self, statuses):
        changed = {
            cell_id: status for cell_id, status in statuses.items()
            if cell_id in self._cells and self._cells[cell_id]["status"] != status
        }
        if not changed:
            return
        table = self.table_widget
        with QSignalBlocker(table), QSignalBlocker(table.horizontalHeader()):
            sorted_rows = table.isSortingEnabled()
            table.setSortingEnabled(False)
            for row in range(table.rowCount()):
                cell_id = self._cell_id_at(row)
                if cell_id in changed:
                    status = changed[cell_id]
                    self._cells[cell_id]["status"] = status
                    item = table.item(row, STATUS_COLUMN)
                    item.setText(status)
                    item.setToolTip(status)
            table.setSortingEnabled(sorted_rows)
        if self.status_filter.currentIndex() or self.search_input.text().strip():
            self.apply_filters()
        else:
            self.update_selected_cell()

    def new_database(self):
        path, _ = QFileDialog.getSaveFileName(self, "Create database", "", DATABASE_FILTER)
        self._change_database(path, self.db.switch_database)

    def open_database(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open database", "", DATABASE_FILTER)
        self._change_database(path, self.db.switch_database)

    def save_database(self):
        try:
            self.db.save()
        except sqlite3.DatabaseError as error:
            self.log_and_show_error("Could not save database", error)
        else:
            self.statusBar().showMessage("Database saved.", 5000)

    def save_as_database(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save database as", "", DATABASE_FILTER)
        self._change_database(path, self.db.save_as)

    def preload_data(self):
        record = self.get_selected_row_data()
        if record and not self._source_problem(record):
            self._start_data_load_worker(record)

    def open_analysis(self):
        selected = [cell["id"] for cell in self.selected_cells() if cell["id"]]
        if not 0 < len(selected) <= MAX_ANALYSIS_BATTERIES:
            return
        if self.analysis_page is None:
            self.analysis_page = AnalysisWindow(self.stack)
            self.analysis_page.set_batteries(self._cells)
            self.analysis_page.selection_requested.connect(self._analysis_selection_changed)
            self.analysis_page.back_requested.connect(self.show_library)
            self.analysis_page.export_requested.connect(self.save_processed_data)
            self.stack.addWidget(self.analysis_page)
        self.stack.setCurrentWidget(self.analysis_page)
        for cell_id in selected:
            self._load_errors.pop(cell_id, None)
        self.analysis_page.select_batteries(selected)
        self._refresh_analysis()
        self.update_selected_cell()

    def _analysis_selection_changed(self):
        for cell_id in self.analysis_page.selected_battery_ids:
            self._load_errors.pop(cell_id, None)
        self._refresh_analysis()

    def _refresh_analysis(self):
        page = self.analysis_page
        if page is None or self.stack.currentWidget() is not page or self._close_after_load:
            return
        ready, errors, pending = {}, {}, []
        for cell_id in page.selected_battery_ids:
            record = self._cells.get(cell_id)
            if record is None:
                continue
            if cell_id in self.preload_cache:
                ready[cell_id] = self.preload_cache[cell_id]
                self.preload_cache.move_to_end(cell_id)
            elif error := self._load_errors.get(cell_id) or self._source_problem(record):
                errors[cell_id] = error
            else:
                pending.append(cell_id)
        page.set_samples(ready, pending, errors)
        if pending and self._data_load_future is None:
            self._start_data_load_worker(self._cells[pending[0]])

    def _start_data_load_worker(self, record):
        if self._data_load_future is not None:
            return
        cell_id = record["id"]
        self._invalidate_cell(cell_id)
        self._load_request = cell_id
        self._data_load_future = self._data_loader.submit(
            self._load_and_process_cell, record["file_path"], record["instrument"], record["p_active_mass"]
        )
        self._load_timer.start()
        self.update_selected_cell()
        self.statusBar().showMessage(f"Loading {cell_id}…")

    def _handle_data_load_result(self, processed, error):
        cell_id = self._load_request
        self._load_request = None
        if cell_id is None:
            return
        if error or processed is None or processed.empty:
            self._load_errors[cell_id] = error or "The data file contains no usable rows."
            self.statusBar().showMessage(f"Could not load data for {cell_id}.", 7000)
            return

        self.preload_cache[cell_id] = processed
        if len(self.preload_cache) > MAX_ANALYSIS_BATTERIES:
            self.preload_cache.popitem(last=False)
        self.statusBar().showMessage(f"Loaded {cell_id} · {len(processed):,} rows.", 5000)

    def _finish_data_load(self):
        future = self._data_load_future
        if future is None or not future.done():
            return
        self._load_timer.stop()
        self._data_load_future = None
        try:
            data = future.result()
        except FileNotFoundError:
            self._handle_data_load_result(None, "Data file not found. Use Edit battery to update the file path.")
        except Exception as error:
            self._handle_data_load_result(None, str(error) or type(error).__name__)
        else:
            self._handle_data_load_result(data, "")
        if self._close_after_load:
            self.close()
        else:
            self.update_selected_cell()
            self._refresh_analysis()

    def show_library(self):
        self.stack.setCurrentWidget(self.library_page)
        self.update_selected_cell()

    def save_processed_data(self):
        if self.analysis_page is not None and self.stack.currentWidget() is self.analysis_page:
            selected = self.analysis_page.selected_battery_ids
            if len(selected) != 1:
                return
            cell_id = selected[0]
        else:
            record = self.get_selected_row_data()
            if not record:
                return
            cell_id = record["id"]
        data = self.preload_cache.get(cell_id)
        if data is None:
            return
        default_filename = f"{cell_id}_processed_raw_data.csv"
        filename, _ = QFileDialog.getSaveFileName(
            self, f"Export processed data · {cell_id}", default_filename, "CSV files (*.csv)"
        )
        if filename:
            try:
                data.to_csv(filename, index=False)
            except (OSError, ValueError) as error:
                self.log_and_show_error("Could not export processed data", error)
            else:
                self.statusBar().showMessage(f"Exported {cell_id} to {filename}", 7000)

    def log_and_show_error(self, message, error):
        logger.error("%s: %s", message, error, exc_info=True)
        QMessageBox.critical(self, "Battery Data Manager", f"{message}.\n\n{error}")

    def closeEvent(self, event):
        if self._data_load_future is not None:
            self._close_after_load = True
            self.statusBar().showMessage("Closing after the current data load finishes…")
            event.ignore()
            return
        self._data_loader.shutdown(wait=False)
        if self._file_scan is not None:
            self._file_scan.cancelled.set()
        self._file_scan_timer.stop()
        self._pending_file_scan = None
        self.settings.setValue("last_database_path", os.path.abspath(self.db.db_name))
        self.settings.setValue("window_geometry", self.saveGeometry())
        self.settings.setValue("library_splitter", self.library_splitter.saveState())
        self.db.close()
        super().closeEvent(event)

    def show_about(self):
        QMessageBox.information(self, "About Battery Data Manager", "Manage, analyze and plot battery test data.")

    def _load_and_process_cell(self, file_path, instrument_type, p_active_mass):
        """
        Unified data loading and processing pipeline.
        
        Args:
            file_path: Path to raw data file
            instrument_type: "Biologic", "Arbin", or "Maccor"
            p_active_mass: Positive electrode active mass in grams
        
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
            return df_clean
        except Exception as e:
            logger.error(f"Error during data cleaning: {e}", exc_info=True)
            raise


class EditCellDialog(QDialog):
    def __init__(self, parent, record, last_dir, save):
        super().__init__(parent)
        self.setWindowTitle("Edit battery" if record else "New battery")
        self.resize(570, 590)
        self.last_dir = last_dir
        self._save = save
        self.saved_id = None
        record = record or {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 15, 18, 15)
        form = QFormLayout()
        form.setVerticalSpacing(8)

        def field(key):
            value = record.get(key)
            return "" if value is None else str(value)

        self.id_input = QLineEdit(field("id"))
        self.instrument_input = self.combo_box(INSTRUMENTS, field("instrument"))
        date = QDate.fromString(field("date"), "yyyy-MM-dd")
        self.date_input = QDateEdit(date if date.isValid() else QDate.currentDate())
        self.date_input.setDisplayFormat("yyyy-MM-dd")
        self.date_input.setCalendarPopup(True)
        self.positive_input = self.combo_box(("NMC83", "NMC811", "LFP", "LCO", "Other"),
                                             field("positive_electrode"), editable=True)
        self.negative_input = self.combo_box(("150um Li", "Graphite", "Silicon", "Other"),
                                             field("negative_electrode"), editable=True)
        self.p_mass_input = QLineEdit(field("p_active_mass"))
        self.n_mass_input = QLineEdit(field("n_active_mass"))
        self.p_mass_input.setPlaceholderText("e.g. 0.005 for 5 mg")
        self.n_mass_input.setPlaceholderText("e.g. 0.005 for 5 mg")
        self.electrolyte_input = self.combo_box(("1M LiPF6 in FEC/DMC 1/4", "Gen 2", "Other"),
                                                field("electrolyte"), editable=True)
        self.format_input = self.combo_box(("Coin", "Pouch", "Swagelok", "Other"), field("format"))
        self.voltage_input = QLineEdit(field("voltage_range"))
        self.status_input = self.combo_box(STATUSES, field("status"))
        self.file_path_input = QLineEdit(field("file_path"))
        browse = QPushButton("Browse…")
        browse.clicked.connect(self.browse_file)
        file_row = QHBoxLayout()
        file_row.addWidget(self.file_path_input, 1)
        file_row.addWidget(browse)
        self.comment_input = QPlainTextEdit(field("comment"))
        self.comment_input.setFixedHeight(65)

        for label, widget in (
            ("Battery ID", self.id_input), ("Instrument", self.instrument_input),
            ("Test date", self.date_input), ("Status", self.status_input),
            ("Positive electrode", self.positive_input), ("Positive mass (g)", self.p_mass_input),
            ("Negative electrode", self.negative_input), ("Negative mass (g)", self.n_mass_input),
            ("Electrolyte", self.electrolyte_input), ("Cell format", self.format_input),
            ("Voltage range", self.voltage_input), ("Data file", file_row), ("Notes", self.comment_input),
        ):
            form.addRow(label, widget)

        layout.addLayout(form, 1)
        self.validation_text = QLabel()
        self.validation_text.setWordWrap(True)
        self.validation_text.setTextFormat(Qt.TextFormat.PlainText)
        self.validation_text.hide()
        layout.addWidget(self.validation_text)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.save_record)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    @staticmethod
    def combo_box(choices, value, *, editable=False):
        box = QComboBox()
        box.setEditable(editable)
        box.addItems(choices)
        if editable:
            box.setEditText(value)
        elif value:
            if box.findText(value) < 0:
                box.addItem(value)
            box.setCurrentText(value)
        return box

    def browse_file(self):
        filters = DATA_FILE_FILTERS.get(self.instrument_input.currentText(), "All files (*)")
        filename, _ = QFileDialog.getOpenFileName(self, "Choose instrument data file", self.last_dir, filters)
        if filename:
            self.file_path_input.setText(filename)
            self.last_dir = os.path.dirname(filename)

    def show_validation(self, message, field=None):
        self.validation_text.setText(message)
        self.validation_text.show()
        if field is not None:
            field.setFocus()
            field.selectAll()

    def save_record(self):
        cell_id = self.id_input.text().strip()
        if not cell_id:
            self.show_validation("Enter a battery ID.", self.id_input)
            return
        masses = {}
        for key, field, description in (
            ("p_active_mass", self.p_mass_input, "positive"),
            ("n_active_mass", self.n_mass_input, "negative"),
        ):
            text = field.text().strip()
            try:
                value = float(text) if text else 0.0
            except ValueError:
                value = math.nan
            if not math.isfinite(value) or value < 0:
                self.show_validation(f"Enter a nonnegative {description} mass in grams.", field)
                return
            masses[key] = value

        data = {
            "id": cell_id,
            "instrument": self.instrument_input.currentText(),
            "date": self.date_input.date().toString("yyyy-MM-dd"),
            "positive_electrode": self.positive_input.currentText().strip(),
            "negative_electrode": self.negative_input.currentText().strip(),
            **masses,
            "electrolyte": self.electrolyte_input.currentText().strip(),
            "format": self.format_input.currentText(),
            "voltage_range": self.voltage_input.text().strip(),
            "status": self.status_input.currentText(),
            "file_path": self.file_path_input.text().strip(),
            "comment": self.comment_input.toPlainText(),
        }
        try:
            self._save(data)
        except sqlite3.IntegrityError:
            self.show_validation(f"Battery ID {cell_id} already exists. Use a different ID or edit the existing record.",
                                 self.id_input)
        except sqlite3.DatabaseError as error:
            logger.exception("Could not save battery %s", cell_id)
            self.show_validation(f"Could not save this battery. {error}")
        else:
            self.saved_id = cell_id
            self.accept()
