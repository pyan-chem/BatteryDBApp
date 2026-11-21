import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from threading import Event
from time import monotonic, sleep
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pandas as pd
from matplotlib.backend_bases import MouseEvent
from PySide6.QtCore import QItemSelectionModel, QSettings, Qt
from PySide6.QtWidgets import QApplication, QPushButton, QToolTip

from database import BatteryDatabase
from ui import main_window
from ui.analysis_window import AnalysisWindow


class BatteryUITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        ini_file = str(self.directory / "settings.ini")
        self.database = BatteryDatabase(str(self.directory / "library.db"))
        self.rows = [
            {"id": "SAMPLE-A", "instrument": "Arbin", "positive_electrode": "NMC811",
             "file_path": str(self.directory / "sample-a.res"), "p_active_mass": 0.005,
             "status": "Active"},
            {"id": "SAMPLE-B", "instrument": "Maccor", "positive_electrode": "LFP",
             "file_path": str(self.directory / "sample-b.001"), "p_active_mass": 0.006,
             "status": "Stopped"},
        ]
        for row in self.rows:
            self.database.add_cell(row)
        with patch.object(main_window, "BatteryDatabase", return_value=self.database), patch.object(
            main_window, "QSettings", side_effect=lambda *args: QSettings(ini_file, QSettings.Format.IniFormat)
        ):
            self.window = main_window.MainWindow()
        self.addCleanup(self.database.close)
        self.addCleanup(self.window.close)
        self.data = pd.DataFrame(
            {"cycle_number": [0, 1, 1, 3, 3], "time_h": [0., 1., 2., 3., 4.], "voltage_V": [3., 3.4, 3.5, 3.4, 3.5]}
        )

    def tearDown(self):
        # Finish queued canvas paints before unittest releases their widgets.
        self.app.processEvents()

    def select(self, cell_id):
        table = self.window.table_widget
        for row in range(table.rowCount()):
            if table.item(row, 0).text() == cell_id:
                table.selectRow(row)
                return
        self.fail(f"Cell {cell_id} missing from table")

    def extend_selection(self, cell_id):
        table = self.window.table_widget
        for row in range(table.rowCount()):
            if table.item(row, 0).text() == cell_id:
                table.selectionModel().select(
                    table.model().index(row, 0),
                    QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows,
                )
                return
        self.fail(f"Cell {cell_id} missing from table")

    def visible_ids(self):
        table = self.window.table_widget
        return [table.item(row, 0).text() for row in range(table.rowCount()) if not table.isRowHidden(row)]

    def wait_for(self, condition):
        deadline = monotonic() + 8
        while not condition() and monotonic() < deadline:
            self.app.processEvents()
            sleep(0.01)
        self.assertTrue(condition(), "Timed out waiting for the interface to update")

    def test_search_and_filters_keep_matching_record(self):
        search = getattr(self.window, "search_input", None)
        self.assertIsNotNone(search, "The library needs a search field")
        search.setText("lfp")
        self.assertEqual(self.visible_ids(), ["SAMPLE-B"])
        self.window.table_widget.sortItems(0, Qt.SortOrder.DescendingOrder)
        self.assertEqual(self.visible_ids(), ["SAMPLE-B"])
        self.select("SAMPLE-B")
        self.assertIn("sample-b.001", self.window.details_text.text())
        self.window.instrument_filter.setCurrentText("Arbin")
        self.assertEqual(self.visible_ids(), [])
        self.assertIsNone(self.window.get_selected_row_data())
        self.assertFalse(self.window.plot_btn.isEnabled())

    def test_selected_details_give_values_space_to_render(self):
        self.window.resize(950, 650)
        self.window.show()
        self.select("SAMPLE-A")
        self.app.processEvents()
        material = self.window.detail_values["positive_electrode"]
        mass = self.window.detail_values["p_active_mass"]
        self.assertEqual(material.text(), "NMC811")
        self.assertEqual(mass.text(), "0.005")
        for widget in (material, mass):
            self.assertTrue(widget.isVisible())
            self.assertGreaterEqual(widget.width(), widget.fontMetrics().horizontalAdvance(widget.text()))

    def test_startup_and_database_open_scan_visible_statuses(self):
        self.wait_for(lambda: self.window._cells["SAMPLE-A"]["status"] == "Stopped")
        second = self.directory / "other.db"
        path = self.directory / "tracked.res"
        path.write_bytes(b"baseline")
        with BatteryDatabase(second) as database:
            database.add_cell({**self.rows[0], "file_path": str(path)})

        def open_second():
            with patch.object(main_window.QFileDialog, "getOpenFileName", return_value=(str(second), "")):
                self.window.open_database()

        open_second()
        self.wait_for(lambda: self.window._cells["SAMPLE-A"]["status"] == "Stopped")
        path.write_bytes(b"changed length")
        open_second()
        self.wait_for(lambda: self.window._cells["SAMPLE-A"]["status"] == "Update")
        self.select("SAMPLE-A")
        self.assertIn("Update", self.window.detail_summary.text())
        self.window.status_filter.setCurrentText("Update")
        self.assertEqual(self.visible_ids(), ["SAMPLE-A"])

    def test_scan_results_are_discarded_on_database_switch(self):
        from data_import.file_status import file_key, scan_file_metadata

        self.wait_for(lambda: self.window._file_scan is None)
        path = Path(self.rows[0]["file_path"])
        path.write_bytes(b"actual contents")
        second = self.directory / "other.db"
        refs = {"SAMPLE-A": str(path)}
        with BatteryDatabase(second) as database:
            database.add_cell(self.rows[0])
            database.apply_file_scan(refs, scan_file_metadata(refs.values()))

        first_started = Event()
        release_first = Event()

        def scan(paths, cancelled):
            if not first_started.is_set():
                first_started.set()
                release_first.wait(3)
                return {file_key(str(path)): (1, 1)}
            return scan_file_metadata(paths, cancelled)

        with patch("data_import.file_status.scan_file_metadata", side_effect=scan):
            self.window._queue_file_scan()
            try:
                self.wait_for(first_started.is_set)
                with patch.object(main_window.QFileDialog, "getOpenFileName", return_value=(str(second), "")):
                    self.window.open_database()
            finally:
                release_first.set()
            self.wait_for(lambda: self.window._file_scan is None)
        self.assertEqual(self.window._cells["SAMPLE-A"]["status"], "Stopped")
        self.assertEqual(self.window.db.get_cell("SAMPLE-A")[11], "Stopped")

    def test_file_scan_retries_when_writer_lock_is_released(self):
        scan = self.window._file_scan
        scan.future.result(timeout=2)
        lock = sqlite3.connect(self.database.db_name)
        try:
            lock.execute("BEGIN IMMEDIATE")
            self.window._finish_file_scan()
            self.assertIs(self.window._file_scan, scan)
            self.assertEqual(self.window._cells["SAMPLE-A"]["status"], "Active")
        finally:
            lock.close()
        self.wait_for(lambda: self.window._cells["SAMPLE-A"]["status"] == "Stopped")

    def test_export_button_follows_selected_cell(self):
        self.select("SAMPLE-A")
        with patch.object(self.window, "_load_and_process_cell", return_value=self.data):
            self.window.preload_btn.click()
            self.wait_for(self.window.save_processed_btn.isEnabled)
        self.select("SAMPLE-B")
        self.assertFalse(self.window.save_processed_btn.isEnabled())

    def test_switching_library_drops_cache_for_identical_ids(self):
        self.window.preload_cache["SAMPLE-A"] = self.data
        second = self.directory / "second.db"
        with BatteryDatabase(second) as database:
            database.add_cell({**self.rows[0], "file_path": str(self.directory / "different.res")})
        with patch.object(main_window.QFileDialog, "getOpenFileName", return_value=(str(second), "")):
            self.window.open_database()
        self.select("SAMPLE-A")
        self.assertNotIn("SAMPLE-A", self.window.preload_cache)
        self.assertFalse(self.window.save_processed_btn.isEnabled())

    def test_in_flight_analysis_cannot_attach_to_same_id_in_new_database(self):
        release = Event()
        second = self.directory / "second.db"
        with BatteryDatabase(second) as database:
            database.add_cell({**self.rows[0], "file_path": str(self.directory / "alt.res")})

        def load(*args):
            release.wait(3)
            return self.data

        self.select("SAMPLE-A")
        with patch.object(self.window, "_load_and_process_cell", side_effect=load):
            self.window.plot_btn.click()
            try:
                with patch.object(main_window.QFileDialog, "getOpenFileName", return_value=(str(second), "")):
                    self.window.open_database()
                self.select("SAMPLE-A")
            finally:
                release.set()
            self.wait_for(self.window.plot_btn.isEnabled)
        self.assertIs(self.window.stack.currentWidget(), self.window.library_page)
        self.assertNotIn("SAMPLE-A", self.window.preload_cache)
        self.assertFalse(self.window.save_processed_btn.isEnabled())

    def test_maccor_file_can_be_plotted_and_exported_from_analysis(self):
        source = Path(self.rows[1]["file_path"])
        source.write_text(
            "Synthetic fixture\nTest Time (sec)\tCapacity (AHr)\tCurrent (A)\tVoltage (V)\tCycle P\tMD\tEnergy (WHr)\n"
            "0\t0.000\t0.01\t3.12\t1\tC\t0\n3600\t0.001\t0.01\t3.63\t1\tC\t0.00363\n",
            encoding="utf-8",
        )
        self.select("SAMPLE-B")
        self.window.plot_btn.click()
        self.wait_for(lambda: self.window.analysis_page is not None and self.window.analysis_page.preview_btn.isEnabled())
        analysis = self.window.analysis_page
        analysis.preview_btn.click()
        self.assertIn("SAMPLE-B", analysis.figure.axes[0].get_title())
        self.assertEqual(list(analysis.figure.axes[0].lines[0].get_ydata()), [3.12, 3.63])
        analysis.extract_btn.click()
        copied = self.app.clipboard().text().replace("\r\n", "\n")
        self.assertIn("SAMPLE-B\n0.0\t3.12\n1.0\t3.63", copied)

        destination = self.directory / "exported.csv"
        with patch.object(main_window.QFileDialog, "getSaveFileName", return_value=(str(destination), "")) as dialog:
            export = next(button for button in analysis.findChildren(QPushButton) if button.text().startswith("Export"))
            export.click()
        self.assertIn("SAMPLE-B", dialog.call_args.args[1])
        exported = pd.read_csv(destination)
        self.assertEqual(exported["voltage_V"].tolist(), [3.12, 3.63])
        self.assertAlmostEqual(exported["charge_cap_mAh_g"].iloc[-1], 1000 / 6)

    def test_analysis_embeds_plot_and_marks_missing_cycles(self):
        analysis = AnalysisWindow(processed_data=self.data, cell_id="SAMPLE-A")
        self.addCleanup(analysis.close)
        canvas = getattr(analysis, "canvas", None)
        self.assertIsNotNone(canvas, "Analysis needs an embedded plot")
        self.assertEqual(analysis.parse_cycles(), [1, 3])
        analysis.all_cycles_chk.setChecked(False)
        analysis.cycles_input.setText("99")
        self.assertIsNone(analysis.parse_cycles())
        self.assertFalse(analysis.preview_btn.isEnabled())
        analysis.cycles_input.setText("0,3")
        analysis.preview_btn.click()
        self.assertEqual([list(line.get_xdata()) for line in canvas.figure.axes[0].lines], [[0.], [3., 4.]])

    def test_missing_voltage_sample_leaves_a_gap_in_the_curve(self):
        data = pd.DataFrame({"cycle_number": [1, 1, 1], "time_h": [0., 1., 2.],
                             "voltage_V": [3., float("nan"), 4.]})
        analysis = AnalysisWindow(processed_data=data, cell_id="SAMPLE-A")
        self.addCleanup(analysis.close)
        analysis.preview_btn.click()
        line, = analysis.figure.axes[0].lines
        self.assertEqual(list(line.get_xdata()), [0., 1., 2.])
        self.assertTrue(pd.isna(line.get_ydata()[1]))

    def test_analysis_can_change_battery_without_leaving_workspace(self):
        self.window.preload_cache["SAMPLE-A"] = self.data
        self.window.preload_cache["SAMPLE-B"] = self.data.assign(voltage_V=[2.9, 3.1, 3.2, 3.3, 3.4])
        self.select("SAMPLE-A")
        self.window.open_analysis()
        page = self.window.analysis_page
        page.all_cycles_chk.setChecked(False)
        page.cycles_input.setText("3")
        page.preview_btn.click()
        picker = getattr(page, "sample_list", None)
        self.assertIsNotNone(picker, "Select another battery inside the analysis workspace")
        picker.setCurrentRow(1)
        self.assertIs(self.window.stack.currentWidget(), page)
        self.assertEqual(page.cycles_input.text(), "3")
        self.assertFalse(page.all_cycles_chk.isChecked())
        line, = page.figure.axes[0].lines
        self.assertEqual(list(line.get_ydata()), [3.3, 3.4])
        self.assertIn("SAMPLE-B", page.figure.axes[0].get_title())

    def test_library_can_plot_two_batteries_with_a_distinct_legend(self):
        self.select("SAMPLE-A")
        self.extend_selection("SAMPLE-B")
        sample_b = self.data.assign(voltage_V=[2.9, 3.1, 3.2, 3.3, 3.4])

        def load(path, *args):
            return self.data if path == self.rows[0]["file_path"] else sample_b

        with patch.object(self.window, "_load_and_process_cell", side_effect=load):
            self.window.plot_btn.click()
            page = self.window.analysis_page
            self.assertIsNotNone(page, "Open the workspace while selected data loads")
            self.wait_for(page.preview_btn.isEnabled)
        page.all_cycles_chk.setChecked(False)
        page.cycles_input.setText("3")
        page.preview_btn.click()
        axes = page.figure.axes[0]
        self.assertEqual([item.get_text() for item in axes.get_legend().get_texts()], ["SAMPLE-A", "SAMPLE-B"])
        self.assertEqual(len({line.get_color() for line in axes.lines}), 2)
        self.assertEqual([list(line.get_ydata()) for line in axes.lines], [[3.4, 3.5], [3.3, 3.4]])
        page.extract_btn.click()
        copied = self.app.clipboard().text()
        self.assertIn("Battery ID\t", copied)
        self.assertIn("SAMPLE-A\t3\t3.0\t3.4", copied)
        self.assertIn("SAMPLE-B\t3\t3.0\t3.3", copied)

    def test_analysis_selection_caps_comparison_at_ten(self):
        for index in range(11):
            row = {**self.rows[0], "id": f"COMPARE-{index:02d}"}
            self.database.add_cell(row)
        self.window.load_data()
        self.select("COMPARE-00")
        for index in range(1, 11):
            self.extend_selection(f"COMPARE-{index:02d}")
        self.assertFalse(self.window.plot_btn.isEnabled())

        self.window.table_widget.clearSelection()
        for index in range(10):
            cell_id = f"COMPARE-{index:02d}"
            self.window.preload_cache[cell_id] = self.data
            self.extend_selection(cell_id)
        self.assertTrue(self.window.plot_btn.isEnabled())
        self.window.plot_btn.click()
        page = self.window.analysis_page
        page.preview_btn.click()
        legend = page.figure.axes[0].get_legend()
        self.assertEqual([item.get_text() for item in legend.texts], [f"COMPARE-{i:02d}" for i in range(10)])
        self.assertEqual(len({handle.get_color() for handle in legend.legend_handles}), 10)
        page.sample_list.item(10).setSelected(True)
        self.assertFalse(page.sample_list.item(10).isSelected())
        self.assertEqual(len(page.selected_battery_ids), 10)

    def test_searching_batteries_preserves_previous_comparison_selection(self):
        for cell_id in ("SAMPLE-A", "SAMPLE-B"):
            self.window.preload_cache[cell_id] = self.data
        self.select("SAMPLE-A")
        self.window.open_analysis()
        page = self.window.analysis_page
        page.sample_search.setText("sample-b")
        page.sample_list.item(1).setSelected(True)
        self.assertEqual(page.selected_battery_ids, ("SAMPLE-A", "SAMPLE-B"))
        page.preview_btn.click()
        self.assertEqual([item.get_text() for item in page.figure.axes[0].get_legend().texts], ["SAMPLE-A", "SAMPLE-B"])

    def test_battery_with_missing_file_does_not_block_other_curves(self):
        self.window.preload_cache["SAMPLE-A"] = self.data
        self.select("SAMPLE-A")
        self.extend_selection("SAMPLE-B")
        with self.assertLogs("ui.main_window", level="ERROR"):
            self.window.plot_btn.click()
            page = self.window.analysis_page
            self.wait_for(page.preview_btn.isEnabled)
        page.preview_btn.click()
        self.assertIn("SAMPLE-B: Data file not found", page.sample_status.text())
        self.assertEqual([item.get_text() for item in page.figure.axes[0].get_legend().texts], ["SAMPLE-A"])

    def test_reopening_the_same_battery_retries_a_failed_read(self):
        self.select("SAMPLE-A")
        with patch.object(self.window, "_load_and_process_cell",
                          side_effect=[FileNotFoundError("temporary"), self.data]):
            self.window.open_analysis()
            page = self.window.analysis_page
            self.wait_for(lambda: "Data file not found" in page.sample_status.text())
            self.window.show_library()
            self.window.open_analysis()
            self.assertIs(self.window.analysis_page, page)
            self.wait_for(page.preview_btn.isEnabled)

    def test_hover_shows_sample_and_actual_coordinates(self):
        analysis = AnalysisWindow(processed_data=self.data, cell_id="SAMPLE-A")
        self.addCleanup(analysis.close)
        analysis.resize(940, 640)
        analysis.show()
        analysis.preview_btn.click()
        self.app.processEvents()
        axes = analysis.figure.axes[0]
        x, y = axes.transData.transform((2., 3.5))
        event = MouseEvent("motion_notify_event", analysis.canvas, x, y)
        analysis.canvas.callbacks.process("motion_notify_event", event)
        tip = QToolTip.text()
        self.assertIn("SAMPLE-A", tip)
        self.assertIn("Cycle 1", tip)
        self.assertIn("Time (h): 2", tip)
        self.assertIn("Voltage (V): 3.5", tip)

        analysis.toolbar.zoom()
        axes.set_xlim(2.8, 3.5)
        axes.set_ylim(3.35, 3.45)
        analysis.canvas.draw()
        x, y = axes.transData.transform((3., 3.4))
        analysis.canvas.callbacks.process("motion_notify_event", MouseEvent("motion_notify_event", analysis.canvas, x, y))
        tip = QToolTip.text()
        self.assertIn("Cycle 3", tip)
        self.assertIn("Time (h): 3", tip)
        self.assertIn("Voltage (V): 3.4", tip)
