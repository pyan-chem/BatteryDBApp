import logging
import re
from bisect import bisect_left, bisect_right

import numpy as np
import pandas as pd
from matplotlib import colormaps
from matplotlib.lines import Line2D
from PySide6.QtCore import QItemSelectionModel, QSignalBlocker, Qt, Signal
from PySide6.QtWidgets import (
    QApplication, QCheckBox, QComboBox, QFormLayout, QFrame, QHBoxLayout, QLabel,
    QLineEdit, QListWidget, QListWidgetItem, QPushButton, QScrollArea, QSpinBox, QSplitter,
    QVBoxLayout, QWidget,
)
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg, NavigationToolbar2QT
from matplotlib.figure import Figure

from analysis.plot_data import PLOTS, analyze_plot
from ui.point_inspector import PointInspector


logger = logging.getLogger(__name__)
MAX_ANALYSIS_BATTERIES = 10
SAMPLE_COLORS = colormaps["tab10"].colors


class AnalysisWindow(QWidget):
    back_requested = Signal()
    export_requested = Signal()
    selection_requested = Signal()

    def __init__(self, parent=None, processed_data=None, cell_id=None):
        super().__init__(parent)
        self.samples = {cell_id: processed_data} if processed_data is not None else {}
        self.selected_battery_ids = tuple(self.samples)
        self._pending = ()
        self._load_errors = {}
        self._has_loaded_sample = bool(self.samples)
        self.available_cycles = self._available_cycles()
        self._repeat_plot = False
        self._result = None
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(22, 18, 22, 14)
        layout.setSpacing(13)
        header = QHBoxLayout()
        back = QPushButton("Back to batteries")
        back.clicked.connect(self.back_requested)
        self.title = QLabel()
        self.title.setObjectName("pageTitle")
        self.title.setTextFormat(Qt.TextFormat.PlainText)
        self.title.setWordWrap(True)
        self.export_btn = QPushButton("Export processed CSV…")
        self.export_btn.clicked.connect(self.export_requested)
        header.addWidget(back)
        header.addWidget(self.title, 1)
        header.addWidget(self.export_btn)
        layout.addLayout(header)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setChildrenCollapsible(False)
        splitter.setHandleWidth(12)
        layout.addWidget(splitter, 1)

        controls = QFrame()
        controls.setObjectName("analysisControls")
        controls.setMinimumWidth(250)
        controls_layout = QVBoxLayout(controls)
        controls_layout.setContentsMargins(16, 15, 16, 15)
        controls_layout.setSpacing(8)
        controls_layout.addWidget(self._create_sample_picker())
        type_label = QLabel("&Analysis type")
        self.analysis_type_combo = QComboBox()
        self.analysis_type_combo.addItems(PLOTS.keys())
        type_label.setBuddy(self.analysis_type_combo)
        controls_layout.addWidget(type_label)
        controls_layout.addWidget(self.analysis_type_combo)

        self.cycle_availability = QLabel()
        self.cycle_availability.setWordWrap(True)
        controls_layout.addWidget(self.cycle_availability)
        self.all_cycles_chk = QCheckBox("All test cycles")
        self.all_cycles_chk.setChecked(self.available_cycles != [0])
        controls_layout.addWidget(self.all_cycles_chk)
        self.cycles_input = QLineEdit("0" if self.available_cycles == [0] else "")
        self.cycles_input.setAccessibleName("Cycle numbers")
        self.cycles_input.setPlaceholderText("e.g. 1,3,5 or 1–5")
        controls_layout.addWidget(self.cycles_input)
        self.zero_note = QLabel("Enter 0 to include the pre-cycle / rest segment.")
        self.zero_note.setWordWrap(True)
        controls_layout.addWidget(self.zero_note)

        self.interpolation_settings = QWidget()
        interpolation_layout = QFormLayout(self.interpolation_settings)
        interpolation_layout.setContentsMargins(0, 0, 0, 0)
        self.interp_chk = QCheckBox("Interpolate dQ/dV")
        self.interp_points_input = QSpinBox()
        self.interp_points_input.setRange(2, 1000000)
        self.interp_points_input.setValue(1000)
        interpolation_layout.addRow(self.interp_chk)
        interpolation_layout.addRow("Points", self.interp_points_input)
        controls_layout.addWidget(self.interpolation_settings)

        self.preview_btn = QPushButton("Plot selected cycles")
        self.preview_btn.setProperty("primary", True)
        self.preview_btn.clicked.connect(self.data_preview)
        self.extract_btn = QPushButton("Copy plotted data")
        self.extract_btn.clicked.connect(self.extract_data)
        controls_layout.addWidget(self.preview_btn)
        controls_layout.addWidget(self.extract_btn)
        self.feedback = QLabel()
        self.feedback.setTextFormat(Qt.TextFormat.PlainText)
        self.feedback.setWordWrap(True)
        self.feedback.setAccessibleName("Analysis status")
        controls_layout.addWidget(self.feedback)
        controls_layout.addStretch()

        chart_panel = QWidget()
        chart_layout = QVBoxLayout(chart_panel)
        chart_layout.setContentsMargins(0, 0, 0, 0)
        chart_layout.setSpacing(3)
        self.figure = Figure(figsize=(7, 5), layout="constrained")
        self.canvas = FigureCanvasQTAgg(self.figure)
        self.canvas.setAccessibleName("Analysis chart")
        self.canvas.setMinimumSize(250, 250)
        self.toolbar = NavigationToolbar2QT(self.canvas, chart_panel, coordinates=False)
        self.canvas.setMouseTracking(True)
        self.point_inspector = PointInspector(self.canvas)
        chart_layout.addWidget(self.toolbar)
        chart_layout.addWidget(self.canvas, 1)
        control_scroll = QScrollArea()
        control_scroll.setFrameShape(QFrame.Shape.NoFrame)
        control_scroll.setWidgetResizable(True)
        control_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        control_scroll.setMinimumWidth(265)
        control_scroll.setWidget(controls)
        splitter.addWidget(control_scroll)
        splitter.addWidget(chart_panel)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([290, 880])

        for signal in (self.analysis_type_combo.currentTextChanged, self.all_cycles_chk.toggled,
                       self.cycles_input.textChanged, self.interp_chk.toggled,
                       self.interp_points_input.valueChanged):
            signal.connect(self.update_controls)
        self.cycles_input.returnPressed.connect(self.data_preview)
        self._update_sample_info()
        self.clear_plot()
        self.update_controls()

    def _create_sample_picker(self):
        self.sample_picker = QWidget()
        layout = QVBoxLayout(self.sample_picker)
        layout.setContentsMargins(0, 0, 0, 8)
        layout.setSpacing(6)
        label = QLabel("&Batteries")
        self.sample_search = QLineEdit()
        self.sample_search.setAccessibleName("Search analysis batteries")
        self.sample_search.setPlaceholderText("Search batteries…")
        self.sample_search.setClearButtonEnabled(True)
        self.sample_list = QListWidget()
        self.sample_list.setAccessibleName("Batteries to analyze")
        self.sample_list.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        self.sample_list.setMinimumHeight(90)
        self.sample_list.setMaximumHeight(128)
        label.setBuddy(self.sample_list)
        self.sample_status = QLabel()
        self.sample_status.setWordWrap(True)
        self.sample_status.setTextFormat(Qt.TextFormat.PlainText)
        tip = QLabel("Click to switch. Ctrl/Shift to compare.")
        tip.setWordWrap(True)
        layout.addWidget(label)
        layout.addWidget(self.sample_search)
        layout.addWidget(self.sample_list)
        layout.addWidget(tip)
        layout.addWidget(self.sample_status)
        self.sample_search.textChanged.connect(self._filter_batteries)
        self.sample_list.itemSelectionChanged.connect(self._on_sample_selection)
        self.sample_picker.hide()
        return self.sample_picker

    def set_batteries(self, records, *, preserve_selection=True):
        selected = self.selected_battery_ids if preserve_selection else ()
        with QSignalBlocker(self.sample_list), QSignalBlocker(self.sample_search):
            if not preserve_selection:
                self.sample_search.clear()
            self.sample_list.clear()
            for cell_id in sorted((key for key in records if key), key=str.casefold):
                record = records[cell_id]
                item = QListWidgetItem(cell_id)
                details = " · ".join(str(record[key]) for key in ("instrument", "date", "positive_electrode")
                                     if record.get(key))
                item.setData(Qt.ItemDataRole.UserRole, f"{cell_id} {details}".casefold())
                item.setToolTip(details)
                self.sample_list.addItem(item)
            self._filter_batteries()
            self._set_list_selection(selected)
        self.sample_picker.show()
        self._on_sample_selection()
        if not self.selected_battery_ids:
            self.set_samples({}, (), {})

    def _filter_batteries(self):
        terms = self.sample_search.text().casefold().split()
        for row in range(self.sample_list.count()):
            item = self.sample_list.item(row)
            item.setHidden(not all(term in item.data(Qt.ItemDataRole.UserRole) for term in terms))

    def select_batteries(self, cell_ids):
        if len(cell_ids) > MAX_ANALYSIS_BATTERIES:
            return
        with QSignalBlocker(self.sample_list), QSignalBlocker(self.sample_search):
            self.sample_search.clear()
            self._filter_batteries()
            self._set_list_selection(cell_ids)
        self._on_sample_selection()
        if self.sample_list.currentItem() is not None:
            self.sample_list.scrollToItem(self.sample_list.currentItem())

    def _set_list_selection(self, cell_ids):
        selection = self.sample_list.selectionModel()
        selection.clearSelection()
        selected_rows = []
        for row in range(self.sample_list.count()):
            if self.sample_list.item(row).text() in cell_ids:
                index = self.sample_list.model().index(row, 0)
                selection.select(index, QItemSelectionModel.SelectionFlag.Select)
                selected_rows.append(row)
        self.sample_list.setCurrentRow(selected_rows[0] if selected_rows else -1,
                                       QItemSelectionModel.SelectionFlag.NoUpdate)

    def _on_sample_selection(self):
        selected_rows = self.sample_list.selectionModel().selectedRows()
        selected = tuple(self.sample_list.item(index.row()).text()
                         for index in sorted(selected_rows, key=lambda index: index.row()))
        if len(selected) > MAX_ANALYSIS_BATTERIES:
            with QSignalBlocker(self.sample_list):
                self._set_list_selection(self.selected_battery_ids)
            self.sample_status.setText(f"Select up to {MAX_ANALYSIS_BATTERIES} batteries.")
            return
        if selected != self.selected_battery_ids:
            self.selected_battery_ids = selected
            self.selection_requested.emit()
        self._update_sample_info()

    def _available_cycles(self):
        cycles = set()
        for data in self.samples.values():
            if "cycle_number" in data:
                cycles.update(int(cycle) for cycle in data["cycle_number"].dropna().unique())
        return sorted(cycles)

    def set_samples(self, ready, pending, errors):
        pending = tuple(pending)
        same_samples = tuple(ready) == tuple(self.samples) and all(
            frame is self.samples[cell_id] for cell_id, frame in ready.items()
        )
        changed = not same_samples or pending != self._pending or errors != self._load_errors
        self.samples = ready
        self._pending = pending
        self._load_errors = errors
        self._update_sample_info()
        if not changed:
            return
        if not same_samples:
            self.available_cycles = self._available_cycles()
        if self.samples and not pending and not self._has_loaded_sample:
            self._has_loaded_sample = True
            if self.available_cycles == [0] and self.all_cycles_chk.isChecked():
                with QSignalBlocker(self.all_cycles_chk), QSignalBlocker(self.cycles_input):
                    self.all_cycles_chk.setChecked(False)
                    self.cycles_input.setText("0")
        self.update_controls()
        if self._repeat_plot and self.preview_btn.isEnabled():
            self.data_preview()

    def _update_sample_info(self):
        count = len(self.selected_battery_ids)
        if count == 1:
            self.title.setText(f"Analysis · {self.selected_battery_ids[0]}")
        elif count:
            self.title.setText(f"Analysis · {count} batteries")
        else:
            self.title.setText("Analysis")
        self.export_btn.setEnabled(count == 1 and self.selected_battery_ids[0] in self.samples)
        self.export_btn.setToolTip("Select one battery to export its processed CSV." if count != 1 else "")
        message = f"{count} / {MAX_ANALYSIS_BATTERIES} selected"
        if self._pending:
            message += f" · Loading {self._pending[0]}…"
        if self._load_errors:
            cell_id, error = next(iter(self._load_errors.items()))
            message += f"\n{cell_id}: {error}"
            if len(self._load_errors) > 1:
                message += f"\n{len(self._load_errors)} batteries unavailable."
        self.sample_status.setText(message)
        self.sample_status.setToolTip("\n".join(f"{cell_id}: {error}" for cell_id, error in self._load_errors.items()))

    def format_cycle_ranges(self):
        if not self.available_cycles:
            return ""
        start = end = self.available_cycles[0]
        ranges = []
        for cycle in self.available_cycles[1:]:
            if cycle == end + 1:
                end = cycle
            else:
                ranges.append(str(start) if start == end else f"{start}–{end}")
                start = end = cycle
        ranges.append(str(start) if start == end else f"{start}–{end}")
        if len(ranges) > 8:
            return ", ".join(ranges[:7]) + f", …, {ranges[-1]}"
        return ", ".join(ranges)

    def parse_cycles(self):
        cycles, _ = self._read_cycle_selection()
        return cycles

    def _read_cycle_selection(self):
        if self._pending:
            return None, "Waiting for the selected batteries to load."
        if not self.available_cycles:
            return None, "No cycles are available to plot."
        if self.all_cycles_chk.isChecked():
            cycles = [cycle for cycle in self.available_cycles if cycle != 0]
            return (cycles, "") if cycles else (None, "Only Cycle 0 is available. Enter 0 to plot it.")
        text = self.cycles_input.text().strip()
        if not text:
            return None, "Enter cycle numbers or ranges."
        cycles = set()
        unavailable = []
        for part in text.split(","):
            part = part.strip()
            match = re.fullmatch(r"(\d{1,18})(?:\s*[-–]\s*(\d{1,18}))?", part)
            if not match:
                return None, "Use comma-separated cycle numbers or ranges, such as 1,3,5–10."
            first = int(match[1])
            last = int(match[2] or match[1])
            if first > last:
                return None, "A cycle range must start at or below its end."
            start = bisect_left(self.available_cycles, first)
            stop = bisect_right(self.available_cycles, last)
            if start == stop:
                unavailable.append(part)
            else:
                cycles.update(self.available_cycles[start:stop])
        if not cycles:
            return None, "No recorded cycles match this selection."
        notice = f"Unavailable cycle entries: {', '.join(unavailable)}." if unavailable else ""
        return sorted(cycles), notice

    def update_controls(self):
        self.cycles_input.setEnabled(not self.all_cycles_chk.isChecked())
        dq_dv_selected = self.analysis_type_combo.currentText() == "dQ/dV"
        self.interpolation_settings.setVisible(dq_dv_selected)
        self.interp_points_input.setEnabled(self.interp_chk.isChecked())
        available = self.format_cycle_ranges()
        self.cycle_availability.setText(f"Available cycles: {available}" if available else "No cycles loaded.")
        self.zero_note.setVisible(0 in self.available_cycles)
        cycles, notice = self._read_cycle_selection()
        self.preview_btn.setEnabled(cycles is not None)
        if self._result is not None:
            self.clear_plot()
        self.feedback.setToolTip("")
        if cycles is None:
            self.feedback.setText(notice)
            return
        message = f"{len(cycles):,} cycle(s) selected."
        self.feedback.setText(f"{message}\n{notice}" if notice else message)

    def clear_plot(self):
        self._result = None
        self.point_inspector.set_curves()
        self.figure.clear()
        self.toolbar.update()
        self.toolbar.setEnabled(False)
        self.extract_btn.setEnabled(False)
        axes = self.figure.add_subplot()
        axes.set_axis_off()
        axes.text(0.5, 0.5, "Select cycles and plot to preview the data.",
                  ha="center", va="center", color="#52636a", transform=axes.transAxes)
        self.canvas.draw_idle()

    def data_preview(self):
        cycles, notice = self._read_cycle_selection()
        if cycles is None:
            self.feedback.setText(notice)
            return
        self._repeat_plot = True
        analysis_type = self.analysis_type_combo.currentText()
        definition = PLOTS[analysis_type]
        self.point_inspector.set_curves()
        self.figure.clear()
        axes = self.figure.add_subplot()
        comparison = len(self.selected_battery_ids) > 1
        results, issues, counts, curves, legend = {}, {}, {}, [], []
        for index, (cell_id, sample) in enumerate(self.samples.items()):
            try:
                data = self._analyze(sample, cycles, analysis_type, definition)
                drawn, drawn_cycles = self._draw_sample(
                    axes, data, cell_id, analysis_type, definition, SAMPLE_COLORS[index], comparison
                )
            except Exception as error:
                logger.exception("Could not plot %s for %s", analysis_type, cell_id)
                issues[cell_id] = str(error)
                continue
            if not drawn:
                issues[cell_id] = "No usable data for the selected cycles."
                continue
            results[cell_id] = data
            counts[cell_id] = len(drawn_cycles)
            curves.extend(drawn)
            legend.append(Line2D([], [], color=SAMPLE_COLORS[index], linewidth=2, label=cell_id))

        if not results:
            self.clear_plot()
            details = "\n".join(f"{key}: {value}" for key, value in issues.items())
            self.feedback.setText(next(iter(issues.values()), "No usable data for the selected cycles."))
            self.feedback.setToolTip(details)
            return

        if comparison:
            if len(definition.series) > 1:
                legend.extend(Line2D([], [], color="0.35", linestyle="--" if name == "Discharge" else "-", label=name)
                              for _, name in definition.series)
            axes.legend(handles=legend, fontsize=8, ncols=2 if len(legend) > 5 else 1, loc="best")
            title = f"{analysis_type} · {len(results)} batteries"
        else:
            if 1 < len(curves) <= 12 or (definition.per_cycle and len(curves) == 1):
                axes.legend(fontsize=9)
            title = f"{analysis_type} · {next(iter(results))}"
        axes.set(xlabel=definition.x_label, ylabel=definition.y_label, title=title)
        axes.spines[["top", "right"]].set_visible(False)
        axes.grid(True, alpha=0.24)
        self.point_inspector.set_curves(axes, curves)
        self.toolbar.update()
        self.canvas.draw_idle()

        self._result = analysis_type, cycles, results
        self.extract_btn.setEnabled(True)
        self.toolbar.setEnabled(True)
        summaries = [f"{cell_id}: {count} of {len(cycles)} selected cycles plotted" for cell_id, count in counts.items()]
        if comparison:
            message = f"Plotted {len(results)} of {len(self.selected_battery_ids)} batteries."
        else:
            message = f"Plotted {next(iter(counts.values())):,} of {len(cycles):,} selected cycles."
        if issues:
            cell_id, reason = next(iter(issues.items()))
            message += f"\n{cell_id}: {reason}"
            summaries.extend(f"{key}: {value}" for key, value in issues.items())
        if notice:
            message += f"\n{notice}"
        self.feedback.setText(message)
        self.feedback.setToolTip("\n".join(summaries))

    def _analyze(self, sample, cycles, analysis_type, definition):
        interpolation_points = self.interp_points_input.value() if self.interp_chk.isChecked() else None
        return analyze_plot(sample, cycles, analysis_type, definition, interpolation_points=interpolation_points)

    @staticmethod
    def _draw_sample(axes, data, cell_id, analysis_type, definition, color, comparison):
        curves = []
        plotted_cycles = set()
        groups = data.groupby("cycle_number") if definition.per_cycle else [(None, data)]
        for group_index, (cycle, block) in enumerate(groups):
            for index, (column, name) in enumerate(definition.series):
                if column not in block:
                    continue
                series = block[block["type"] == name.lower()] if analysis_type == "dQ/dV" else block
                x, y = series[definition.x], series[column]
                valid = np.isfinite(x) & np.isfinite(y)
                if not valid.any():
                    continue
                plotted_cycles.update(series.loc[valid, "cycle_number"].unique())
                style = "-" if name != "Discharge" or len(definition.series) == 1 else "--"
                if definition.per_cycle:
                    label = "Cycle 0 (rest)" if cycle == 0 else f"Cycle {cycle}"
                    if len(definition.series) > 1:
                        label = f"{label} · {name}"
                    series_color = color if comparison else SAMPLE_COLORS[group_index % len(SAMPLE_COLORS)]
                    marker = "o" if valid.sum() == 1 else None
                else:
                    label = name
                    series_color = color if comparison else ("#146b69", "#6557a1")[index]
                    marker = "o"
                line, = axes.plot(x, y, color=series_color, linestyle=style, label=label, linewidth=1.4,
                                  marker=marker, markersize=3)
                curves.append((line, f"{cell_id} · {label}"))
        return curves, plotted_cycles

    def extract_data(self):
        if self._result is None:
            return
        analysis_type, cycles, results = self._result
        try:
            columns = PLOTS[analysis_type].export_columns
            if len(self.selected_battery_ids) > 1:
                columns = ("cycle_number", *(column for column in columns if column != "cycle_number"))
                if analysis_type == "dQ/dV":
                    columns = (*columns, "type")
                frames = []
                for cell_id, data in results.items():
                    frame = data.reindex(columns=columns).copy()
                    frame.insert(0, "Battery ID", cell_id)
                    frames.append(frame)
                copied = pd.concat(frames, ignore_index=True)
                text = copied.to_csv(sep="\t", index=False)
            else:
                cell_id, data = next(iter(results.items()))
                copied = data.reindex(columns=columns)
                cell_label = cell_id
                if analysis_type == "Voltage vs. Time":
                    copied.columns = ["Time (h)", "Voltage (V)"]
                elif analysis_type == "dQ/dV":
                    cell_label = f"{cell_id}_{'_'.join(map(str, cycles))}"
                header = "\t".join(copied.columns)
                rows = copied.to_csv(sep="\t", index=False, header=False)
                text = f"{header}\n\n{cell_label}\n{rows}"
            QApplication.clipboard().setText(text)
        except Exception as error:
            logger.exception("Could not copy %s plot data", analysis_type)
            self.feedback.setText(f"Could not copy plotted data. {error}")
        else:
            self.feedback.setText(f"Copied {len(copied):,} rows from {len(results)} battery data set(s).")
