from bisect import bisect_right
from html import escape

import numpy as np
from PySide6.QtCore import QPoint
from PySide6.QtWidgets import QToolTip
from scipy.spatial import cKDTree


class PointInspector:
    """Find the nearest visible data point in screen pixels."""

    def __init__(self, canvas):
        self.canvas = canvas
        self.axes = None
        self.curves = []
        self._viewport = None
        self._tree = None
        self._curve_ends = []
        self._point_rows = []
        self._hovered_point = None
        canvas.mpl_connect("motion_notify_event", self.inspect)
        canvas.mpl_connect("axes_leave_event", self.hide)
        canvas.mpl_connect("figure_leave_event", self.hide)
        canvas.mpl_connect("button_press_event", self.hide)

    def set_curves(self, axes=None, curves=()):
        self.hide()
        self.axes = axes
        self.curves = curves
        self._viewport = None
        self._tree = None
        self._curve_ends = []
        self._point_rows = []

    def hide(self, event=None):
        if self._hovered_point is not None:
            QToolTip.hideText()
            self._hovered_point = None

    def _index_points(self, viewport):
        self._viewport = viewport
        self._curve_ends = []
        self._point_rows = []
        points = []
        size = 0
        x_min, x_max = sorted(self.axes.get_xlim())
        y_min, y_max = sorted(self.axes.get_ylim())
        for line, label in self.curves:
            values = line.get_xydata()
            x, y = values.T
            rows = np.flatnonzero((x_min <= x) & (x <= x_max) & (y_min <= y) & (y <= y_max))
            if not len(rows):
                continue
            pixels = self.axes.transData.transform(values[rows])
            points.append(pixels)
            size += len(rows)
            self._curve_ends.append(size)
            self._point_rows.append((line, label, rows))
        self._tree = cKDTree(np.concatenate(points)) if points else None

    def inspect(self, event):
        axes = self.axes
        if axes is None or event.inaxes is not axes or event.buttons:
            self.hide()
            return
        viewport = (axes.get_xlim(), axes.get_ylim(), axes.get_xscale(), axes.get_yscale(), tuple(axes.bbox.bounds))
        if viewport != self._viewport:
            self.hide()
            self._index_points(viewport)
        if self._tree is None:
            return
        ratio = self.canvas.device_pixel_ratio
        distance, index = self._tree.query((event.x, event.y), distance_upper_bound=8 * ratio)
        if not np.isfinite(distance):
            self.hide()
            return
        index = int(index)
        if index == self._hovered_point and QToolTip.isVisible():
            return
        curve = bisect_right(self._curve_ends, index)
        start = self._curve_ends[curve - 1] if curve else 0
        line, label, rows = self._point_rows[curve]
        x, y = line.get_xydata()[rows[index - start]]
        details = [
            label,
            f"{axes.get_xlabel()}: {x:.10g}",
            f"{axes.get_ylabel()}: {y:.10g}",
        ]
        text = "<qt>" + "<br>".join(escape(part) for part in details) + "</qt>"
        cursor = QPoint(round(event.x / ratio), self.canvas.height() - round(event.y / ratio))
        QToolTip.showText(self.canvas.mapToGlobal(cursor), text, self.canvas)
        self._hovered_point = index
