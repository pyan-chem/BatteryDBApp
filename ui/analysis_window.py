from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
                             QComboBox, QLineEdit, QPushButton, QCheckBox,
                             QGroupBox, QFormLayout, QMessageBox, QApplication)
from PySide6.QtCore import Qt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import logging

# Get logger for this module
logger = logging.getLogger(__name__)

# Add parent directory to path
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from analysis.data_processor import DataProcessor

class AnalysisWindow(QDialog):
    def __init__(self, parent=None, processed_data=None, cell_id=None):
        super().__init__(parent)
        try:
            logger.info(f"Opening analysis window for cell: {cell_id}")
            self.setWindowTitle(f"Data Analysis - Cell {cell_id}")
            self.resize(650, 480)
            
            self.processed_data = processed_data
            self.cell_id = cell_id
            self.filtered_data = None
            
            self.init_ui()
            logger.debug(f"Analysis window initialized for cell: {cell_id}")
        except Exception as e:
            logger.error(f"Error initializing AnalysisWindow: {e}", exc_info=True)
            raise
    
    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)
        self.setLayout(layout)
        
        # Analysis Type Selection
        type_group = QGroupBox("Analysis Type")
        type_layout = QVBoxLayout()
        type_group.setLayout(type_layout)
        
        self.analysis_type_combo = QComboBox()
        self.analysis_type_combo.addItems([
            "Voltage vs. Time",
            "Cycling vs. Capacity",
            "Cycling vs. Accumulated Energy",
            "dQ/dV",
            "dV/dQ",
            "Voltage vs. Capacity cyclic",
            "Cycling vs. Overvoltage",
            "Cycling vs. Endpointslippage"
        ])
        type_layout.addWidget(QLabel("Select Analysis:"))
        type_layout.addWidget(self.analysis_type_combo)
        
        layout.addWidget(type_group)
        
        # Cycles Selection
        cycles_group = QGroupBox("Cycle Selection")
        cycles_layout = QVBoxLayout()
        cycles_group.setLayout(cycles_layout)
        
        # All cycles checkbox
        self.all_cycles_chk = QCheckBox("All")
        self.all_cycles_chk.setChecked(True)
        self.all_cycles_chk.stateChanged.connect(self.on_all_cycles_changed)
        
        # Cycles input
        cycles_input_layout = QHBoxLayout()
        cycles_input_layout.addWidget(QLabel("Cycles:"))
        self.cycles_input = QLineEdit()
        self.cycles_input.setPlaceholderText("e.g., 1,3,5 or 1-5")
        self.cycles_input.setEnabled(False)  # Disabled when "All" is checked
        self.cycles_input.textChanged.connect(self.on_cycles_input_changed)
        cycles_input_layout.addWidget(self.cycles_input)
        
        cycles_layout.addWidget(self.all_cycles_chk)
        cycles_layout.addLayout(cycles_input_layout)
        
        layout.addWidget(cycles_group)
        
        # Interpolation Options (for dQ/dV)
        self.interp_group = QGroupBox("Interpolation (dQ/dV only)")
        interp_layout = QHBoxLayout()
        self.interp_group.setLayout(interp_layout)
        
        self.interp_chk = QCheckBox("Enable Interpolation")
        self.interp_chk.setChecked(False)
        interp_layout.addWidget(self.interp_chk)
        
        interp_layout.addWidget(QLabel("Points:"))
        self.interp_points_input = QLineEdit("1000")
        self.interp_points_input.setMaximumWidth(80)
        interp_layout.addWidget(self.interp_points_input)
        interp_layout.addStretch()
        
        layout.addWidget(self.interp_group)
        
        # Connect analysis type change to show/hide interpolation
        self.analysis_type_combo.currentTextChanged.connect(self.on_analysis_type_changed)
        self.on_analysis_type_changed(self.analysis_type_combo.currentText())
        
        # Action Buttons
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(8)
        
        self.extract_btn = QPushButton("Extract Data")
        self.extract_btn.clicked.connect(self.extract_data)
        btn_layout.addWidget(self.extract_btn)
        
        self.preview_btn = QPushButton("Data Preview")
        self.preview_btn.clicked.connect(self.data_preview)
        btn_layout.addWidget(self.preview_btn)
        
        self.close_btn = QPushButton("Close")
        self.close_btn.clicked.connect(self.close)
        btn_layout.addWidget(self.close_btn)
        
        layout.addLayout(btn_layout)
    
    def on_all_cycles_changed(self, state):
        """Enable/disable cycles input based on 'All' checkbox"""
        is_checked = state == Qt.CheckState.Checked.value
        self.cycles_input.setEnabled(not is_checked)
        if is_checked:
            self.cycles_input.clear()
    
    def on_cycles_input_changed(self, text):
        """Uncheck 'All' when user types in cycles input"""
        if text.strip():
            self.all_cycles_chk.setChecked(False)
    
    def on_analysis_type_changed(self, analysis_type):
        """Show/hide interpolation options based on analysis type"""
        if analysis_type == "dQ/dV":
            # Always show interpolation group for dQ/dV
            self.interp_group.setVisible(True)
        else:
            self.interp_group.setVisible(False)
    
    def parse_cycles(self):
        """Parse cycles input and return list of cycle numbers"""
        if self.all_cycles_chk.isChecked():
            # Return all unique cycles
            return sorted(self.processed_data['cycle_number'].unique().tolist())
        
        cycles_text = self.cycles_input.text().strip()
        if not cycles_text:
            QMessageBox.warning(self, "Error", "Please specify cycles or check 'All'")
            return None
        
        cycles = []
        try:
            # Parse comma-separated values and ranges
            parts = cycles_text.split(',')
            for part in parts:
                part = part.strip()
                if '-' in part:
                    # Range like "1-5"
                    start, end = part.split('-')
                    cycles.extend(range(int(start), int(end) + 1))
                else:
                    # Single value like "3"
                    cycles.append(int(part))
            
            return sorted(list(set(cycles)))  # Remove duplicates and sort
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Invalid cycle format: {str(e)}")
            return None
    
    def filter_data_by_cycles(self, cycles):
        """Filter processed data by selected cycles"""
        if cycles is None:
            return None
        
        filtered = self.processed_data[self.processed_data['cycle_number'].isin(cycles)]
        
        if filtered.empty:
            QMessageBox.warning(self, "Error", "No data found for selected cycles")
            return None
        
        return filtered
    
    def extract_data(self):
        """Extract data based on analysis type and copy to clipboard"""
        try:
            analysis_type = self.analysis_type_combo.currentText()
            cycles = self.parse_cycles()
            
            if cycles is None:
                logger.warning(f"Extract data failed: invalid cycle selection")
                return
            
            logger.info(f"Extracting {analysis_type} data for cell {self.cell_id}, cycles {cycles}")
            
            # Process data based on analysis type
            try:
                if analysis_type == "Voltage vs. Time":
                    filtered_data = self.processed_data[self.processed_data['cycle_number'].isin(cycles)]
                    extract_df = filtered_data[['time_h', 'voltage_V']].copy()
                    extract_df.columns = ['Time (h)', 'Voltage (V)']
                
                elif analysis_type == "Cycling vs. Capacity":
                    analysis_data = DataProcessor.cycling_vs_capacity(self.processed_data)
                    extract_df = analysis_data[analysis_data['cycle_number'].isin(cycles)][['cycle_number', 'charge_cap_mAh_g', 'discharge_cap_mAh_g']]
                
                elif analysis_type == "Cycling vs. Accumulated Energy":
                    analysis_data = DataProcessor.cycling_vs_accumulated_energy(self.processed_data)
                    extract_df = analysis_data[analysis_data['cycle_number'].isin(cycles)][['cycle_number', 'Accu_Energy_Wh']]
                
                elif analysis_type == "Cycling vs. Overvoltage":
                    analysis_data = DataProcessor.cycling_vs_overvoltage(self.processed_data)
                    extract_df = analysis_data[analysis_data['cycle_number'].isin(cycles)][['cycle_number', 'Overvoltage']]
                
                elif analysis_type == "Voltage vs. Capacity cyclic":
                    analysis_data = DataProcessor.voltage_vs_capacity_cyclic(self.processed_data)
                    filtered_data = analysis_data[analysis_data['cycle_number'].isin(cycles)]
                    extract_df = filtered_data[['cycle_number', 'voltage_V', '(Q-Qo)_mAh_g']]
                
                elif analysis_type == "Cycling vs. Endpointslippage":
                    analysis_data = DataProcessor.cycling_vs_endpointslippage(self.processed_data)
                    extract_df = analysis_data[analysis_data['cycle_number'].isin(cycles)][['cycle_number', 'discharge_endpoint', 'charge_endpoint']]
                
                elif analysis_type == "dQ/dV":
                    # Check interpolation settings
                    use_interp = self.interp_chk.isChecked()
                    interp_points = int(self.interp_points_input.text()) if use_interp else None
                    analysis_data = DataProcessor.dqdv(self.processed_data, interpolate_flag=use_interp, interp_points=interp_points)
                    # For dQ/dV, prepare data without cycle_number and type columns
                    filtered = analysis_data[analysis_data['cycle_number'].isin(cycles)]
                    # Group by cycle for special formatting
                    extract_df = filtered[['voltage_V', 'dQdV_charge', 'dQdV_discharge']]
                
                elif analysis_type == "dV/dQ":
                    analysis_data = DataProcessor.dvdq(self.processed_data)
                    extract_df = analysis_data[analysis_data['cycle_number'].isin(cycles)][['cycle_number', 'discharge_cap_mAh_g', 'dVdQ']]
                
                else:
                    logger.warning(f"Analysis type not implemented: {analysis_type}")
                    QMessageBox.information(self, "Info", f"{analysis_type} not implemented yet")
                    return
                
                # Prepare data with header rows
                import io
                output = io.StringIO()
                
                # Write column headers
                output.write('\t'.join(extract_df.columns) + '\n')
                
                # Add empty row and ID row
                output.write('\n')  # Empty row
                if analysis_type == "dQ/dV":
                    # For dQ/dV, add cycle info after ID
                    cycle_info = '_'.join([str(c) for c in cycles])
                    output.write(f'{self.cell_id}_{cycle_info}\n')
                else:
                    output.write(f'{self.cell_id}\n')
                
                # Write data
                extract_df.to_csv(output, sep='\t', index=False, header=False)
                
                # Copy to clipboard
                QApplication.clipboard().setText(output.getvalue())
                logger.info(f"Data extracted and copied to clipboard: {len(extract_df)} rows")
                QMessageBox.information(self, "Success", "Data copied to clipboard.")
            except Exception as e:
                logger.error(f"Error extracting {analysis_type} data: {e}", exc_info=True)
                QMessageBox.critical(self, "Error", f"Failed to extract data: {str(e)}")
        except Exception as e:
            logger.error(f"Error in extract_data: {e}", exc_info=True)
            QMessageBox.critical(self, "Error", f"Failed to extract data: {str(e)}")
    
    def data_preview(self):
        """Preview data with plot in a popup window"""
        try:
            analysis_type = self.analysis_type_combo.currentText()
            cycles = self.parse_cycles()
            
            if cycles is None:
                logger.warning(f"Preview failed: invalid cycle selection")
                return
            
            logger.info(f"Previewing {analysis_type} data for cell {self.cell_id}, cycles {cycles}")
            
            if analysis_type == "Voltage vs. Time":
                filtered_data = self.processed_data[self.processed_data['cycle_number'].isin(cycles)]
                self.show_plot(filtered_data, cycles, analysis_type)
            
            elif analysis_type == "Cycling vs. Capacity":
                analysis_data = DataProcessor.cycling_vs_capacity(self.processed_data)
                filtered = analysis_data[analysis_data['cycle_number'].isin(cycles)]
                self.show_plot(filtered, cycles, analysis_type)
            
            elif analysis_type == "Cycling vs. Accumulated Energy":
                analysis_data = DataProcessor.cycling_vs_accumulated_energy(self.processed_data)
                filtered = analysis_data[analysis_data['cycle_number'].isin(cycles)]
                self.show_plot(filtered, cycles, analysis_type)
            
            elif analysis_type == "Cycling vs. Overvoltage":
                analysis_data = DataProcessor.cycling_vs_overvoltage(self.processed_data)
                filtered = analysis_data[analysis_data['cycle_number'].isin(cycles)]
                self.show_plot(filtered, cycles, analysis_type)
            
            elif analysis_type == "Voltage vs. Capacity cyclic":
                analysis_data = DataProcessor.voltage_vs_capacity_cyclic(self.processed_data)
                filtered = analysis_data[analysis_data['cycle_number'].isin(cycles)]
                self.show_plot(filtered, cycles, analysis_type)
            
            elif analysis_type == "Cycling vs. Endpointslippage":
                analysis_data = DataProcessor.cycling_vs_endpointslippage(self.processed_data)
                filtered = analysis_data[analysis_data['cycle_number'].isin(cycles)]
                self.show_plot(filtered, cycles, analysis_type)
            
            elif analysis_type == "dQ/dV":
                # Check interpolation settings
                use_interp = self.interp_chk.isChecked()
                interp_points = int(self.interp_points_input.text()) if use_interp else None
                logger.debug(f"dQ/dV with interpolation={use_interp}, points={interp_points}")
                analysis_data = DataProcessor.dqdv(self.processed_data, interpolate_flag=use_interp, interp_points=interp_points)
                filtered = analysis_data[analysis_data['cycle_number'].isin(cycles)]
                self.show_plot(filtered, cycles, analysis_type)
            
            elif analysis_type == "dV/dQ":
                analysis_data = DataProcessor.dvdq(self.processed_data)
                filtered = analysis_data[analysis_data['cycle_number'].isin(cycles)]
                self.show_plot(filtered, cycles, analysis_type)
            
            else:
                logger.warning(f"Analysis type not implemented: {analysis_type}")
                QMessageBox.information(self, "Info", f"{analysis_type} not implemented yet")
        except Exception as e:
            logger.error(f"Error in data preview: {e}", exc_info=True)
            QMessageBox.critical(self, "Error", f"Failed to preview data: {str(e)}")
    
    def show_plot(self, data, cycles, analysis_type):
        """Show plot based on analysis type"""
        try:
            logger.debug(f"Creating plot for {analysis_type}")
            preview_window = PlotPreviewWindow(self, self.cell_id, analysis_type)
            
            fig = preview_window.figure
            ax = fig.add_subplot(111)
            
            try:
                if analysis_type == "Voltage vs. Time":
                    for cycle in cycles:
                        cycle_data = data[data['cycle_number'] == cycle]
                        ax.plot(cycle_data['time_h'], cycle_data['voltage_V'], 
                               label=f'Cycle {cycle}', linewidth=1.5)
                    ax.set_xlabel('Time (h)', fontsize=12)
                    ax.set_ylabel('Voltage (V)', fontsize=12)
                
                elif analysis_type == "Cycling vs. Capacity":
                    ax.plot(data['cycle_number'], data['charge_cap_mAh_g'], 'o-', label='Charge', linewidth=2)
                    ax.plot(data['cycle_number'], data['discharge_cap_mAh_g'], 's-', label='Discharge', linewidth=2)
                    ax.set_xlabel('Cycle Number', fontsize=12)
                    ax.set_ylabel('Capacity (mAh/g)', fontsize=12)
                    ax.legend()
                
                elif analysis_type == "Cycling vs. Accumulated Energy":
                    ax.plot(data['cycle_number'], data['Accu_Energy_Wh'], 'o-', linewidth=2, color='green')
                    ax.set_xlabel('Cycle Number', fontsize=12)
                    ax.set_ylabel('Accumulated Energy (Wh)', fontsize=12)
                
                elif analysis_type == "Cycling vs. Overvoltage":
                    ax.plot(data['cycle_number'], data['Overvoltage'], 'o-', linewidth=2, color='red')
                    ax.set_xlabel('Cycle Number', fontsize=12)
                    ax.set_ylabel('Overvoltage (V)', fontsize=12)
                
                elif analysis_type == "Voltage vs. Capacity cyclic":
                    for cycle in cycles:
                        cycle_data = data[data['cycle_number'] == cycle]
                        ax.plot(cycle_data['(Q-Qo)_mAh_g'], cycle_data['voltage_V'], 
                               label=f'Cycle {cycle}', linewidth=1.5)
                    ax.set_xlabel('Capacity (Q-Qo) (mAh/g)', fontsize=12)
                    ax.set_ylabel('Voltage (V)', fontsize=12)
                    ax.legend()
                
                elif analysis_type == "Cycling vs. Endpointslippage":
                    ax.plot(data['cycle_number'], data['discharge_endpoint'], 'o-', label='Discharge Endpoint', linewidth=2)
                    ax.plot(data['cycle_number'], data['charge_endpoint'], 's-', label='Charge Endpoint', linewidth=2)
                    ax.set_xlabel('Cycle Number', fontsize=12)
                    ax.set_ylabel('Endpoint Slippage', fontsize=12)
                    ax.legend()
                
                elif analysis_type == "dQ/dV":
                    for cycle in cycles:
                        charge_data = data[(data['cycle_number'] == cycle) & (data['type'] == 'charge')]
                        discharge_data = data[(data['cycle_number'] == cycle) & (data['type'] == 'discharge')]
                        if len(charge_data) > 0:
                            ax.plot(charge_data['voltage_V'], charge_data['dQdV_charge'], 'o-', 
                                   label=f'Cycle {cycle} Charge', linewidth=1.5, alpha=0.7)
                        if len(discharge_data) > 0:
                            ax.plot(discharge_data['voltage_V'], discharge_data['dQdV_discharge'], 's-', 
                                   label=f'Cycle {cycle} Discharge', linewidth=1.5, alpha=0.7)
                    ax.set_xlabel('Voltage (V)', fontsize=12)
                    ax.set_ylabel('dQ/dV', fontsize=12)
                    ax.legend(fontsize=8)
                
                elif analysis_type == "dV/dQ":
                    for cycle in cycles:
                        cycle_data = data[data['cycle_number'] == cycle]
                        ax.plot(cycle_data['discharge_cap_mAh_g'], cycle_data['dVdQ'], 
                               label=f'Cycle {cycle}', linewidth=1.5)
                    ax.set_xlabel('Discharge Capacity (mAh/g)', fontsize=12)
                    ax.set_ylabel('dV/dQ', fontsize=12)
                    ax.legend()
                
                ax.set_title(f'{analysis_type} - Cell {self.cell_id}', fontsize=14)
                ax.grid(True, alpha=0.3)
                logger.debug(f"Plot created successfully for {analysis_type}")
                
            except Exception as e:
                logger.error(f"Error plotting {analysis_type}: {e}", exc_info=True)
                ax.text(0.5, 0.5, f'Error plotting:\n{str(e)}', 
                       ha='center', va='center', transform=ax.transAxes)
            
            preview_window.canvas.draw()
            preview_window.exec()
        except Exception as e:
            logger.error(f"Error in show_plot: {e}", exc_info=True)


class PlotPreviewWindow(QDialog):
    """Popup window to display matplotlib plots"""
    def __init__(self, parent=None, cell_id=None, title="Plot Preview"):
        super().__init__(parent)
        self.setWindowTitle(f"{title} - Cell {cell_id}")
        self.resize(820, 600)
        
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        # Create matplotlib figure
        self.figure = Figure(figsize=(8, 6))
        self.canvas = FigureCanvas(self.figure)
        layout.addWidget(self.canvas)
        
        # Close button
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.close)
        layout.addWidget(close_btn)
