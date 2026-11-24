import csv
from itertools import islice
import pandas as pd
import os
import sqlite3
import logging
from contextlib import closing
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory

# Get logger for this module
logger = logging.getLogger(__name__)

class DataImporter:
    MACCOR_ANALYSIS_COLUMNS = {
        "Echem_Rec#", "Echem_Test_Time_h", "Echem_Cycle", "Echem_Capacity_Ah",
        "Echem_Energy_Wh", "Echem_Current_A", "Echem_Voltage_V", "Echem_Mode",
    }

    @staticmethod
    def import_data(file_path, instrument_type, *, for_analysis=False):
        """
        Imports data from Biologic (.mpr), Arbin (.res), or Maccor (.txt, .xyz, .001-.999) files.
        
        Args:
            file_path: Path to the data file
            instrument_type: "Arbin", "Biologic", or "Maccor"
            for_analysis: Allow a verified column subset for Maccor analysis
        
        Returns:
            pandas DataFrame with the data
        """
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        ext = os.path.splitext(file_path)[1].lower()
        logger.debug(f"Importing {instrument_type} file: {file_path} (extension: {ext})")
        
        if instrument_type == "Arbin":
            if ext != ".res":
                logger.error(f"Invalid Arbin file format: {file_path} (expected .res, got {ext})")
                raise ValueError(f"Arbin files must be .res format, got {ext}")
            return DataImporter._read_arbin(file_path)
        elif instrument_type == "Biologic":
            if ext != ".mpr":
                logger.error(f"Invalid Biologic file format: {file_path} (expected .mpr, got {ext})")
                raise ValueError(f"Biologic files must be .mpr format, got {ext}")
            return DataImporter._read_biologic(file_path)
        elif instrument_type == "Maccor":
            # Support .txt, .xyz, and numeric extensions (.001, .002, etc.)
            if ext not in [".txt", ".xyz"] and not (ext.startswith(".") and ext[1:].isdigit()):
                logger.error(f"Invalid Maccor file format: {file_path} (expected .txt/.xyz/numeric, got {ext})")
                raise ValueError(f"Maccor files must be .txt, .xyz, or numeric format (.001, .002, etc.), got {ext}")
            return DataImporter._read_maccor(file_path, for_analysis=for_analysis)
        else:
            logger.error(f"Unknown instrument type: {instrument_type}")
            raise ValueError(f"Unknown instrument type: {instrument_type}")

    @staticmethod
    def _read_biologic(file_path):
        """Read Biologic .mpr files using yadg first, then fallback to galvani."""
        required_columns = [
            'time/s',
            'Ewe/V',
            'I/mA',
            'half cycle',
            '(Q-Qo)/mA.h',
            '|Energy|/W.h',
            'Q charge/discharge/mA.h',
        ]

        logger.info(f"Reading Biologic file with yadg: {file_path}")
        try:
            yadg_df = DataImporter._read_biologic_yadg(file_path)
            missing = [col for col in required_columns if col not in yadg_df.columns]
            if not missing:
                logger.info(f"yadg Biologic import succeeded: {len(yadg_df)} rows")
                return yadg_df
            logger.warning(
                f"yadg failed, falling back to galvani: missing required columns {missing}"
            )
        except Exception as yadg_error:
            logger.warning(f"yadg failed, falling back to galvani: {yadg_error}")

        try:
            from galvani import BioLogic
            
            logger.info(f"Reading Biologic file with galvani: {file_path}")
            mpr_file = BioLogic.MPRfile(file_path)
            
            # Convert the data to a DataFrame
            df = pd.DataFrame(mpr_file.data)
            
            logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
            return df
            
        except ImportError:
            logger.error("galvani library not installed")
            raise ImportError("galvani library not installed. Please install it with: pip install galvani")
        except FileNotFoundError as e:
            logger.error(f"Biologic file not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading Biologic file '{file_path}': {str(e)}")
            raise Exception(f"Error reading Biologic file: {str(e)}")

    @staticmethod
    def _read_biologic_yadg(file_path):
        """Read Biologic .mpr files using yadg and map fields to legacy DataProcessor columns."""
        from yadg.extractors import extract

        tree = extract("eclab.mpr", file_path)
        dataset = tree.to_dataset()

        # Map yadg variables to the existing Biologic column names consumed by DataProcessor.
        mapped = pd.DataFrame()
        variable_mapping = {
            'time/s': 'time',
            'Ewe/V': 'Ewe',
            'I/mA': 'I',
            'half cycle': 'half cycle',
            '(Q-Qo)/mA.h': '(Q-Qo)',
            '|Energy|/W.h': 'Energy we',
        }

        for target_col, source_var in variable_mapping.items():
            if source_var in dataset:
                mapped[target_col] = pd.Series(dataset[source_var].values)

        # yadg reports charge/discharge capacity in coulomb (C); convert to mA.h.
        if 'Q charge or discharge' in dataset:
            mapped['Q charge/discharge/mA.h'] = pd.Series(dataset['Q charge or discharge'].values) / 3.6

        return mapped

    @staticmethod
    def _read_arbin(file_path):
        """Read Arbin .res files using galvani library or pyodbc."""
        try:
            # First, try using galvani with mdbtools (if available)
            try:
                if shutil.which("mdb-export") is None:
                    raise RuntimeError("mdbtools is not available (missing mdb-export)")
                logger.info(f"Reading Arbin file: {file_path}")

                with TemporaryDirectory(prefix="batterydb-arbin-") as temp_dir:
                    temp_db_path = str(Path(temp_dir) / "arbin.sqlite")
                    # Galvani writes progress to stdout, which MCP reserves for protocol messages.
                    result = subprocess.run(
                        [sys.executable, "-X", "utf8", "-m", "galvani.res2sqlite", str(file_path), temp_db_path],
                        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
                        encoding="utf-8", errors="replace",
                    )
                    if result.returncode:
                        raise RuntimeError(result.stderr.strip() or f"Galvani exited with status {result.returncode}")
                    with closing(sqlite3.connect(temp_db_path)) as db:
                        df = pd.read_sql(sql="SELECT * FROM Channel_Normal_Table", con=db)
                    logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
                    return df

            except Exception as galvani_error:
                # If galvani fails, try pyodbc fallback
                logger.warning(f"Galvani import failed for '{file_path}': {str(galvani_error)}, attempting pyodbc fallback...")
                return DataImporter._read_arbin_pyodbc(file_path)
                
        except Exception as e:
            logger.error(f"Error reading Arbin file '{file_path}': {str(e)}")
            raise Exception(f"Error reading Arbin file: {str(e)}")

    @staticmethod
    def _read_arbin_pyodbc(file_path):
        """Fallback method to read Arbin .res MDB files using pyodbc."""
        try:
            import pyodbc
            
            logger.info(f"Reading Arbin file via pyodbc: {file_path}")
            
            if ";" in str(file_path):
                raise ValueError("Arbin paths containing semicolons require the mdbtools reader.")
            connection_string = f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={file_path};"

            try:
                with closing(pyodbc.connect(connection_string, readonly=True)) as conn:
                    cursor = conn.cursor()
                    logger.debug("Fetching data from Channel_Normal_Table")
                    cursor.execute("SELECT * FROM Channel_Normal_Table")
                    columns = [description[0] for description in cursor.description]
                    rows = [tuple(row) for row in cursor.fetchall()]
                df = pd.DataFrame(rows, columns=columns)
                logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
                return df
                
            except Exception as pyodbc_error:
                logger.error(f"pyodbc failed to read '{file_path}': {str(pyodbc_error)}", exc_info=True)
                raise Exception(f"pyodbc failed: {str(pyodbc_error)}")
                
        except ImportError:
            logger.error("pyodbc library not installed")
            raise ImportError("pyodbc library not installed. Please install it with: pip install pyodbc")

    @staticmethod
    def _read_maccor(file_path, encoding='utf-8', *, for_analysis=False):
        """
        Read Maccor tab-separated files (.txt, .xyz, or numeric extensions like .001, .002).
        
        Args:
            file_path: Path to the Maccor file
            encoding: File encoding (default: utf-8)
        
        Returns:
            pandas DataFrame with Maccor data
        """
        try:
            logger.info(f"Reading Maccor file: {file_path}")
            
            # Create column mapping for various Maccor format compatibility
            column_mapping = {
                # Time columns (convert seconds to hours)
                'Echem_Test Time (sec)': 'Echem_Test_Time_h',  
                'Echem_Test (Sec)': 'Echem_Test_Time_h',       
                'Echem_Step Time (sec)': 'Echem_Step_Time_h',  
                'Echem_Step (Sec)': 'Echem_Step_Time_h',
                
                # Capacity columns
                'Echem_Capacity (AHr)': 'Echem_Capacity_Ah',
                'Echem_Amp-hr': 'Echem_Capacity_Ah',
                
                # Current columns
                'Echem_Current (A)': 'Echem_Current_A',
                'Echem_Amps': 'Echem_Current_A',
                
                # Voltage columns
                'Echem_Voltage (V)': 'Echem_Voltage_V',
                'Echem_Volts': 'Echem_Voltage_V',
                
                # Energy columns
                'Echem_Energy (WHr)': 'Echem_Energy_Wh',
                'Echem_Watt-hr': 'Echem_Energy_Wh',
                
                # Cycle columns
                'Echem_Cycle P': 'Echem_Cycle',
                'Echem_Cyc#': 'Echem_Cycle',
                
                # Mode columns
                'Echem_MD': 'Echem_Mode',
                'Echem_State': 'Echem_Mode',
                
                # Status columns
                'Echem_ES': 'Echem_Status'
            }
            
            # Some Maccor exports start with the header, while others have a preamble.
            with open(file_path, encoding=encoding, newline="") as stream:
                for header_row, line in enumerate(islice(stream, 32)):
                    fields = next(csv.reader([line], delimiter='\t'), [])
                    known_columns = {
                        column_mapping.get('Echem_' + field.lstrip('\ufeff').strip())
                        for field in fields
                    }
                    if len(known_columns - {None, 'Echem_Step_Time_h', 'Echem_Status'}) >= 2:
                        break
                else:
                    raise ValueError("Could not find a Maccor column header in the first 32 lines.")

            options = {"sep": '\t', "skiprows": header_row, "encoding": encoding}
            df = None
            if for_analysis:
                df = DataImporter._read_maccor_analysis(file_path, options, fields, column_mapping)
            try:
                if df is None:
                    df = pd.read_csv(file_path, **options)
            except Exception as error:
                logger.warning("C engine failed (%s), falling back to Python engine", type(error).__name__)
                df = pd.read_csv(file_path, engine='python', **options)
            df.columns = ['Echem_' + column.strip() for column in df.columns]

            # Apply column mapping
            df = df.rename(columns=lambda x: column_mapping.get(x, x))
            logger.debug(f"Applied column mapping, mapped {len([x for x in column_mapping.values() if x in df.columns])} columns")
            
            # Convert time columns from seconds to hours
            time_columns = ['Echem_Test_Time_h', 'Echem_Step_Time_h']
            for col in time_columns:
                if col in df.columns:
                    logger.debug(f"Converting {col} from seconds to hours")
                    df[col] = pd.to_numeric(df[col], errors='coerce') / 3600.0
            
            # Convert numeric columns
            numeric_columns = {
                'Echem_Capacity_Ah': float,
                'Echem_Current_A': float,
                'Echem_Voltage_V': float,
                'Echem_Energy_Wh': float,
            }
            
            for col, dtype in numeric_columns.items():
                if col in df.columns:
                    logger.debug(f"Converting {col} to numeric type")
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Handle Cycle column
            if 'Echem_Cycle' in df.columns:
                logger.debug("Processing Echem_Cycle column (strip thousands separators)")
                # Some Maccor exports format large cycle numbers with thousands separators
                # e.g. '1,000'. Remove commas before numeric conversion to preserve values.
                df['Echem_Cycle'] = pd.to_numeric(
                    df['Echem_Cycle'].astype(str).str.replace(',', '', regex=False),
                    errors='coerce'
                ).fillna(0).astype(int)
            
            # Handle Mode column
            if 'Echem_Mode' in df.columns:
                logger.debug("Processing Echem_Mode column")
                df['Echem_Mode'] = df['Echem_Mode'].astype(str).str.strip().str.upper()
            
            logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
            return df
            
        except FileNotFoundError:
            logger.error(f"Maccor file not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading Maccor file '{file_path}': {str(e)}", exc_info=True)
            raise Exception(f"Error reading Maccor file: {str(e)}")

    @staticmethod
    def _read_maccor_analysis(file_path, options, fields, column_mapping):
        """Project columns only when record IDs preserve the full-row cleanup semantics."""
        names = ['Echem_' + field.lstrip('\ufeff').strip() for field in fields]
        mapped = [column_mapping.get(name, name) for name in names]
        required = DataImporter.MACCOR_ANALYSIS_COLUMNS
        positions = [index for index, name in enumerate(mapped) if name in required]
        if len(positions) != len(required) or {mapped[index] for index in positions} != required:
            return None

        # pandas can ignore ragged rows with usecols. Keep the original parser for
        # ragged or quoted records, including multiline fields.
        with open(file_path, encoding=options["encoding"], newline="") as stream:
            before = os.fstat(stream.fileno())
            records = islice(stream, options["skiprows"] + 1, None)
            tabs = len(fields) - 1
            if any((line.count('\t') != tabs or '"' in line or '\x00' in line)
                   and line.strip() for line in records):
                return None

        try:
            frame = pd.read_csv(file_path, usecols=positions, **options)
        except pd.errors.ParserError:
            return None
        after = os.stat(file_path)
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            return None
        selected_names = ['Echem_' + name.strip() for name in frame.columns]
        selected_mapped = [column_mapping.get(name, name) for name in selected_names]
        if set(selected_mapped) != required:
            return None
        record_ids = frame.iloc[:, selected_mapped.index("Echem_Rec#")]
        if record_ids.hasnans or not record_ids.is_unique:
            return None
        return frame

