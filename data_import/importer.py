import pandas as pd
import os
import sqlite3
import logging
from pathlib import Path

# Get logger for this module
logger = logging.getLogger(__name__)

class DataImporter:
    @staticmethod
    def import_data(file_path, instrument_type):
        """
        Imports data from Biologic (.mpr), Arbin (.res), or Maccor (.txt, .xyz, .001-.999) files.
        
        Args:
            file_path: Path to the data file
            instrument_type: "Arbin", "Biologic", or "Maccor"
        
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
            return DataImporter._read_maccor(file_path)
        else:
            logger.error(f"Unknown instrument type: {instrument_type}")
            raise ValueError(f"Unknown instrument type: {instrument_type}")

    @staticmethod
    def _read_biologic(file_path):
        """Read Biologic .mpr files using galvani library."""
        try:
            from galvani import BioLogic
            
            logger.info(f"Reading Biologic file: {file_path}")
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
    def _read_arbin(file_path):
        """Read Arbin .res files using galvani library or pyodbc."""
        try:
            # First, try using galvani with mdbtools (if available)
            try:
                from galvani.res2sqlite import convert_arbin_to_sqlite
                
                logger.info(f"Reading Arbin file: {file_path}")
                
                # Create a temporary SQLite database
                temp_db_path = Path(file_path).stem + "_temp.sqlite"
                
                try:
                    # Convert Arbin .res file to SQLite
                    convert_arbin_to_sqlite(file_path, temp_db_path)
                    
                    # Read from the SQLite database
                    with sqlite3.connect(temp_db_path) as db:
                        df = pd.read_sql(sql="SELECT * FROM Channel_Normal_Table", con=db)
                    
                    logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
                    return df
                    
                finally:
                    # Always clean up temporary database (even on error)
                    if os.path.exists(temp_db_path):
                        try:
                            os.remove(temp_db_path)
                            logger.debug(f"Cleaned up temp file: {temp_db_path}")
                        except OSError as cleanup_error:
                            logger.warning(f"Failed to clean up temp file '{temp_db_path}': {cleanup_error}")
                    
            except ImportError:
                # galvani not available, use pyodbc fallback
                logger.info("galvani not available, attempting pyodbc fallback...")
                return DataImporter._read_arbin_pyodbc(file_path)
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
            
            # Create ODBC connection string for MDB file
            connection_string = f'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={file_path};'
            
            try:
                # Connect to the MDB file
                logger.debug(f"Establishing pyodbc connection to: {file_path}")
                conn = pyodbc.connect(connection_string)
                cursor = conn.cursor()
                
                # Read the data table using cursor to avoid SQLAlchemy warning
                logger.debug("Fetching data from Channel_Normal_Table")
                cursor.execute("SELECT * FROM Channel_Normal_Table")
                columns = [description[0] for description in cursor.description]
                # Convert pyodbc Row objects to tuples for proper DataFrame construction
                rows = [tuple(row) for row in cursor.fetchall()]
                df = pd.DataFrame(rows, columns=columns)
                
                conn.close()
                logger.debug(f"Connection closed successfully")
                
                logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
                return df
                
            except Exception as pyodbc_error:
                logger.error(f"pyodbc failed to read '{file_path}': {str(pyodbc_error)}", exc_info=True)
                raise Exception(f"pyodbc failed: {str(pyodbc_error)}")
                
        except ImportError:
            logger.error("pyodbc library not installed")
            raise ImportError("pyodbc library not installed. Please install it with: pip install pyodbc")

    @staticmethod
    def _read_maccor(file_path, encoding='utf-8'):
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
            
            # Read the tab-separated file, skip first row
            logger.debug("Parsing Maccor tab-separated file")
            try:
                # Try default C engine first (faster)
                df = pd.read_csv(file_path, sep='\t', skiprows=1, encoding=encoding)
                logger.debug("Parsed with C engine")
            except Exception as e:
                # Fallback to python engine if C engine fails
                logger.warning(f"C engine failed ({type(e).__name__}), falling back to python engine")
                df = pd.read_csv(file_path, sep='\t', skiprows=1, engine='python', encoding=encoding)
            logger.debug(f"Initial data shape: {df.shape}")
            
            # Add Echem_ prefix to all column names
            df.columns = ['Echem_' + c.strip() for c in df.columns]
            logger.debug(f"Prefixed column names with 'Echem_': {len(df.columns)} columns")
            
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
                logger.debug("Processing Echem_Cycle column")
                df['Echem_Cycle'] = pd.to_numeric(df['Echem_Cycle'], errors='coerce').fillna(0).astype(int)
            
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

