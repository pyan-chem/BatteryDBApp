import sqlite3
import pandas as pd
import logging

# Get logger for this module
logger = logging.getLogger(__name__)

class BatteryDatabase:
    """SQLite database for battery cell data."""
    
    # Whitelist of allowed columns for safety
    ALLOWED_COLUMNS = {
        'id', 'instrument', 'date', 'negative_electrode', 'positive_electrode',
        'n_active_mass', 'p_active_mass', 'electrolyte', 'format',
        'voltage_range', 'comment', 'status', 'file_path'
    }
    
    def __init__(self, db_name="battery_data.db"):
        self.db_name = db_name
        try:
            self.conn = sqlite3.connect(db_name)
            logger.info(f"Connected to database: {db_name}")
            self.create_table()
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to connect to database '{db_name}': {e}")
            raise
    
    def __enter__(self):
        """Context manager entry: return self for use in 'with' statement."""
        logger.debug(f"Entering context manager for database: {self.db_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: safely close database connection.
        
        Args:
            exc_type: Exception type if an error occurred
            exc_val: Exception value if an error occurred
            exc_tb: Exception traceback if an error occurred
        
        Returns:
            False: Do not suppress exceptions
        """
        try:
            self.close()
            logger.debug(f"Exiting context manager for database: {self.db_name}")
        except Exception as e:
            logger.warning(f"Error closing database in context manager: {e}")
        return False
    
    @staticmethod
    def _validate_columns(data):
        """Validate that all keys in data are allowed columns.
        
        Args:
            data: Dictionary with column names as keys
        
        Raises:
            ValueError: If any invalid columns are found
        """
        invalid = set(data.keys()) - BatteryDatabase.ALLOWED_COLUMNS
        if invalid:
            raise ValueError(f"Invalid column names: {invalid}")

    def create_table(self):
        """Create cells table if it does not exist."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cells (
                    id TEXT PRIMARY KEY,
                    instrument TEXT,
                    date TEXT,
                    negative_electrode TEXT,
                    positive_electrode TEXT,
                    n_active_mass REAL,
                    p_active_mass REAL,
                    electrolyte TEXT,
                    format TEXT,
                    voltage_range TEXT,
                    comment TEXT,
                    status TEXT,
                    file_path TEXT
                )
            """)
            self.conn.commit()
            logger.debug("Database table 'cells' ready")
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def add_cell(self, data):
        """Add or replace a cell in the database.
        
        Args:
            data: Dictionary with cell data
        
        Raises:
            ValueError: If data contains invalid columns
            sqlite3.DatabaseError: If database operation fails
        """
        try:
            self._validate_columns(data)
            cursor = self.conn.cursor()
            placeholders = ", ".join(["?"] * len(data))
            columns = ", ".join(data.keys())
            values = list(data.values())
            
            sql = f"INSERT OR REPLACE INTO cells ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, values)
            self.conn.commit()
            logger.info(f"Added/replaced cell: {data.get('id')}")
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to add cell {data.get('id', 'unknown')}: {e}")
            raise

    def update_cell(self, old_id, data):
        """Update an existing cell in the database.
        
        Args:
            old_id: Original cell ID
            data: Dictionary with updated cell data
        
        Raises:
            ValueError: If data contains invalid columns
            sqlite3.DatabaseError: If database operation fails
        """
        try:
            self._validate_columns(data)
            if old_id != data.get("id"):
                logger.info(f"Cell ID changed from {old_id} to {data.get('id')}, deleting old entry")
                self.delete_cell(old_id)
                self.add_cell(data)
                return

            cursor = self.conn.cursor()
            columns = list(data.keys())
            values = list(data.values())
            set_clause = ", ".join([f"{col}=?" for col in columns])
            sql = f"UPDATE cells SET {set_clause} WHERE id=?"
            cursor.execute(sql, values + [old_id])
            self.conn.commit()
            logger.info(f"Updated cell: {old_id}")
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to update cell {old_id}: {e}")
            raise

    def delete_cell(self, cell_id):
        """Delete a cell from the database.
        
        Args:
            cell_id: The ID of the cell to delete
        
        Raises:
            sqlite3.DatabaseError: If database operation fails
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM cells WHERE id=?", (cell_id,))
            self.conn.commit()
            logger.info(f"Deleted cell: {cell_id}")
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to delete cell {cell_id}: {e}")
            raise

    def get_all_cells(self):
        """Retrieve all cells from the database.
        
        Returns:
            pandas DataFrame with all cell data
        
        Raises:
            sqlite3.DatabaseError: If database operation fails
        """
        try:
            df = pd.read_sql_query("SELECT * FROM cells", self.conn)
            logger.debug(f"Retrieved {len(df)} cells from database")
            return df
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to retrieve cells: {e}")
            raise

    def get_cell(self, cell_id):
        """Retrieve a single cell by ID.
        
        Args:
            cell_id: The ID of the cell to retrieve
        
        Returns:
            Database row tuple or None if not found
        
        Raises:
            sqlite3.DatabaseError: If database operation fails
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM cells WHERE id=?", (cell_id,))
            result = cursor.fetchone()
            logger.debug(f"Retrieved cell: {cell_id}")
            return result
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to retrieve cell {cell_id}: {e}")
            raise

    def switch_database(self, db_name):
        """Switch to a different database file.
        
        Args:
            db_name: Path to the new database file
        
        Raises:
            sqlite3.DatabaseError: If database operation fails
        """
        try:
            self.close()
            self.db_name = db_name
            self.conn = sqlite3.connect(db_name)
            logger.info(f"Switched to database: {db_name}")
            self.create_table()
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to switch to database '{db_name}': {e}")
            raise

    def save(self):
        """Commit any pending changes to the database.
        
        Raises:
            sqlite3.DatabaseError: If database operation fails
        """
        try:
            self.conn.commit()
            logger.debug("Database saved")
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to save database: {e}")
            raise

    def save_as(self, new_path):
        """Save database to a new file path.
        
        Args:
            new_path: Path for the new database file
        
        Raises:
            sqlite3.DatabaseError: If database operation fails
        """
        try:
            with sqlite3.connect(new_path) as dest:
                self.conn.backup(dest)
            self.db_name = new_path
            logger.info(f"Database saved as: {new_path}")
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to save database as '{new_path}': {e}")
            raise

    def close(self):
        """Safely close the database connection.
        
        Logs a warning if the connection is already closed, but does not raise an error.
        This allows safe calling of close() multiple times.
        """
        if self.conn:
            try:
                self.conn.close()
                logger.info(f"Closed database: {self.db_name}")
            except sqlite3.ProgrammingError as e:
                logger.warning(f"Error closing database connection: {e}")
            finally:
                self.conn = None
        else:
            logger.debug("Database connection already closed")
