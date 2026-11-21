import os
import sqlite3
import pandas as pd
import logging

from data_import.file_status import FILE_STOPPED, FILE_UPDATED, file_key

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
        self.conn = None
        try:
            self.conn = sqlite3.connect(db_name)
            logger.info(f"Connected to database: {db_name}")
            self.create_table()
        except sqlite3.DatabaseError as e:
            self.close()
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

    def create_table(self, connection=None):
        """Create cells table if it does not exist."""
        connection = self.conn if connection is None else connection
        try:
            cursor = connection.cursor()
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
            columns = {column[1] for column in connection.execute("PRAGMA table_info(cells)")}
            missing = self.ALLOWED_COLUMNS - columns
            if missing:
                raise sqlite3.DatabaseError(f"Invalid battery database. Missing fields: {', '.join(sorted(missing))}")
            connection.commit()
            logger.debug("Database table 'cells' ready")
        except sqlite3.DatabaseError as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def add_cell(self, data):
        """Add a new cell, rejecting a duplicate ID.
        
        Args:
            data: Dictionary with cell data
        
        Raises:
            ValueError: If data contains invalid columns
            sqlite3.DatabaseError: If database operation fails
        """
        try:
            self._validate_columns(data)
            placeholders = ", ".join(["?"] * len(data))
            columns = ", ".join(data.keys())
            values = list(data.values())
            
            sql = f"INSERT INTO cells ({columns}) VALUES ({placeholders})"
            with self.conn:
                self.conn.execute(sql, values)
            logger.info(f"Added cell: {data.get('id')}")
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
            columns = list(data.keys())
            values = list(data.values())
            set_clause = ", ".join([f"{col}=?" for col in columns])
            sql = f"UPDATE cells SET {set_clause} WHERE id=?"
            with self.conn:
                self.conn.execute(sql, values + [old_id])
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

    def apply_file_scan(self, references, metadata):
        """Compare a completed scan with this database's previous observations."""
        timeout = self.conn.execute("PRAGMA busy_timeout").fetchone()[0]
        self.conn.execute("PRAGMA busy_timeout=0")
        persisted = True
        try:
            with self.conn:
                self.conn.execute("BEGIN IMMEDIATE")
                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS data_file_metadata (
                        path TEXT PRIMARY KEY,
                        modified_ns INTEGER,
                        size_bytes INTEGER
                    )
                """)
                statuses, status_writes, changed_metadata, obsolete_paths = self._compare_file_scan(references, metadata)
                self.conn.executemany("UPDATE cells SET status = ? WHERE id IS ?", status_writes)
                self.conn.executemany("""
                    INSERT INTO data_file_metadata(path, modified_ns, size_bytes) VALUES (?, ?, ?)
                    ON CONFLICT(path) DO UPDATE
                    SET modified_ns = excluded.modified_ns, size_bytes = excluded.size_bytes
                """, ((path, *observation) for path, observation in changed_metadata.items()))
                self.conn.executemany(
                    "DELETE FROM data_file_metadata WHERE path = ?", ((path,) for path in obsolete_paths)
                )
        except sqlite3.OperationalError as error:
            if (getattr(error, "sqlite_errorcode", 0) & 0xff) != sqlite3.SQLITE_READONLY:
                raise
            statuses, _, _, _ = self._compare_file_scan(references, metadata)
            persisted = False
        finally:
            self.conn.execute(f"PRAGMA busy_timeout={timeout}")
        return statuses, sum(status == FILE_UPDATED for status in statuses.values()), persisted

    def _compare_file_scan(self, references, metadata):
        previous = {}
        if self.conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'data_file_metadata'").fetchone():
            previous = {
                path: (modified_ns, size_bytes)
                for path, modified_ns, size_bytes in self.conn.execute(
                    "SELECT path, modified_ns, size_bytes FROM data_file_metadata"
                )
            }
        statuses = {}
        status_writes = []
        changed_metadata = {}
        linked_paths = set()
        for cell_id, path, status in self.conn.execute("SELECT id, file_path, status FROM cells"):
            key = file_key(path)
            if key is not None:
                linked_paths.add(key)
            if cell_id not in references or references[cell_id] != path:
                continue
            observed = metadata.get(key)
            last = previous.get(key)
            updated = observed is not None and last is not None and observed != last
            new_status = FILE_UPDATED if updated else FILE_STOPPED
            statuses[cell_id] = new_status
            if new_status != status:
                status_writes.append((new_status, cell_id))
            if key in metadata and observed is None and last is None:
                changed_metadata[key] = (None, None)
            elif observed is not None and observed != last:
                changed_metadata[key] = observed
        return statuses, status_writes, changed_metadata, previous.keys() - linked_paths

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
            connection = sqlite3.connect(db_name)
            try:
                self.create_table(connection)
            except sqlite3.DatabaseError:
                connection.close()
                raise
            self.close()
            self.db_name, self.conn = db_name, connection
            logger.info(f"Switched to database: {db_name}")
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
        """Save a copy and make that copy the active database.
        
        Args:
            new_path: Path for the new database file
        
        Raises:
            sqlite3.DatabaseError: If database operation fails
        """
        try:
            if os.path.exists(self.db_name) and os.path.exists(new_path):
                if os.path.samefile(self.db_name, new_path):
                    self.save()
                    return
            connection = sqlite3.connect(new_path)
            try:
                self.conn.backup(connection)
            except sqlite3.DatabaseError:
                connection.close()
                raise
            self.close()
            self.db_name, self.conn = new_path, connection
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
