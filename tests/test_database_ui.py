import sqlite3
import tempfile
import unittest
from pathlib import Path

from database import BatteryDatabase


class DatabaseUITests(unittest.TestCase):
    def test_duplicate_create_preserves_original(self):
        with BatteryDatabase(":memory:") as database:
            database.add_cell({"id": "A", "instrument": "Arbin"})
            with self.assertRaises(sqlite3.IntegrityError):
                database.add_cell({"id": "A", "instrument": "Maccor"})
            self.assertEqual(database.get_cell("A")[1], "Arbin")

    def test_duplicate_rename_preserves_both_records(self):
        with BatteryDatabase(":memory:") as database:
            database.add_cell({"id": "A", "instrument": "Arbin"})
            database.add_cell({"id": "B", "instrument": "Biologic"})
            with self.assertRaises(sqlite3.IntegrityError):
                database.update_cell("A", {"id": "B", "instrument": "Maccor"})
            self.assertEqual(database.get_cell("A")[1], "Arbin")
            self.assertEqual(database.get_cell("B")[1], "Biologic")

    def test_failed_switch_preserves_open_database(self):
        with tempfile.TemporaryDirectory() as temporary, BatteryDatabase(":memory:") as database:
            invalid_path = Path(temporary) / "absent" / "data.db"
            database.add_cell({"id": "A", "instrument": "Arbin"})
            with self.assertRaises(sqlite3.DatabaseError):
                database.switch_database(invalid_path)
            self.assertEqual(database.db_name, ":memory:")
            self.assertEqual(database.get_cell("A")[1], "Arbin")

    def test_incompatible_database_preserves_open_database(self):
        with tempfile.TemporaryDirectory() as temporary, BatteryDatabase(":memory:") as database:
            wrong_path = Path(temporary) / "incompatible.db"
            connection = sqlite3.connect(wrong_path)
            try:
                connection.execute("CREATE TABLE cells (id TEXT PRIMARY KEY)")
            finally:
                connection.close()
            database.add_cell({"id": "A", "instrument": "Arbin"})
            with self.assertRaises(sqlite3.DatabaseError):
                database.switch_database(wrong_path)
            self.assertEqual(database.get_cell("A")[1], "Arbin")

    def test_save_as_writes_future_changes_to_the_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            original = Path(temporary) / "original.db"
            copy = Path(temporary) / "copy.db"
            with BatteryDatabase(original) as database:
                database.add_cell({"id": "A", "instrument": "Arbin"})
                database.save_as(copy)
                database.add_cell({"id": "B", "instrument": "Maccor"})
            with BatteryDatabase(original) as database:
                self.assertIsNone(database.get_cell("B"))
            with BatteryDatabase(copy) as database:
                self.assertEqual(database.get_cell("A")[1], "Arbin")
                self.assertEqual(database.get_cell("B")[1], "Maccor")
