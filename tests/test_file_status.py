import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from database import BatteryDatabase


class FileStatusTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.path = self.directory / "sample.res"
        self.path.write_bytes(b"old")
        self.db_path = self.directory / "library.db"
        self.references = {"ONE": str(self.path), "TWO": str(self.path), "UNLINKED": None}
        with BatteryDatabase(self.db_path) as database:
            for cell_id, path in self.references.items():
                database.add_cell({"id": cell_id, "file_path": path, "status": "Active"})

    def scan(self, database, references=None):
        from data_import.file_status import scan_file_metadata

        references = self.references if references is None else references
        metadata = scan_file_metadata(references.values())
        database.apply_file_scan(references, metadata)

    @staticmethod
    def statuses(database):
        return dict(database.conn.execute("SELECT id, status FROM cells"))

    def test_persisted_baseline_reports_update_for_one_scan(self):
        with BatteryDatabase(self.db_path) as database:
            self.scan(database)
            self.assertEqual(self.statuses(database), {"ONE": "Stopped", "TWO": "Stopped", "UNLINKED": "Stopped"})
        initial_stat = self.path.stat()
        self.path.write_bytes(b"new")
        os.utime(self.path, ns=(initial_stat.st_atime_ns, initial_stat.st_mtime_ns + 3_000_000_000))
        with BatteryDatabase(self.db_path) as database:
            self.scan(database)
            self.assertEqual(self.statuses(database), {"ONE": "Update", "TWO": "Update", "UNLINKED": "Stopped"})
        with BatteryDatabase(self.db_path) as database:
            self.scan(database)
            self.assertEqual(self.statuses(database), {"ONE": "Stopped", "TWO": "Stopped", "UNLINKED": "Stopped"})

    def test_missing_file_retains_its_previous_known_metadata(self):
        with BatteryDatabase(self.db_path) as database:
            self.scan(database)
            initial_stat = self.path.stat()
            self.path.unlink()
            self.scan(database)
            self.assertEqual(self.statuses(database)["ONE"], "Stopped")
            self.path.write_bytes(b"more bytes")
            os.utime(self.path, ns=(initial_stat.st_atime_ns, initial_stat.st_mtime_ns))
            self.scan(database)
            self.assertEqual(self.statuses(database)["ONE"], "Update")

    def test_file_created_after_a_scan_reports_update(self):
        self.path.unlink()
        with BatteryDatabase(self.db_path) as database:
            self.scan(database)
            self.assertEqual(self.statuses(database)["ONE"], "Stopped")
            self.path.write_bytes(b"first instrument output")
            self.scan(database)
            self.assertEqual(self.statuses(database)["ONE"], "Update")

    def test_separate_databases_keep_separate_baselines(self):
        other_path = self.directory / "other.db"
        with BatteryDatabase(self.db_path) as first, BatteryDatabase(other_path) as second:
            self.scan(first)
            self.path.write_bytes(b"changed size")
            second.add_cell({"id": "ONE", "file_path": str(self.path), "status": "Active"})
            self.scan(second)
            self.assertEqual(self.statuses(second)["ONE"], "Stopped")
            self.scan(first)
            self.assertEqual(self.statuses(first)["ONE"], "Update")

    def test_read_only_database_can_report_status_without_changing_baseline(self):
        from data_import.file_status import scan_file_metadata

        with BatteryDatabase(self.db_path) as database:
            self.scan(database)
            self.path.write_bytes(b"updated on disk")
            database.conn.execute("PRAGMA query_only = ON")
            metadata = scan_file_metadata(self.references.values())
            statuses, updated, persisted = database.apply_file_scan(self.references, metadata)
            self.assertEqual(statuses["ONE"], "Update")
            self.assertEqual(updated, 2)
            self.assertFalse(persisted)
            self.assertEqual(self.statuses(database)["ONE"], "Stopped")

    def test_apply_does_not_touch_a_record_relinked_during_the_scan(self):
        from data_import.file_status import scan_file_metadata

        with BatteryDatabase(self.db_path) as database:
            self.scan(database)
            self.path.write_bytes(b"changed")
            observed = scan_file_metadata(self.references.values())
            database.update_cell("ONE", {"file_path": str(self.directory / "new.res"), "status": "Active"})
            database.apply_file_scan(self.references, observed)
            self.assertEqual(self.statuses(database)["ONE"], "Active")
            self.assertEqual(self.statuses(database)["TWO"], "Update")

    def test_shared_paths_are_examined_once_and_file_contents_stay_closed(self):
        from data_import.file_status import scan_file_metadata

        equivalent = os.path.join(str(self.path.parent), ".", self.path.name)
        with patch("data_import.file_status.os.stat", wraps=os.stat) as file_stat, patch(
            "builtins.open", side_effect=AssertionError("The scan must not open data files")
        ):
            self.assertEqual(len(scan_file_metadata([str(self.path), equivalent, None, ""])), 1)
            self.assertEqual(file_stat.call_count, 1)
