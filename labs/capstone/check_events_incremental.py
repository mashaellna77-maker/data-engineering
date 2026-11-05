from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pipeline as p


def read_rows(db_path):
    con = duckdb.connect(str(db_path))
    try:
        return con.execute("""
            SELECT event_id, event_time, ingest_time
            FROM raw.delivery_events
            ORDER BY event_id
        """).fetchall()
    finally:
        con.close()


original_db = p.DB

try:
    with TemporaryDirectory() as folder:
        p.DB = Path(folder) / "test_events.duckdb"

        # First batch includes a repeated event.
        first_batch = """
            (
                SELECT * FROM (
                    VALUES
                        ('e1', '2026-09-20', '2026-09-20T10:00:00Z'),
                        ('e1', '2026-09-20', '2026-09-20T10:00:00Z'),
                        ('e2', '2026-09-20', '2026-09-20T11:00:00Z')
                ) AS v(event_id, event_time, ingest_time)
            )
        """

        p._load_events_incremental(first_batch)

        first_rows = read_rows(p.DB)
        assert len(first_rows) == 2
        print("PASS: first load stored two unique events")

        # e3 happened earlier but arrived later.
        # e4 is unseen and outside the three-day review window.
        second_batch = """
            (
                SELECT * FROM (
                    VALUES
                        ('e1', '2026-09-20', '2026-09-20T10:00:00Z'),
                        ('e3', '2026-09-01', '2026-09-21T10:00:00Z'),
                        ('e3', '2026-09-01', '2026-09-21T10:00:00Z'),
                        ('e4', '2026-09-01', '2026-09-01T10:00:00Z')
                ) AS v(event_id, event_time, ingest_time)
            )
        """

        p._load_events_incremental(second_batch)

        rows = read_rows(p.DB)
        events = {row[0]: row for row in rows}

        assert len(rows) == 4
        assert events["e3"][1] == "2026-09-01"
        print("PASS: late event loaded")

        assert "e4" in events
        print("PASS: unseen event outside review window loaded")

        assert all(row in rows for row in first_rows)
        print("PASS: previous events preserved")

        assert len(events) == len(rows)
        print("PASS: no duplicate event IDs")

        p._load_events_incremental(second_batch)

        assert read_rows(p.DB) == rows
        print("PASS: replay changed neither rows nor values")

        print("ALL EVENT CHECKS PASSED")

finally:
    p.DB = original_db