from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pipeline as p


def read_rows(db_path):
    con = duckdb.connect(str(db_path))
    try:
        return con.execute("""
            SELECT payment_id, amount, updated_at
            FROM raw.payments
            ORDER BY payment_id
        """).fetchall()
    finally:
        con.close()


original_db = p.DB

try:
    with TemporaryDirectory() as folder:
        p.DB = Path(folder) / "test_payments.duckdb"

        # First load: two payments.
        first_batch = """
            (
                SELECT * FROM (
                    VALUES
                        ('p1', 100.0, '2026-09-20T10:00:00Z'),
                        ('p2', 200.0, '2026-09-20T11:00:00Z')
                ) AS v(payment_id, amount, updated_at)
            )
        """

        p._load_payments_incremental(first_batch)

        assert len(read_rows(p.DB)) == 2
        print("PASS: first load inserted two payments")

        # Update p1 and insert p3. p2 must remain in the destination.
        second_batch = """
            (
                SELECT * FROM (
                    VALUES
                        ('p1', 150.0, '2026-09-21T10:00:00Z'),
                        ('p3', 300.0, '2026-09-21T11:00:00Z')
                ) AS v(payment_id, amount, updated_at)
            )
        """

        p._load_payments_incremental(second_batch)

        rows = read_rows(p.DB)
        amounts = {row[0]: row[1] for row in rows}

        assert len(rows) == 3
        assert amounts["p1"] == 150.0
        assert amounts["p2"] == 200.0
        assert amounts["p3"] == 300.0

        print("PASS: existing payment updated")
        print("PASS: new payment inserted")
        print("PASS: previous payment preserved")

        # Replay the same batch and compare every stored value.
        before_replay = rows

        p._load_payments_incremental(second_batch)

        after_replay = read_rows(p.DB)

        assert after_replay == before_replay
        print("PASS: replay changed neither rows nor values")

        print("ALL PAYMENT CHECKS PASSED")

finally:
    p.DB = original_db