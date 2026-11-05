# Reproduction verification

Verified on Windows in a separate local clone: qafila-capstone-check.

Commands completed:
- uv sync --frozen
- uv run python prepare_data.py
- uv run python capstone/pipeline.py
- uv run python capstone/check_payments_incremental.py
- uv run python capstone/check_events_incremental.py

Results:
- Pipeline succeeded: 34 assets materialized, 47 checks.
- Operations HTML generated.
- All payment incremental checks passed.
- All event incremental checks passed.
- Known quality warnings remain, including 244 reconciliation mismatches.

This verifies a fresh local environment on the same machine,
not execution on a different operating system.
