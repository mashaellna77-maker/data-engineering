# Qafila Lakehouse architecture

## Platform coverage

| Stage | Implementation | Tool and reason |
|---|---|---|
| Sources | SQLite app database, JSONL payments extract and delivery events, supplier CSVs and reviews. | Course sources provide database and file source types. |
| Ingest | App loaders, payment incremental merge and event incremental insert. | Python and DuckDB keep the local pipeline simple and runnable offline. |
| Store | raw tables in DuckDB; gold daily city sales published to Iceberg. | DuckDB supports local analytics; Iceberg provides an open table format. |
| Transform | staging, intermediate, dimensions, facts and daily city sales. | dbt provides SQL models and dependency tracking. |
| Test | dbt data tests, unit tests, payments contract and incremental-loader checks. | Automated checks validate data and rerun behavior. |
| Serve | Generated operations.html with daily city aggregates. | Standard Python and HTML provide a local operations view without extra dependencies. |
| Orchestrate | Dagster assets, refresh_all job and schedule definition. | Dagster manages dependencies and exposes run status and lineage. |
| Observe | Dagster logs, asset metadata, quality checks and orders-volume monitor. | Makes failures, volumes and quality warnings visible. |
| Govern | Customer identifier protection, column classification, local folder permissions and aggregate-only operations view. | Protects identifiers and documents handling decisions. |

## Execution
Run from labs:
uv run python capstone/pipeline.py

The command loads sources, builds and tests dbt models,
publishes the gold Iceberg table and generates the operations view.

## Limits
- Payment and event loaders still scan their source files.
- Payments use a three-day update review window.
- Events are treated as immutable records keyed by event_id.
- Other app and file loaders remain full refresh.
- Iceberg publication overwrites the gold table.
- The schedule is defined; scheduled execution has not been demonstrated.
- The latest reconciliation test reports 244 mismatches pending investigation.
- Source-quality warnings remain visible after quarantine.
