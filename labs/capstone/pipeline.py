"""Capstone starter: the Qafila Lakehouse as one Dagster code location.

Sources -> raw (DuckDB) -> dbt (staging, marts) -> gold tables published to Iceberg, with
asset checks from a data contract, a volume monitor and a nightly schedule.

    uv run python capstone/pipeline.py              # materialize everything once, in-process
    uv run dagster dev -f capstone/pipeline.py      # the UI: lineage, runs, checks, schedules

Run from labs/. Re-running is safe: every asset replaces what it owns.
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
LABS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LABS))

import dagster as dg  # noqa: E402
import yaml  # noqa: E402
from dagster_dbt import DbtCliResource, dbt_assets  # noqa: E402

import de_utils as U  # noqa: E402

HOME = U.ARTIFACTS / "capstone"
DB = HOME / "qafila.duckdb"
LAKE = HOME / "lakehouse"
CONTRACT = Path(__file__).with_name("contracts") / "payments.odcs.yaml"
HOME.mkdir(parents=True, exist_ok=True)
os.environ["QAFILA_DUCKDB"] = DB.as_posix()          # dbt's profiles.yml reads this


def sql(query: str, params: list | None = None):
    con = U.connect(DB)
    con.execute("SET python_enable_replacements = false")
    try:
        return con.execute(query, params or []).fetchall()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# 1. Ingestion: one asset per source table (keys match dbt sources: raw.<table>)
# ---------------------------------------------------------------------------
RAW = U.RAW.as_posix()
APP_TABLES = ["customers", "stores", "products", "orders", "order_items"]


def _load_app_table(table: str):
    sql("INSTALL sqlite; LOAD sqlite;")
    sql("CREATE SCHEMA IF NOT EXISTS raw")

    con = U.connect(DB)

    try:
        con.sql(
            f"ATTACH '{RAW}/qafila_oltp.sqlite' "
            "AS app (TYPE sqlite, READ_ONLY)"
        )

        if table == "customers":
            con.execute("""
                CREATE OR REPLACE TABLE raw.customers AS
                SELECT
                    * EXCLUDE (
                        full_name,
                        email,
                        mobile,
                        national_id,
                        iban
                    ),

                    '[REDACTED]'::VARCHAR AS full_name,
                    NULL::VARCHAR AS email,
                    NULL::VARCHAR AS mobile,

                    'person_' || CAST(
                        min(customer_id) OVER (
                            PARTITION BY coalesce(
                                nullif(trim(national_id), ''),
                                'account_' || CAST(customer_id AS VARCHAR)
                            )
                        ) AS VARCHAR
                    ) AS national_id,

                    NULL::VARCHAR AS iban,
                    now()::TIMESTAMP AS _loaded_at

                FROM app.customers
            """)

        else:
            con.execute(f"""
                CREATE OR REPLACE TABLE raw.{table} AS
                SELECT
                    *,
                    now()::TIMESTAMP AS _loaded_at
                FROM app.{table}
            """)

        return con.execute(
            f"SELECT count(*) FROM raw.{table}"
        ).fetchone()[0]

    finally:
        con.close()

def _make_app_asset(table: str):
    @dg.asset(key=["raw", table], group_name="ingest", kinds={"sqlite", "duckdb"}, description=f"App database: {table}")
    def _asset():
        return dg.MaterializeResult(metadata={"rows": _load_app_table(table)})
    return _asset


app_assets = [_make_app_asset(t) for t in APP_TABLES]

FILE_SOURCES = {
    "payments": f"""read_json('{RAW}/api/payments.jsonl', format='newline_delimited', columns={{
        payment_id: 'VARCHAR', order_id: 'BIGINT', amount: 'DOUBLE', currency: 'VARCHAR', method: 'VARCHAR',
        status: 'VARCHAR', card_last4: 'VARCHAR', created_at: 'VARCHAR', updated_at: 'VARCHAR', wallet_provider: 'VARCHAR'}})""",
    "delivery_events": f"""read_json('{RAW}/events/delivery_events.jsonl', format='newline_delimited', columns={{
        event_id: 'VARCHAR', order_id: 'BIGINT', store_id: 'VARCHAR', city: 'VARCHAR', courier_id: 'VARCHAR',
        event_type: 'VARCHAR', event_time: 'VARCHAR', lat: 'DOUBLE', lon: 'DOUBLE', ingest_time: 'VARCHAR'}})""",
    "supplier_prices": f"read_csv('{RAW}/suppliers/*.csv', union_by_name=true, all_varchar=true, filename=true)",
    "reviews": f"read_csv('{RAW}/reviews.csv')",
}
def _load_payments_incremental(reader: str):
    con = U.connect(DB)

    try:
        con.execute("BEGIN TRANSACTION")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Read the source and remove identical duplicates.
        con.execute(f"""
            CREATE TEMP TABLE payment_source AS
            SELECT DISTINCT *
            FROM {reader}
        """)

        invalid = con.execute("""
            SELECT count(*)
            FROM payment_source
            WHERE payment_id IS NULL
               OR try_cast(updated_at AS TIMESTAMPTZ) IS NULL
        """).fetchone()[0]

        if invalid:
            raise ValueError(
                f"Payments: {invalid} rows have an invalid key or updated_at"
            )

        # Reject different versions with the same key and timestamp.
        conflicts = con.execute("""
            SELECT count(*)
            FROM (
                SELECT
                    payment_id,
                    CAST(updated_at AS TIMESTAMPTZ) AS version_at
                FROM payment_source
                GROUP BY 1, 2
                HAVING count(*) > 1
            )
        """).fetchone()[0]

        if conflicts:
            raise ValueError(
                f"Payments: {conflicts} conflicting versions share a timestamp"
            )

        # Keep the existing destination; create it only if missing.
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.payments AS
            SELECT *
            FROM payment_source
            WHERE false
        """)

        invalid_target = con.execute("""
            SELECT count(*)
            FROM raw.payments
            WHERE payment_id IS NULL
               OR try_cast(updated_at AS TIMESTAMPTZ) IS NULL
        """).fetchone()[0]

        if invalid_target:
            raise ValueError(
                "raw.payments contains invalid keys or timestamps"
            )

        duplicates = con.execute("""
            SELECT count(*) - count(DISTINCT payment_id)
            FROM raw.payments
        """).fetchone()[0]

        if duplicates:
            raise ValueError(
                "raw.payments already contains duplicate keys"
            )

        # Read the latest successfully stored timestamp.
        watermark = con.execute("""
            SELECT max(CAST(updated_at AS TIMESTAMPTZ))
            FROM raw.payments
        """).fetchone()[0]

        # Recheck the last three days and keep each payment's latest version.
        con.execute("""
            CREATE TEMP TABLE payment_batch AS
            SELECT *
            FROM payment_source
            WHERE (
                CAST(? AS TIMESTAMPTZ) IS NULL
                OR CAST(updated_at AS TIMESTAMPTZ)
                   >= CAST(? AS TIMESTAMPTZ) - INTERVAL '3 days'
            )
            QUALIFY row_number() OVER (
                PARTITION BY payment_id
                ORDER BY CAST(updated_at AS TIMESTAMPTZ) DESC
            ) = 1
        """, [watermark, watermark])

        columns = [
            row[0]
            for row in con.execute(
                "DESCRIBE raw.payments"
            ).fetchall()
        ]

        def quoted(name):
            return '"' + name.replace('"', '""') + '"'

        column_list = ", ".join(
            quoted(c) for c in columns
        )

        value_list = ", ".join(
            f"s.{quoted(c)}" for c in columns
        )

        update_list = ", ".join(
            f"{quoted(c)} = s.{quoted(c)}"
            for c in columns
            if c != "payment_id"
        )

        differences = " OR ".join(
            f"t.{quoted(c)} IS DISTINCT FROM s.{quoted(c)}"
            for c in columns
            if c != "payment_id"
        )

        before = con.execute("""
            SELECT count(*) FROM raw.payments
        """).fetchone()[0]

        con.execute(f"""
            MERGE INTO raw.payments AS t
            USING payment_batch AS s
            ON t.payment_id = s.payment_id

            WHEN MATCHED
                 AND CAST(s.updated_at AS TIMESTAMPTZ)
                     >= CAST(t.updated_at AS TIMESTAMPTZ)
                 AND ({differences})
            THEN UPDATE SET {update_list}

            WHEN NOT MATCHED
            THEN INSERT ({column_list})
                 VALUES ({value_list})
        """)

        after = con.execute("""
            SELECT count(*) FROM raw.payments
        """).fetchone()[0]

        batch_rows = con.execute("""
            SELECT count(*) FROM payment_batch
        """).fetchone()[0]

        con.execute("COMMIT")

    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise

    finally:
        con.close()

    print(
        f"payments incremental: before={before}, "
        f"after={after}, batch={batch_rows}"
    )

    return {
        "rows": after,
        "rows_before": before,
        "batch_rows": batch_rows,
        "watermark_before": str(watermark),
        "load_mode": "incremental_merge",
    }
def _load_events_incremental(reader: str):
    con = U.connect(DB)

    try:
        con.execute("BEGIN TRANSACTION")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        con.execute(f"""
            CREATE TEMP TABLE event_source AS
            SELECT DISTINCT *
            FROM {reader}
        """)

        invalid = con.execute("""
            SELECT count(*)
            FROM event_source
            WHERE event_id IS NULL
               OR try_cast(ingest_time AS TIMESTAMPTZ) IS NULL
        """).fetchone()[0]

        if invalid:
            raise ValueError(
                f"Events: {invalid} rows have invalid keys or ingest_time"
            )

        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.delivery_events AS
            SELECT *
            FROM event_source
            WHERE false
        """)

        invalid_target = con.execute("""
            SELECT count(*)
            FROM raw.delivery_events
            WHERE event_id IS NULL
               OR try_cast(ingest_time AS TIMESTAMPTZ) IS NULL
        """).fetchone()[0]

        if invalid_target:
            raise ValueError(
                "raw.delivery_events contains invalid keys or ingest_time"
            )

        before = con.execute("""
            SELECT count(*) FROM raw.delivery_events
        """).fetchone()[0]

        # Migrate the existing full-refresh table:
        # keep one stored copy per event_id.
        con.execute("""
            DELETE FROM raw.delivery_events
            WHERE rowid IN (
                SELECT rowid
                FROM raw.delivery_events
                QUALIFY row_number() OVER (
                    PARTITION BY event_id
                    ORDER BY
                        CAST(ingest_time AS TIMESTAMPTZ) DESC,
                        rowid
                ) > 1
            )
        """)

        after_cleanup = con.execute("""
            SELECT count(*) FROM raw.delivery_events
        """).fetchone()[0]

        watermark = con.execute("""
            SELECT max(CAST(ingest_time AS TIMESTAMPTZ))
            FROM raw.delivery_events
        """).fetchone()[0]

        # Review recent arrivals and include every unseen event,
        # even when its timestamp falls outside the review window.
        con.execute("""
            CREATE TEMP TABLE event_batch AS
            SELECT s.*
            FROM event_source AS s
            WHERE (
                CAST(? AS TIMESTAMPTZ) IS NULL
                OR CAST(s.ingest_time AS TIMESTAMPTZ)
                   >= CAST(? AS TIMESTAMPTZ) - INTERVAL '3 days'
                OR NOT EXISTS (
                    SELECT 1
                    FROM raw.delivery_events AS t
                    WHERE t.event_id = s.event_id
                )
            )
            QUALIFY row_number() OVER (
                PARTITION BY s.event_id
                ORDER BY
                    CAST(s.ingest_time AS TIMESTAMPTZ) DESC,
                    to_json(s) DESC
            ) = 1
        """, [watermark, watermark])

        columns = [
            row[0]
            for row in con.execute(
                "DESCRIBE raw.delivery_events"
            ).fetchall()
        ]

        def quoted(name):
            return '"' + name.replace('"', '""') + '"'

        column_list = ", ".join(
            quoted(c) for c in columns
        )

        value_list = ", ".join(
            f"s.{quoted(c)}" for c in columns
        )

        con.execute(f"""
            MERGE INTO raw.delivery_events AS t
            USING event_batch AS s
            ON t.event_id = s.event_id

            WHEN NOT MATCHED
            THEN INSERT ({column_list})
                 VALUES ({value_list})
        """)

        after = con.execute("""
            SELECT count(*) FROM raw.delivery_events
        """).fetchone()[0]

        batch_rows = con.execute("""
            SELECT count(*) FROM event_batch
        """).fetchone()[0]

        con.execute("COMMIT")

    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise

    finally:
        con.close()

    inserted = after - after_cleanup
    removed_duplicates = before - after_cleanup

    print(
        f"events incremental: before={before}, "
        f"after={after}, inserted={inserted}, "
        f"removed_duplicates={removed_duplicates}, "
        f"batch={batch_rows}"
    )

    return {
        "rows": after,
        "rows_inserted": inserted,
        "duplicates_removed": removed_duplicates,
        "batch_rows": batch_rows,
        "watermark_before": str(watermark),
        "load_mode": "incremental_insert",
    }
def _make_file_asset(table: str, reader: str):
    @dg.asset(
        key=["raw", table],
        group_name="ingest",
        kinds={"duckdb"},
        description=f"File/API source: {table}",
    )
    def _asset():
        if table == "payments":
            return dg.MaterializeResult(
                metadata=_load_payments_incremental(reader)
            )

        if table == "delivery_events":
            return dg.MaterializeResult(
                metadata=_load_events_incremental(reader)
            )

        sql("CREATE SCHEMA IF NOT EXISTS raw")
        sql(
            f"CREATE OR REPLACE TABLE raw.{table} "
            f"AS SELECT * FROM {reader}"
        )

        return dg.MaterializeResult(
            metadata={
                "rows": sql(
                    f"SELECT count(*) FROM raw.{table}"
                )[0][0]
            }
        )

    return _asset

file_assets = [_make_file_asset(t, r) for t, r in FILE_SOURCES.items()]


# ---------------------------------------------------------------------------
# 2. Contract checks on the payments feed (subset of ODCS: rowCount, duplicateValues, invalidValues, sql)
# ---------------------------------------------------------------------------
@dg.asset_check(asset=dg.AssetKey(["raw", "payments"]), description="ODCS contract: capstone/contracts/payments.odcs.yaml")
def payments_contract():
    contract = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    obj = contract["schema"][0]
    failures, checked = [], 0
    rules = [(None, r) for r in obj.get("quality", [])] + [(p["name"], r) for p in obj["properties"] for r in p.get("quality", [])]
    for col, rule in rules:
        if rule.get("type") == "sql":
            value = sql(rule["query"].replace("{object}", "raw.payments"))[0][0]
        elif rule["metric"] == "rowCount":
            value = sql("SELECT count(*) FROM raw.payments")[0][0]
        elif rule["metric"] == "duplicateValues":
            cols = ", ".join(rule["arguments"]["properties"])
            value = sql(f"SELECT count(*) - count(DISTINCT ({cols})) FROM raw.payments")[0][0]
        elif rule["metric"] == "invalidValues" and "validValues" in rule["arguments"]:
            allowed = ", ".join(f"'{v}'" for v in rule["arguments"]["validValues"])
            value = sql(f"SELECT count(*) FROM raw.payments WHERE {col} IS NOT NULL AND {col} NOT IN ({allowed})")[0][0]
        elif rule["metric"] == "invalidValues":
            value = sql(f"SELECT count(*) FROM raw.payments WHERE {col} IS NOT NULL AND NOT regexp_full_match({col}, ?)",
                        [rule["arguments"]["pattern"]])[0][0]
        else:
            continue
        checked += 1
        op, target = next((k, v) for k, v in rule.items() if k.startswith("must"))
        ok = {"mustBe": value == target, "mustBeGreaterThan": value > target, "mustBeLessThan": value < target}[op]
        if not ok:
            failures.append(f"{rule.get('id', col)}: {value} ({op} {target})")
    return dg.AssetCheckResult(passed=not failures, severity=dg.AssetCheckSeverity.WARN,
                               metadata={"rules_checked": checked, "failures": "; ".join(failures) or "none"})


# ---------------------------------------------------------------------------
# 3. dbt: staging, intermediate, marts (every dbt test becomes an asset check)
# ---------------------------------------------------------------------------
MANIFEST = U.DBT_PROJECT / "target" / "manifest.json"
if not MANIFEST.exists():
    U.run_dbt(["parse"], db_path=DB, quiet=True)


@dbt_assets(manifest=MANIFEST)
def qafila_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# ---------------------------------------------------------------------------
# 4. Gold: publish marts to Iceberg so any engine can read them; monitor volume
# ---------------------------------------------------------------------------
@dg.asset(key=["gold", "daily_city_sales"], group_name="publish", kinds={"iceberg"},
          deps=[dg.AssetKey(["marts", "mart_daily_city_sales"])])
def gold_daily_city_sales():
    """The dashboard mart as an Iceberg table (full refresh: overwrite)."""
    con = U.connect(DB)
    try:
        data = con.sql("SELECT * FROM marts.mart_daily_city_sales ORDER BY order_date, city").to_arrow_table()
    finally:
        con.close()
    catalog = U.get_catalog(LAKE)
    catalog.create_namespace_if_not_exists("gold")
    if catalog.table_exists("gold.daily_city_sales"):
        table = catalog.load_table("gold.daily_city_sales")
        table.overwrite(data)
    else:
        table = catalog.create_table("gold.daily_city_sales", schema=data.schema)
        table.append(data)
    return dg.MaterializeResult(metadata={"rows": data.num_rows, "snapshots": len(table.snapshots()),
                                          "metadata_location": table.metadata_location})


@dg.asset_check(asset=dg.AssetKey(["marts", "fct_orders"]), description="Volume: no day below 70% of its weekday's 4-week median")
def orders_volume():
    rows = sql("""
        WITH d AS (SELECT order_date, count(*)::DOUBLE AS n FROM marts.fct_orders GROUP BY 1)
        SELECT order_date, n,
               (SELECT median(n) FROM d d2 WHERE d2.order_date < d.order_date AND d2.order_date >= d.order_date - 28
                  AND dayofweek(d2.order_date) = dayofweek(d.order_date)) AS expected
        FROM d ORDER BY order_date""")
    low = [str(day) for day, n, exp in rows if exp and n < 0.7 * exp]
    return dg.AssetCheckResult(passed=not low, metadata={"days_checked": len(rows), "low_days": ", ".join(low) or "none"})


# 5. Operations view and code location
# ---------------------------------------------------------------------------
@dg.asset(
    key=["serve", "operations_view"],
    group_name="serve",
    deps=[dg.AssetKey(["gold", "daily_city_sales"])],
)
def operations_view():
    from html import escape
    from datetime import datetime, timezone

    rows = sql("""
        SELECT
            order_date,
            city,
            orders,
            delivered_orders,
            revenue_sar,
            avg_basket_sar,
            sla_rate
        FROM marts.mart_daily_city_sales
        ORDER BY order_date DESC, city
    """)

    if not rows:
        raise ValueError("Operations mart is empty")

    def number(value, decimals=0):
        return "—" if value is None else f"{value:,.{decimals}f}"

    table_rows = []

    for day, city, orders, delivered, revenue, basket, sla in rows:
        cells = [
            escape(str(day)),
            escape(city),
            number(orders),
            number(delivered),
            number(revenue, 2),
            number(basket, 2),
            "—" if sla is None else f"{sla * 100:.1f}%",
        ]

        table_rows.append(
            "<tr>"
            + "".join(f"<td>{cell}</td>" for cell in cells)
            + "</tr>"
        )

    generated_at = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )

    page = """<!doctype html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>قافلة · عرض العمليات</title>
<style>
body {
    font-family: system-ui, sans-serif;
    background: #f3f6fa;
    color: #172c42;
    margin: 0;
    padding: 30px;
}
main { max-width: 1200px; margin: auto; }
.panel {
    background: white;
    padding: 22px;
    border-radius: 16px;
    overflow-x: auto;
}
table {
    width: 100%;
    border-collapse: collapse;
    white-space: nowrap;
}
th, td {
    padding: 13px;
    text-align: right;
    border-bottom: 1px solid #e5ebf2;
}
th { background: #eaf1f8; }
.note { color: #53677b; line-height: 1.9; }
</style>
</head>
<body>
<main>
<h1>قافلة · عرض العمليات</h1>
<p>الطلبات والمبيعات اليومية حسب المدينة</p>
<p class="note">آخر يوم بيانات: __LATEST__</p>
<p class="note">إنشاء الصفحة: __GENERATED__</p>
<div class="panel">
<table>
<thead>
<tr>
<th>اليوم بتوقيت الرياض</th>
<th>المدينة</th>
<th>جميع الطلبات</th>
<th>الطلبات المسلّمة</th>
<th>المبيعات · ريال</th>
<th>متوسط السلة · ريال</th>
<th>الالتزام بمدة التوصيل</th>
</tr>
</thead>
<tbody>__ROWS__</tbody>
</table>
</div>
<p class="note">
المبيعات ومتوسط السلة للطلبات المسلّمة ويستبعدان رسوم التوصيل.
نسبة الالتزام تشمل الطلبات ذات معلومات الالتزام المعروفة فقط.
الشرطة — تعني أن القيمة غير متوفرة.
</p>
<p class="note">
تُحدّث الصفحة بعد نجاح تشغيل المنصة. البيانات اصطناعية من الكورس.
</p>
</main>
</body>
</html>
"""

    page = (
        page.replace("__LATEST__", escape(str(rows[0][0])))
        .replace("__GENERATED__", escape(generated_at))
        .replace("__ROWS__", "".join(table_rows))
    )

    output = HOME / "operations.html"
    temporary = HOME / "operations.html.tmp"
    temporary.write_text(page, encoding="utf-8")
    temporary.replace(output)

    return dg.MaterializeResult(
        metadata={
            "rows": len(rows),
            "output_path": str(output),
        }
    )


everything = dg.define_asset_job(
    "refresh_all",
    selection=dg.AssetSelection.all(),
    executor_def=dg.in_process_executor,
)

defs = dg.Definitions(
    assets=[
        *app_assets,
        *file_assets,
        qafila_dbt,
        gold_daily_city_sales,
        operations_view,
    ],
    asset_checks=[payments_contract, orders_volume],
    jobs=[everything],
    schedules=[
        dg.ScheduleDefinition(
            job=everything,
            cron_schedule="30 1 * * *",
            execution_timezone="Asia/Riyadh",
        )
    ],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(U.DBT_PROJECT),
            profiles_dir=str(U.DBT_PROJECT),
        )
    },
    executor=dg.in_process_executor,
)


if __name__ == "__main__":
    sys.stdout.reconfigure(errors="replace")
    U.ensure_data()

    result = dg.materialize(
        [
            *app_assets,
            *file_assets,
            qafila_dbt,
            gold_daily_city_sales,
            operations_view,
            payments_contract,
            orders_volume,
        ],
        resources={
            "dbt": DbtCliResource(
                project_dir=str(U.DBT_PROJECT),
                profiles_dir=str(U.DBT_PROJECT),
            )
        },
        run_config={
            "loggers": {
                "console": {
                    "config": {"log_level": "ERROR"}
                }
            }
        },
    )

    mats = result.get_asset_materialization_events()
    checks = result.get_asset_check_evaluations()

    print(
        f"\nrun {'succeeded' if result.success else 'FAILED'}: "
        f"{len(mats)} assets materialized, {len(checks)} checks"
    )

    for check in checks:
        if not check.passed:
            metadata = {
                key: value.value
                for key, value in check.metadata.items()
            }

            detail = metadata.get("failures") or (
                f"{metadata.get('dagster_dbt/failed_row_count', '?')} "
                "failing rows"
            )

            print(
                f"  WARNING: {check.asset_key.to_user_string()} "
                f"· {check.check_name}: {detail}"
            )

    print(f"warehouse: {DB}")
    print(f"lakehouse: {LAKE}")
    print(f"operations view: {HOME / 'operations.html'}")