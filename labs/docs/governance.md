# Governance note

## Scope
Qafila course data is synthetic.
Personal-data fields are nevertheless handled as personal data
for this capstone.

Project governance owner: Mashael Alotaibi.

## Column classification
These are project handling categories, not a formal legal assessment.

| Table | Columns | Classification and handling |
|---|---|---|
| customers | full_name, email, mobile, national_id, iban | Direct identifiers. In the analytical raw copy, the name is redacted, contact details and IBAN are null, and national_id contains a person token. |
| customers | customer_id, city, district, created_at, updated_at, is_deleted, _loaded_at | Restricted customer-linked information. Retained for processing; excluded from public row-level exports. |
| delivery_events | courier_id | Personal identifier. Retained in processing; excluded from the public operations view. |
| delivery_events | event_id, order_id, city, event_type, event_time, ingest_time, lat, lon | Restricted linkable activity and location information. Retained for processing; only aggregate metrics may be served publicly. |
| orders | customer_id, order_id | Linkable identifiers. Retained for joins and tests; excluded from public row-level exports. |
| orders | store_id, status, payment_method, promo_code, delivery_fee, created_at, updated_at, _loaded_at | Restricted when associated with an identifiable order. |
| order_items | order_id, line_no, product_id, quantity, unit_price, discount, _loaded_at | Restricted purchase information when linked to an order. |
| payments | payment_id, order_id, card_last4 | Restricted payment identifiers and partial financial information. Excluded from the public operations view. |
| payments | amount, currency, method, status, created_at, updated_at, wallet_provider | Restricted transaction information when linked to a payment or order. |
| reviews | customer_id, review_id, review_text, created_at | Restricted identifiers, linkable activity and potentially identifying free text. No public row-level review export. |
| reviews | product_id, rating, lang | Restricted within customer-linked review records; aggregate reporting is permitted. |
| stores | all columns | Business reference data; coordinates describe stores rather than individual movements. |
| products | all columns | Business product data. |
| supplier_prices | all columns | Internal commercial information. |

Derived person_id and customer_id remain linkable identifiers.
Pseudonymization does not make customer records anonymous.

## Implemented customer protection
Protection is applied by the customer ingestion loader.
The current verification found:
- 7442 customers.
- 0 rows violating the configured identifier-protection rules.

The original source SQLite file remains unchanged.
Replacing analytical tables does not guarantee secure erasure
from old storage pages or backups.

## Storage and access
Raw data, staging data, quarantine and warehouse files are local.
Their access is controlled through Windows filesystem permissions.
DuckDB schemas do not provide separate user-access enforcement here.
Filesystem permissions must be verified before sharing the project.

Only aggregate, identifier-free gold outputs are intended for
the operations view and presentation.

## Git and presentation
Do not commit source data, warehouse files, lakehouse files,
secrets or row-level personal-data exports.
Do not show personal records in slides or screenshots.
The current git check found no tracked files under
labs/data/raw, labs/artifacts or labs/warehouse.
This check does not establish that repository history is clean.

## Retention
Keep local source and analytical files only for the project purpose.
Remove local project data and backups when no longer needed.
Source corrections and erasure requests require review of all
affected copies; changing a mart alone is insufficient.

## Verified folder permissions
The raw and capstone artifact folders grant full control to:
- AzureAD\MashaelAlotaibi
- NT AUTHORITY\SYSTEM
- BUILTIN\Administrators

No Everyone or Users entries appeared in the folder-level checks.
Child-file permissions were not individually inspected.
