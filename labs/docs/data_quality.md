# Data quality policy

## Payments

Grain: one row per payment attempt.

- raw.payments retains the original records.
- staging.stg_payments includes only finite amounts greater than zero.
- quarantine.qrn_payments stores rejected records with rejection_reason.
- Failed payment attempts remain valid records when their amounts are valid.

Verified result:
- Raw: 41,785 records.
- Valid staging: 41,769 records.
- Quarantine: 16 records, all non_positive_amount.

## Failure policy

- Invalid amounts in raw produce a warning and are quarantined.
- Invalid amounts reaching staging fail the dbt build.
- Missing or extra payment IDs after routing fail the dbt build.
- A failed build prevents publication of the dependent gold asset.
- A successful run may still contain source-quality warnings.

## Quarantine ownership

Project review owner: Mashael Alotaibi.

After each run, the owner reviews quarantine counts and rejection reasons.
Corrections must come from the source; rejected amounts are not guessed
or changed automatically. Corrected records are loaded again and checked.

The quarantine table reflects current rejected records.
It is rebuilt on each run and does not retain historical review decisions.
The pending_review field is a label, not a persistent ticket workflow.

## Open issues

- Review the existing quarantine for invalid order-item quantities.
- Decide how to handle invalid supplier costs.
- Investigate order-total/payment mismatches; do not automatically
  change financial values to make the test pass.
## Verified supplier-price routing
- Total staged prices: 640.
- Valid prices: 636.
- Quarantined prices: 4.
- Current downstream models do not calculate costs from this feed.
- Future cost calculations must use stg_supplier_prices_valid.

## Verified order-item quarantine
- Quarantined order lines: 142.
- fct_order_items filters for quantity > 0.
- assert_quarantined_items_not_in_fact passed.

## Remaining reconciliation issue
- The latest order-total/payment test reported 244 mismatches.
- These remain warnings pending investigation.
- No financial amounts were changed to force a match.
