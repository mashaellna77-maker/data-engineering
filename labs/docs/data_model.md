# Data model and grain

## Layers
- Bronze/raw: loaded source data, with customer identifier protection.
- Silver/staging: typed and normalized records.
- Intermediate: reusable calculations.
- Gold/marts: facts, dimensions and analytical aggregates.
- Quarantine: rejected records retained for review.

## Fact tables

| Table | Grain | Unique key |
|---|---|---|
| marts.fct_orders | One row per order, including different order statuses. | order_id |
| marts.fct_order_items | One row per order line with positive quantity. | order_line_id; derived from order_id and line_no |

## Dimensions
- dim_customers: current customer accounts, including an unknown member.
- dim_products: product reference information.
- dim_stores: store reference information.
- dim_date: calendar dates.

## Operations mart
mart_daily_city_sales has one row per Riyadh calendar day and city.
Its key is (order_date, city).

Measures:
- orders: all orders in that day and city.
- delivered_orders: orders whose status is delivered.
- revenue_sar: sum of items_amount for delivered orders.
- avg_basket_sar: average items_amount for delivered orders.
- sla_rate: share of non-null within_sla values that are true.

Revenue and average basket exclude delivery fees.
sla_rate excludes orders with unknown delivery timing.
A missing revenue or SLA value must not be interpreted as zero.

## Modeling decisions
- Aggregate order lines before adding the delivery fee, so it is counted once.
- Use an unknown customer member to preserve orders after customer erasure.
- Exclude non-positive quantities from the order-item fact.
- Use Riyadh calendar dates for daily reporting.
- Publish daily city aggregates to Iceberg as gold.daily_city_sales.
