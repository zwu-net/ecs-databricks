-- Most sold product in the last (full, calendar) month.
--
-- Definitions used:
--   "last month"  = the previous full calendar month, not a rolling 30 days.
--   "sold"        = item_status = 'Delivered' -- placed-but-not-yet-fulfilled
--                    or returned items don't count as sold.
--   "most sold"   = ranked by total units (SUM(quantity)), not revenue or
--                    order count. See the revenue variant below if the
--                    business actually means revenue.
--
-- Runs against the star-schema gold layer: every join below is a single
-- hop by surrogate key (fact -> gold_dim_date, fact -> gold_dim_product) --
-- the concrete payoff of denormalizing gold, versus the multi-hop joins a
-- snowflake layout (like silver, in this project) would need for the same
-- question.

WITH last_month AS (
  SELECT
    year(add_months(current_date(), -1))  AS y,
    month(add_months(current_date(), -1)) AS m
)
SELECT
  p.product_id,
  p.product_name,
  p.category,
  SUM(f.quantity) AS total_units_sold
FROM {catalog}.{schema}.gold_fact_order_items f
JOIN {catalog}.{schema}.gold_dim_date d ON f.order_date_key = d.date_key
JOIN {catalog}.{schema}.gold_dim_product p ON f.product_key = p.product_key
JOIN last_month lm ON d.year = lm.y AND d.month = lm.m
WHERE f.item_status = 'Delivered'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_units_sold DESC
LIMIT 1;

-- ---------------------------------------------------------------------
-- Top-5 variant (useful if asked "not just the winner"):
-- ---------------------------------------------------------------------
-- ... same query, just LIMIT 1 -> LIMIT 5.

-- ---------------------------------------------------------------------
-- Revenue-based variant (if "most sold" is challenged to mean "top revenue"):
-- ---------------------------------------------------------------------
-- SELECT
--   p.product_id,
--   p.product_name,
--   SUM(f.line_amount) AS total_revenue
-- FROM {catalog}.{schema}.gold_fact_order_items f
-- JOIN {catalog}.{schema}.gold_dim_date d ON f.order_date_key = d.date_key
-- JOIN {catalog}.{schema}.gold_dim_product p ON f.product_key = p.product_key
-- JOIN last_month lm ON d.year = lm.y AND d.month = lm.m
-- WHERE f.item_status = 'Delivered'
-- GROUP BY p.product_id, p.product_name
-- ORDER BY total_revenue DESC
-- LIMIT 1;

-- ---------------------------------------------------------------------
-- Already materialized: gold_monthly_product_sales does this aggregation
-- as a table refreshed by the pipeline itself, so an ad hoc query can
-- often just be:
-- ---------------------------------------------------------------------
-- SELECT * FROM {catalog}.{schema}.gold_monthly_product_sales
-- ORDER BY year DESC, month DESC, total_units_sold DESC
-- LIMIT 1;
