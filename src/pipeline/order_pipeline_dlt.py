# Databricks notebook source
# MAGIC %md
# MAGIC # order-lakehouse pipeline
# MAGIC A single Lakeflow Declarative Pipeline implementing the full
# MAGIC bronze -> silver -> gold design for the order/order-item data model:
# MAGIC customers place orders, each order has one or more line items, and an
# MAGIC order is `Completed` once every item on it is `Delivered`.
# MAGIC
# MAGIC **Order events are streamed.** Auto Loader ingests them into bronze,
# MAGIC and `dlt.apply_changes` (AUTO CDC) merges them into silver with
# MAGIC `stored_as_scd_type=1` -- current fulfillment state only, no history
# MAGIC needed for an item's status.
# MAGIC
# MAGIC **Dimensions are CDC-driven.** Customer and product data comes from
# MAGIC its own CDC event stream (standing in for a real CDC connector in
# MAGIC front of a customer-profile service and a product catalog), merged
# MAGIC with `dlt.apply_changes` using `stored_as_scd_type=2` -- full version
# MAGIC history, because dimension attributes (a customer's region, a
# MAGIC product's price) are exactly the kind of thing point-in-time reporting
# MAGIC needs to reflect *as of a point in time*, not just as they stand today.
# MAGIC Same AUTO CDC primitive as the order-item merge, different
# MAGIC history-keeping mode, chosen for what's actually being modeled.
# MAGIC
# MAGIC **Data quality** is declared inline as expectations, at three
# MAGIC severities: `expect_or_drop` (drop bad rows, keep going), `expect`
# MAGIC (log/track but don't drop or fail), `expect_or_fail` (stop the
# MAGIC pipeline update). Each is used below where its severity fits the
# MAGIC failure mode.
# MAGIC
# MAGIC **Gold is a star schema.** Silver stays snowflake-shaped (normalized,
# MAGIC multi-hop joins) because its job is conformance and correctness. Gold
# MAGIC is denormalized on purpose -- one fact table, flat dimensions, a
# MAGIC single join hop to each -- because it's what analysts and BI tools
# MAGIC query directly, and query simplicity matters more than storage
# MAGIC normalization at that layer. See `docs/er-diagram.md` for the full
# MAGIC conceptual / logical / physical model and the star-vs-snowflake
# MAGIC reasoning.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

volume_path = spark.conf.get("volume_path")
order_events_path = f"{volume_path}/order_events"
customer_changes_path = f"{volume_path}/customer_changes"
product_changes_path = f"{volume_path}/product_changes"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze: order events
# MAGIC Auto Loader, append-only, no transformation -- bronze is the
# MAGIC replayable audit trail. Reads specifically from `order_events/`, not
# MAGIC the volume root, since that root also holds `customer_changes/` and
# MAGIC `product_changes/` and a recursive read would mix all three shapes
# MAGIC into one stream.

# COMMAND ----------
@dlt.table(
    name="bronze_order_events",
    comment="Raw order/item events landed by the synthetic OMS generator, ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_order_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(order_events_path)
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver: order items (AUTO CDC, SCD Type 1)
# MAGIC Expectations run on an intermediate view, before `apply_changes` --
# MAGIC `apply_changes` itself doesn't take expectations, so bad rows need
# MAGIC handling upstream of it.
# MAGIC
# MAGIC Severity choices:
# MAGIC - `valid_order_item_id` / `valid_product_id` -- **drop**. A row with no
# MAGIC   key can't be upserted meaningfully.
# MAGIC - `valid_quantity` -- **drop**. A non-positive quantity is corrupt
# MAGIC   data, not a business event.
# MAGIC - `valid_item_status` -- **drop**. An unrecognized status is either
# MAGIC   schema drift or a typo upstream -- worth losing the row over rather
# MAGIC   than letting an unknown status leak into the `Completed` rollup.
# MAGIC - `has_event_time` -- **warn only**. A missing event time is a
# MAGIC   completeness gap worth tracking, but the row is still usable --
# MAGIC   `apply_changes` just can't sequence it precisely.

# COMMAND ----------
@dlt.view(name="bronze_order_events_valid")
@dlt.expect_or_drop("valid_order_item_id", "order_item_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop(
    "valid_item_status",
    "item_status IN ('Placed', 'Packed', 'Shipped', 'Delivered', 'Returned')",
)
@dlt.expect("has_event_time", "event_time IS NOT NULL")
def bronze_order_events_valid():
    return (
        dlt.read_stream("bronze_order_events")
        .withColumn("order_date", F.to_timestamp("order_date"))
        .withColumn("event_time", F.to_timestamp("event_time"))
    )

# COMMAND ----------
dlt.create_streaming_table(
    name="silver_order_items",
    comment="Conformed, deduped order items -- latest status per order_item_id.",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="silver_order_items",
    source="bronze_order_events_valid",
    keys=["order_item_id"],
    sequence_by="event_time",
    stored_as_scd_type=1,  # current state only -- no history needed for this use case
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver: order-level status rollup
# MAGIC `order_status` is derived, never set directly -- `Completed` only
# MAGIC when every item on the order is `Delivered`. `rollup_consistency` is a
# MAGIC hard invariant (delivered count can never exceed item count; if it
# MAGIC does, the aggregation itself is broken), so it's `expect_or_fail` --
# MAGIC worth stopping the pipeline over, unlike the row-level checks above.

# COMMAND ----------
@dlt.table(
    name="silver_orders",
    comment="Order-level status rollup derived from silver_order_items.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("rollup_consistency", "delivered_item_count <= item_count")
@dlt.expect(
    "known_order_status",
    "order_status IN ('Completed', 'Partially Returned', 'In Progress')",
)
def silver_orders():
    items = dlt.read("silver_order_items")
    agg = items.groupBy("order_id", "customer_id").agg(
        F.min("order_date").alias("order_date"),
        F.count("*").alias("item_count"),
        F.sum(F.when(F.col("item_status") == "Delivered", 1).otherwise(0)).alias(
            "delivered_item_count"
        ),
        F.sum(F.when(F.col("item_status") == "Returned", 1).otherwise(0)).alias(
            "returned_item_count"
        ),
        F.max("event_time").alias("updated_at"),
    )
    return agg.withColumn(
        "order_status",
        F.when(F.col("delivered_item_count") == F.col("item_count"), F.lit("Completed"))
        .when(F.col("returned_item_count") > 0, F.lit("Partially Returned"))
        .otherwise(F.lit("In Progress")),
    ).drop("returned_item_count")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimensions: CDC-driven, SCD Type 2
# MAGIC Same `dlt.apply_changes` primitive as `silver_order_items` above, but
# MAGIC `stored_as_scd_type=2` instead of `1` -- full history, since dimension
# MAGIC attributes are state that reporting needs to see as of a point in
# MAGIC time, not just as they stand today.
# MAGIC
# MAGIC `apply_changes` with `stored_as_scd_type=2` automatically adds
# MAGIC `__START_AT` / `__END_AT` columns: each row is one *version* of a
# MAGIC business key, `__END_AT IS NULL` means "this is the current version,"
# MAGIC and a `DELETE` event closes out the current version without inserting
# MAGIC a replacement -- there's no "current" row for a deleted entity
# MAGIC anymore, but its history stays queryable.
# MAGIC
# MAGIC How each CDC operation is handled:
# MAGIC - **`APPEND`** (initial load) / **`UPDATE`** -- both are just upserts
# MAGIC   to `apply_changes`; no distinct code path needed for "insert" vs.
# MAGIC   "update."
# MAGIC - **`DELETE`** -- matched via `apply_as_deletes="operation = 'DELETE'"`
# MAGIC   (customer dimension only, in this project).
# MAGIC - **Out-of-order events** -- `sequence_by="change_time"` inserts a
# MAGIC   late-arriving event into the version history at the correct point,
# MAGIC   rather than always treating it as "the latest."

# COMMAND ----------
@dlt.table(
    name="bronze_customer_changes",
    comment="Raw customer CDC events (APPEND/UPDATE/DELETE), ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_customer_changes():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(customer_changes_path)
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------
@dlt.table(
    name="bronze_product_changes",
    comment="Raw product CDC events (APPEND/UPDATE), ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_product_changes():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(product_changes_path)
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------
@dlt.view(name="customer_changes_valid")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "operation IN ('APPEND', 'UPDATE', 'DELETE')")
@dlt.expect("has_change_time", "change_time IS NOT NULL")
def customer_changes_valid():
    return dlt.read_stream("bronze_customer_changes").withColumn(
        "change_time", F.to_timestamp("change_time")
    )

# COMMAND ----------
@dlt.view(name="product_changes_valid")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_list_price", "list_price > 0")
@dlt.expect_or_drop("valid_operation", "operation IN ('APPEND', 'UPDATE')")
def product_changes_valid():
    return dlt.read_stream("bronze_product_changes").withColumn(
        "change_time", F.to_timestamp("change_time")
    )

# COMMAND ----------
dlt.create_streaming_table(
    name="silver_dim_customer_scd2",
    comment="Full customer change history -- one row per version, __END_AT IS NULL for the current one.",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="silver_dim_customer_scd2",
    source="customer_changes_valid",
    keys=["customer_id"],
    sequence_by="change_time",
    apply_as_deletes="operation = 'DELETE'",
    except_column_list=["operation"],
    stored_as_scd_type=2,
)

# COMMAND ----------
dlt.create_streaming_table(
    name="silver_dim_product_scd2",
    comment="Full product change history -- one row per version, __END_AT IS NULL for the current one.",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="silver_dim_product_scd2",
    source="product_changes_valid",
    keys=["product_id"],
    sequence_by="change_time",
    except_column_list=["operation"],
    stored_as_scd_type=2,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold: star schema
# MAGIC `gold_dim_customer` / `gold_dim_product` are the current-state
# MAGIC projection of the SCD2 history tables above (`__END_AT IS NULL`),
# MAGIC which is what a star-schema join normally wants -- "who is this
# MAGIC customer *today*." The full history stays queryable directly on
# MAGIC `silver_dim_customer_scd2` / `silver_dim_product_scd2` for anyone who
# MAGIC needs point-in-time correctness instead (e.g. joining a fact row to
# MAGIC the dimension version that was current `AS OF` that row's
# MAGIC `order_date`, via `__START_AT`/`__END_AT`, instead of to today's
# MAGIC version). That trade-off is deliberate here for simplicity, not an
# MAGIC oversight.
# MAGIC
# MAGIC Surrogate keys are generated with `xxhash64(business_key)`, not
# MAGIC `monotonically_increasing_id()` -- the latter isn't stable across
# MAGIC pipeline reruns and would silently break every fact-to-dimension join
# MAGIC on the next full refresh. A deterministic hash of the business key
# MAGIC produces the same surrogate key every time, across every version of
# MAGIC that customer or product.

# COMMAND ----------
@dlt.table(
    name="gold_dim_date",
    comment="Conformed calendar dimension, generated as a date spine (2020-01-01 through +10 years).",
    table_properties={"quality": "gold"},
)
def gold_dim_date():
    return (
        spark.range(0, 3653)
        .select(F.expr("date_add(to_date('2020-01-01'), CAST(id AS INT))").alias("calendar_date"))
        .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("calendar_date"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("day_of_week", F.date_format("calendar_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("calendar_date").isin(1, 7))
    )

# COMMAND ----------
@dlt.table(
    name="gold_dim_customer",
    comment="Current-state customer dimension (SCD2 history collapsed to __END_AT IS NULL) with a stable surrogate key.",
    table_properties={"quality": "gold"},
)
def gold_dim_customer():
    return (
        dlt.read("silver_dim_customer_scd2")
        .filter("__END_AT IS NULL")
        .withColumn("customer_key", F.xxhash64("customer_id"))
        .select("customer_key", "customer_id", "name", "email", "region")
    )

# COMMAND ----------
@dlt.table(
    name="gold_dim_product",
    comment="Current-state product dimension (SCD2 history collapsed to __END_AT IS NULL) with a stable surrogate key.",
    table_properties={"quality": "gold"},
)
def gold_dim_product():
    return (
        dlt.read("silver_dim_product_scd2")
        .filter("__END_AT IS NULL")
        .withColumn("product_key", F.xxhash64("product_id"))
        .select("product_key", "product_id", "product_name", "category", "list_price")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### Fact table
# MAGIC Grain is one row per order item. `order_id` / `order_item_id` stay as
# MAGIC **degenerate dimensions** -- plain attributes on the fact row, not
# MAGIC separate one-column dimension tables, since an identifier with no
# MAGIC other attributes doesn't earn its own table in Kimball-style design.
# MAGIC `order_status` is copied onto each item row from the `silver_orders`
# MAGIC rollup for query convenience -- still derived, never set directly,
# MAGIC just denormalized down to item grain the way gold denormalizes
# MAGIC everything else.

# COMMAND ----------
@dlt.table(
    name="gold_fact_order_items",
    comment="Star-schema fact table at order-item grain. One join hop to each dimension by surrogate key.",
    table_properties={"quality": "gold"},
)
def gold_fact_order_items():
    items = dlt.read("silver_order_items")
    orders = dlt.read("silver_orders").select("order_id", "order_status")
    customers = dlt.read("gold_dim_customer").select("customer_id", "customer_key")
    products = dlt.read("gold_dim_product").select("product_id", "product_key", "list_price")

    return (
        items.join(orders, "order_id", "left")
        .join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
        .withColumn("order_date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .select(
            "order_item_id",
            "order_id",
            "customer_key",
            "product_key",
            "order_date_key",
            "quantity",
            "list_price",
            (F.col("quantity") * F.col("list_price")).alias("line_amount"),
            "item_status",
            "order_status",
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### Monthly product sales
# MAGIC The interview-question query, built as a table: two single-hop joins
# MAGIC (fact -> `gold_dim_date`, fact -> `gold_dim_product`) instead of the
# MAGIC multi-hop path a snowflake layout would need. This is the concrete
# MAGIC payoff of denormalizing gold into a star -- same answer, fewer and
# MAGIC cheaper joins.

# COMMAND ----------
@dlt.table(
    name="gold_monthly_product_sales",
    comment="Units and revenue sold per product per month, Delivered items only.",
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("non_negative_units", "total_units_sold >= 0")
def gold_monthly_product_sales():
    fact = dlt.read("gold_fact_order_items").filter("item_status = 'Delivered'")
    dates = dlt.read("gold_dim_date").select("date_key", "year", "month", "month_name")
    products = dlt.read("gold_dim_product").select("product_key", "product_id", "product_name", "category")

    return (
        fact.join(dates, fact.order_date_key == dates.date_key)
        .join(products, "product_key")
        .groupBy("year", "month", "month_name", "product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.sum("line_amount").alias("total_revenue"),
        )
    )
