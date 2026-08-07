# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Creates the catalog/schema/volume the pipeline needs. Safe to re-run --
# MAGIC everything is `IF NOT EXISTS`. Run this once before anything else.
# MAGIC
# MAGIC There's no dimension seed data created here -- `dim_customer` and
# MAGIC `dim_product` don't exist as static tables in this project.
# MAGIC Dimensions are built entirely from CDC event streams by the pipeline
# MAGIC itself (see `src/pipeline/order_pipeline_dlt.py`); this notebook's job
# MAGIC is just to make sure the catalog/schema/volume they land in exists.
# MAGIC
# MAGIC Free Edition note: every account gets exactly one metastore and (by
# MAGIC default) a catalog named `workspace`. If your catalog variable is
# MAGIC `workspace`, this notebook does not try to `CREATE CATALOG` (Free
# MAGIC Edition accounts can't create additional catalogs) -- it just creates
# MAGIC the schema/volume inside it.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "order_lakehouse_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"catalog={catalog}, schema={schema}")

# COMMAND ----------
if catalog != "workspace":
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_events
    COMMENT 'Landing zone for synthetic order events and dimension CDC events, read by Auto Loader'
""")

print("Setup complete.")
