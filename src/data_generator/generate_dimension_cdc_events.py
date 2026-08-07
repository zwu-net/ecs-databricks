# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic dimension CDC event generator
# MAGIC Stands in for a real CDC connector (Debezium, a native DB CDC feed,
# MAGIC etc.) sitting in front of the customer-profile and product-catalog
# MAGIC source systems. Each run emits `APPEND` / `UPDATE` / `DELETE` change
# MAGIC events as JSON files into a UC Volume -- the same shape a real CDC
# MAGIC connector produces. `src/pipeline/order_pipeline_dlt.py` picks these up
# MAGIC with Auto Loader and merges them with `dlt.apply_changes` (SCD Type 2),
# MAGIC vs. SCD Type 1 for order items -- same primitive, different
# MAGIC history-keeping mode, because dimension state changes are worth
# MAGIC keeping history on and order-item status changes (for this use case)
# MAGIC aren't.
# MAGIC
# MAGIC The first run emits a full initial load (`APPEND` for every row).
# MAGIC Every run after that randomly mutates a handful of existing rows
# MAGIC (`UPDATE`) and occasionally retires one (`DELETE`), so re-running this
# MAGIC a few times is what demonstrates SCD2 actually accumulating history.

# COMMAND ----------
import json
import random
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("volume_path", "/Volumes/workspace/order_lakehouse_dev/raw_events")
volume_path = dbutils.widgets.get("volume_path")

customer_changes_path = f"{volume_path}/customer_changes"
product_changes_path = f"{volume_path}/product_changes"

# COMMAND ----------
CUSTOMERS = {
    "C001": {"name": "Ada Lovelace", "email": "ada@example.com", "region": "EMEA"},
    "C002": {"name": "Grace Hopper", "email": "grace@example.com", "region": "AMER"},
    "C003": {"name": "Alan Turing", "email": "alan@example.com", "region": "EMEA"},
    "C004": {"name": "Katherine Johnson", "email": "katherine@example.com", "region": "AMER"},
    "C005": {"name": "Hedy Lamarr", "email": "hedy@example.com", "region": "APAC"},
}

PRODUCTS = {
    "P001": {"product_name": "Wireless Mouse", "category": "Electronics", "list_price": 24.99},
    "P002": {"product_name": "Mechanical Keyboard", "category": "Electronics", "list_price": 89.00},
    "P003": {"product_name": "USB-C Hub", "category": "Electronics", "list_price": 34.50},
    "P004": {"product_name": "Standing Desk Mat", "category": "Home Office", "list_price": 45.00},
    "P005": {"product_name": "Noise Cancelling Headphones", "category": "Electronics", "list_price": 199.99},
    "P006": {"product_name": "Desk Lamp", "category": "Home Office", "list_price": 29.00},
    "P007": {"product_name": "Laptop Stand", "category": "Home Office", "list_price": 39.99},
    "P008": {"product_name": "Webcam 1080p", "category": "Electronics", "list_price": 54.00},
}

REGIONS = ["EMEA", "AMER", "APAC"]


def is_first_run() -> bool:
    try:
        return len(dbutils.fs.ls(customer_changes_path)) == 0
    except Exception:
        return True  # path doesn't exist yet -> definitely the first run


def write_events(path: str, events: list, label: str):
    if not events:
        print(f"No {label} events this run.")
        return
    batch_id = uuid.uuid4().hex[:8]
    target_path = f"{path}/{label}_{batch_id}.json"
    rows = "\n".join(json.dumps(e) for e in events)
    dbutils.fs.put(target_path, rows, overwrite=True)
    print(f"Wrote {len(events)} {label} events to {target_path}")


now = datetime.now(timezone.utc).isoformat()
first_run = is_first_run()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Customer changes

# COMMAND ----------
customer_events = []

if first_run:
    for customer_id, attrs in CUSTOMERS.items():
        customer_events.append({
            "customer_id": customer_id,
            "name": attrs["name"],
            "email": attrs["email"],
            "region": attrs["region"],
            "operation": "APPEND",
            "change_time": now,
        })
else:
    # Mutate a random subset -- e.g. a customer moves region, or updates email.
    for customer_id in random.sample(list(CUSTOMERS), k=random.randint(0, 2)):
        attrs = CUSTOMERS[customer_id]
        new_region = random.choice([r for r in REGIONS if r != attrs["region"]])
        customer_events.append({
            "customer_id": customer_id,
            "name": attrs["name"],
            "email": attrs["email"],
            "region": new_region,
            "operation": "UPDATE",
            "change_time": now,
        })
    # Occasionally retire a customer, to demonstrate DELETE handling.
    if random.random() < 0.15:
        customer_id = random.choice(list(CUSTOMERS))
        customer_events.append({
            "customer_id": customer_id,
            "name": CUSTOMERS[customer_id]["name"],
            "email": CUSTOMERS[customer_id]["email"],
            "region": CUSTOMERS[customer_id]["region"],
            "operation": "DELETE",
            "change_time": now,
        })

write_events(customer_changes_path, customer_events, "customer_changes")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Product changes

# COMMAND ----------
product_events = []

if first_run:
    for product_id, attrs in PRODUCTS.items():
        product_events.append({
            "product_id": product_id,
            "product_name": attrs["product_name"],
            "category": attrs["category"],
            "list_price": attrs["list_price"],
            "operation": "APPEND",
            "change_time": now,
        })
else:
    # Mutate a random subset -- e.g. a price change or a recategorization.
    for product_id in random.sample(list(PRODUCTS), k=random.randint(0, 2)):
        attrs = PRODUCTS[product_id]
        new_price = round(attrs["list_price"] * random.uniform(0.85, 1.15), 2)
        product_events.append({
            "product_id": product_id,
            "product_name": attrs["product_name"],
            "category": attrs["category"],
            "list_price": new_price,
            "operation": "UPDATE",
            "change_time": now,
        })

write_events(product_changes_path, product_events, "product_changes")
