# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic order-event generator
# MAGIC Free Edition doesn't give you an easy managed Kafka/Event Hubs to point
# MAGIC at, so this notebook stands in for the upstream OMS: each run appends a
# MAGIC batch of order/item-status-change events as JSON files into a Unity
# MAGIC Catalog Volume, under `order_events/`. The pipeline's Auto Loader
# MAGIC bronze table picks these up exactly the way it would pick up files
# MAGIC landed by a real CDC connector or event exporter -- swap this notebook
# MAGIC out for a real source and nothing downstream needs to change.
# MAGIC
# MAGIC Written to its own subfolder (not the volume root) because the volume
# MAGIC also holds `customer_changes/` and `product_changes/` (see
# MAGIC `generate_dimension_cdc_events.py`) -- a recursive Auto Loader read of
# MAGIC the whole volume would otherwise mix all three event shapes together.

# COMMAND ----------
import json
import random
import uuid
from datetime import datetime, timedelta, timezone

dbutils.widgets.text("volume_path", "/Volumes/workspace/order_lakehouse_dev/raw_events")
dbutils.widgets.text("num_events", "200")

volume_path = dbutils.widgets.get("volume_path")
num_events = int(dbutils.widgets.get("num_events"))

# COMMAND ----------
CUSTOMERS = ["C001", "C002", "C003", "C004", "C005"]
PRODUCTS = ["P001", "P002", "P003", "P004", "P005", "P006", "P007", "P008"]
ITEM_STATUSES = ["Placed", "Packed", "Shipped", "Delivered", "Returned"]

def random_timestamp_within_last_days(days: int) -> str:
    now = datetime.now(timezone.utc)
    delta = timedelta(
        days=random.randint(0, days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return (now - delta).isoformat()

events = []
for _ in range(num_events):
    order_id = f"O{uuid.uuid4().hex[:10]}"
    order_date = random_timestamp_within_last_days(60)
    customer_id = random.choice(CUSTOMERS)
    n_items = random.randint(1, 4)
    chosen_products = random.sample(PRODUCTS, k=n_items)

    for product_id in chosen_products:
        events.append({
            "order_id": order_id,
            "order_item_id": f"{order_id}-{product_id}",
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": random.randint(1, 5),
            "order_date": order_date,
            # Skewed toward Delivered so "most sold" queries have real signal.
            "item_status": random.choices(ITEM_STATUSES, weights=[10, 10, 15, 55, 10])[0],
            "event_time": datetime.now(timezone.utc).isoformat(),
        })

# COMMAND ----------
batch_id = uuid.uuid4().hex[:8]
target_path = f"{volume_path}/order_events/order_events_{batch_id}.json"

rows = "\n".join(json.dumps(e) for e in events)
dbutils.fs.put(target_path, rows, overwrite=True)

print(f"Wrote {len(events)} events to {target_path}")
