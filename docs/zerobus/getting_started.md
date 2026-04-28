# Getting Started with Zerobus Ingest

This guide covers how to provision a Unity Catalog Delta table from a Protobuf descriptor and ingest data into it using the `zerobus-ingest` helpers.

---

## Prerequisites

- A Databricks workspace with Unity Catalog enabled
- A SQL warehouse (any size; Serverless works)
- The `zerobus-ingest` package installed and configured (see [`zerobus-ingest/README.md`](../../zerobus-ingest/README.md))
- A compiled Protobuf `descriptor.bin` for your message type (produced by `buf build`)

---

## Important: External Storage Location Requirement

> **Zerobus tables must be backed by an external storage location — not Databricks-managed (serverless) storage.**

When Zerobus ingests records it writes directly to the underlying object store. Databricks-managed storage (the default for catalog-managed tables in newer Unity Catalog configurations) uses a storage path that is opaque to external writers and is not directly accessible outside of UC-enabled compute. Zerobus requires a path it can write to independently.

This means:

- The catalog or schema that hosts your Zerobus tables **must have an explicit `storage_root`** pointing to an external location (e.g. `s3://your-bucket/path`).
- Do **not** use the default serverless storage option when creating the catalog or schema that will hold Zerobus tables.
- The Databricks workspace must have the appropriate external location configured (with `CREATE EXTERNAL LOCATION` and S3/GCS/ADLS credentials) and the catalog/schema `storage_root` must point into it.

See the [Zerobus limitations section in the Databricks documentation](https://docs.databricks.com/aws/en/ingestion/zerobus-limits) for the authoritative description of this constraint.

---

## 1. Inspect Catalog and Schema Storage

Before creating a table, verify that the target catalog and schema are backed by an external location.

```python
from databricks.sdk import WorkspaceClient
from zerobus_ingest.utils import TableUtils

w = WorkspaceClient()  # uses DATABRICKS_HOST / DATABRICKS_TOKEN from env

catalog_info = TableUtils.get_catalog_info(w, "demos")
print("catalog storage_root:", catalog_info.storage_root)
# e.g. s3://demos-dbx/

schema_info = TableUtils.get_schema_info(w, "demos", "zerobus")
print("schema storage_root:    ", schema_info.storage_root)
print("schema storage_location:", schema_info.storage_location)
# e.g. s3://demos-dbx/zerobus/prod
#      s3://demos-dbx/zerobus/prod/__unitystorage/schemas/<schema-id>
```

If `storage_root` is empty or points to a `dbfs:/` path, the catalog or schema is using Databricks-managed storage and Zerobus will not be able to write to it.

---

## 2. Derive a Table Schema from a Protobuf Descriptor

`TableUtils.descriptor_to_columns` converts a compiled Protobuf `Descriptor` into a list of `ColumnInfo` objects that Unity Catalog understands. Nested messages become `STRUCT` columns; repeated fields become `ARRAY` columns.

```python
from google.protobuf import descriptor_pool, descriptor_pb2
from zerobus_ingest.utils import TableUtils

def load_descriptor(bin_path: str, message_full_name: str):
    """Load a FileDescriptorSet .bin and return the named message Descriptor."""
    with open(bin_path, "rb") as f:
        fds = descriptor_pb2.FileDescriptorSet.FromString(f.read())
    pool = descriptor_pool.DescriptorPool()
    for fd in fds.file:
        pool.Add(fd)
    return pool.FindMessageTypeByName(message_full_name)

descriptor = load_descriptor(
    "gen/python/orders/v1/descriptor.bin",
    "orders.v1.Order",
)

columns = TableUtils.descriptor_to_columns(descriptor)

# Preview the schema
print(TableUtils.pretty_print_columns(columns))
```

Example output:

```
  order_id          STRING
  customer_id       STRING
  status            STRING
  line_items        ARRAY<STRUCT<product_id:STRING, sku:STRING, ...>>
  subtotal          STRUCT<currency_code:STRING, units:LONG, nanos:INT>
  ...
  created_at        LONG
  updated_at        LONG
```

---

## 3. Create a Managed Table

> **Why not `workspace_client.tables.create()`?**
>
> The Unity Catalog REST API's `tables.create` endpoint only supports **external** tables when called from outside a UC-enabled Databricks cluster. To create a managed table from a local machine or CI environment, `TableUtils.create_managed_table` generates a `CREATE TABLE … USING DELTA` DDL statement and executes it through the SQL warehouse API.

```python
from databricks.sdk import WorkspaceClient
from zerobus_ingest.utils import TableUtils

w = WorkspaceClient()

table_info = TableUtils.create_managed_table(
    w,
    catalog="demos",
    schema="zerobus",
    table="orders",
    columns=columns,           # from descriptor_to_columns above
    # warehouse_id=None        # auto-selects first available warehouse
    # if_not_exists=True       # default — safe to run repeatedly
)

print("Created:", table_info.full_name)
print("Storage:", table_info.storage_location)
# demos.zerobus.orders
# s3://demos-dbx/zerobus/prod/__unitystorage/schemas/<id>/tables/<table-id>
```

The `if_not_exists=True` default makes this idempotent — safe to run in setup scripts or CI without checking for prior existence first.

To recreate the table (drop-and-replace):

```python
table_info = TableUtils.create_managed_table(
    w,
    catalog="demos",
    schema="zerobus",
    table="orders",
    columns=columns,
    or_replace=True,
)
```

---

## 4. Create an External Table

For cases where you need to manage the storage path explicitly (e.g. the table is shared with non-Databricks consumers), use `TableUtils.create_table` to register an external Delta table:

```python
table_info = TableUtils.create_table(
    w,
    catalog="demos",
    schema="zerobus",
    table="orders_external",
    storage_location="s3://demos-dbx/zerobus/prod/orders_external",
    columns=columns,
)
```

The caller must have `EXTERNAL_USE_SCHEMA` on the schema and `EXTERNAL_USE_LOCATION` on the external location.

---

## 5. Ingest Records

Once the table exists, use `AsyncZerobusWriter` to stream records into it:

```python
import asyncio
from zerobus_ingest.utils import AsyncZerobusWriter
from zerobus_ingest.config import Config

config = Config.zerobus()   # reads ZEROBUS_* env vars

async def ingest(orders):
    async with AsyncZerobusWriter.from_config(config) as writer:
        for order in orders:
            await writer.write_offset(order)
        await writer.flush()
    print(f"Ingested {len(orders)} records.")

asyncio.run(ingest(orders))
```

See the [AsyncZerobusWriter section in the README](../../zerobus-ingest/README.md#asynczerobuswriter) for full configuration options, callback customization, and performance notes.

---

## End-to-End Example

```python
import asyncio
from databricks.sdk import WorkspaceClient
from zerobus_ingest.config import Config
from zerobus_ingest.datagen.orders import Orders
from zerobus_ingest.main import _load_descriptor_from_binary
from zerobus_ingest.utils import AsyncZerobusWriter, TableUtils

CATALOG = "demos"
SCHEMA  = "zerobus"
TABLE   = "orders"

w = WorkspaceClient()

# 1. Derive schema from compiled descriptor
descriptor = _load_descriptor_from_binary(
    "gen/python/orders/v1/descriptor.bin",
    "orders.v1.Order",
)
columns = TableUtils.descriptor_to_columns(descriptor)

# 2. Create managed table (idempotent)
table_info = TableUtils.create_managed_table(
    w, CATALOG, SCHEMA, TABLE, columns
)
print("Table ready:", table_info.full_name)

# 3. Generate and ingest orders
orders = Orders.generate_orders(1_000, seed=42)

async def run():
    async with AsyncZerobusWriter.from_config(Config.zerobus()) as writer:
        for order in orders:
            await writer.write_offset(order)
        await writer.flush()
    print(f"Ingested {len(orders)} orders into {table_info.full_name}")

asyncio.run(run())
```

---

## API Reference

### `TableUtils.get_catalog_info(workspace_client, catalog)`

Returns `CatalogInfo` (includes `storage_root`, `owner`, `metastore_id`).

### `TableUtils.get_schema_info(workspace_client, catalog, schema)`

Returns `SchemaInfo` (includes `storage_root`, `storage_location`, `schema_id`).

### `TableUtils.descriptor_to_columns(descriptor)`

Converts a Protobuf `Descriptor` to `list[ColumnInfo]`. Handles scalars, enums (→ `INT`), nested messages (→ `STRUCT`), and repeated fields (→ `ARRAY`).

### `TableUtils.pretty_print_columns(columns)`

Returns a human-readable string of column name → type pairs, suitable for logging or inspection.

### `TableUtils.create_managed_table(workspace_client, catalog, schema, table, columns, *, warehouse_id, or_replace, if_not_exists)`

Creates a managed Delta table via SQL execution. Safe from local/CI environments. Polls for statement completion and returns `TableInfo`.

### `TableUtils.create_table(workspace_client, catalog, schema, table, storage_location, *, data_source_format, columns, properties)`

Registers an external Delta table via the Unity Catalog REST API. Requires explicit `storage_location`.
