# zerobus-ingest

Ingest application using the Databricks ZeroBus ingest SDK. 
> Read more about it in the official docs: [Databricks Zerobus](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest)
> Current limitations [Zerobus Limitations](https://docs.databricks.com/aws/en/ingestion/zerobus-limits)

## Environment

Copy `.env` (or `.env-prod` for production) and fill in your Databricks workspace and auth details. Loaded automatically based on `--env dev` (default) or `--env prod`.

### Required variables in `.env`

```env
# Workspace
DATABRICKS_HOST=xxx-xxxxxxxx-xxxx.cloud.databricks.com
DATABRICKS_WORKSPACE_ID=
DATABRICKS_WORKSPACE_URL=https://xxx-xxxxxxxx-xxxx.cloud.databricks.com
DATABRICKS_REGION=us-west-2

# Auth (use one of: token, or client_id + client_secret)
DATABRICKS_TOKEN=dapixxxxxxxxxxxxxx

# Zerobus (for ZerobusWriter / --publish)
# This is your service principal in your workspace (create under settings/identity...)
ZEROBUS_CLIENT_ID=
ZEROBUS_CLIENT_SECRET=

# Unity Catalog target (for ZerobusWriter / --publish)
# this is for the table. This will be a command line arg in the future
UC_CATALOG=
UC_SCHEMA=
UC_TABLE=
```

- **DATABRICKS_HOST** — Host only (e.g. `dbc-xxxx-xxxx.cloud.databricks.com` or short form).
- **DATABRICKS_WORKSPACE_ID** — Numeric workspace ID from the workspace URL.
- **DATABRICKS_WORKSPACE_URL** — Full workspace URL (e.g. `https://dbc-xxxx-xxxx.cloud.databricks.com/`).
- **DATABRICKS_REGION** — Cloud region (e.g. `us-west-2`).
- **DATABRICKS_TOKEN** — Personal access token (if using token auth).
- **DATABRICKS_CLIENT_ID** / **DATABRICKS_CLIENT_SECRET** — OAuth client credentials (if using OAuth).
- **ZEROBUS_CLIENT_ID** / **ZEROBUS_CLIENT_SECRET** — Zerobus ingest client credentials (required for `ZerobusWriter` and `--publish`).
- **UC_CATALOG**, **UC_SCHEMA**, **UC_TABLE** — Unity Catalog catalog, schema, and table for Zerobus ingestion target.

## Installing from dist (sdist or wheel)

The package depends on **bufbuild-protovalidate-protocolbuffers-python**, which is published on Buf’s index, not PyPI. The `extra-index-url` in `pyproject.toml` is only used when working inside this repo; it is not embedded in the sdist, so you must pass the extra index when installing from a built artifact.

**Using uv (recommended):**

```bash
uv pip install --extra-index-url https://buf.build/gen/python dist/zerobus_ingest-0.1.0.tar.gz
# or from a wheel:
uv pip install --extra-index-url https://buf.build/gen/python dist/zerobus_ingest-0.1.0-py3-none-any.whl
```

**Using pip:**

```bash
pip install --extra-index-url https://buf.build/gen/python dist/zerobus_ingest-0.1.0.tar.gz
```

From the project root you can run **`make install-dist`** to install the built wheel/sdist with the correct extra index (uses the first `dist/*.whl` or `dist/*.tar.gz` found).

## Run

### Default (no generation)

Connects to the workspace and prints the workspace ID. Environment is chosen with `--env`.

```bash
uv run main.py              # dev (loads .env)
uv run main.py --env prod   # prod (loads .env-prod)
```

### Generate mode

Use `--generate` to create sample order records (Order protobufs) and print them to stdout. The number of records is controlled with `--count` (default: 100). With `--validate`, each generated order is checked with protovalidate; only valid orders count toward `--count`, so you always get the requested number of valid orders.

```bash
uv run main.py --generate                    # generate 100 orders (default count)
uv run main.py --generate --count 50         # generate 50 orders
uv run main.py --generate --validate        # generate 100 orders, skip any that fail validation
uv run main.py --generate --count 500 --validate
uv run main.py --env prod --generate --count 200   # prod env, 200 orders
```

### Publish mode

Use `--publish` to generate orders and publish each one to Zerobus (same `--count` and env as generate). Requires `ZEROBUS_*` and `UC_*` env vars. By default the sync `ZerobusWriter` is used; add `--async-publish` to use the async writer and record offsets per session. Use `--validate` when generating (without `--orders-file`) so only valid orders are published.

```bash
uv run main.py --publish                      # generate and publish 100 orders (sync)
uv run main.py --publish --count 20           # generate and publish 20 orders
uv run main.py --publish --validate           # generate 100 valid orders and publish
uv run main.py --publish --async-publish      # async writer, records offsets per session
uv run main.py --publish --async-publish --count 50
uv run main.py --env prod --publish --count 50
```

| Option            | Default | Description |
|-------------------|--------|-------------|
| `--env`           | dev    | Environment: `dev` (loads `.env`) or `prod` (loads `.env-prod`). |
| `--generate`      | off    | Generate sample orders via `datagen.Orders.generate_orders()` and print to stdout. |
| `--publish`       | off    | Generate orders and publish each to Zerobus via `ZerobusWriter` (sync) or `AsyncZerobusWriter` (if `--async-publish`). |
| `--async-publish` | off    | Use async writer with `--publish`; records offsets per session and uses default ack callback for progress logging. |
| `--count`         | 100    | Number of records to generate when `--generate` or `--publish` is set. |
| `--validate`     | off    | When generating orders, validate each with protovalidate; only valid orders count toward `--count`. Use with `--generate`, `--publish`, or `--generate-orders-file`. |
| `--generate-orders-file` | —  | Path to write generated orders to a **binary file** (length-delimited protobuf). Use with `--count` (e.g. 10k orders). |
| `--orders-file`          | —  | Path to a .bin file from `--generate-orders-file`. Load orders from file instead of generating (for reproducible testing with `--publish` or `--generate`). Optional `--count` uses only the first N orders. |

### Generate orders to a binary file

Generate a large batch of orders and store them in a single binary file (length-delimited: each record is varint length + serialized `Order` bytes). No proto change required; read the file back into a `list[Order]` in one call. Useful for benchmarks, replay, or feeding the same dataset to publish multiple times. Use `--validate` to ensure only protovalidate-valid orders are written.

```bash
uv run main.py --generate-orders-file orders_10k.bin --count 10000
uv run main.py --generate-orders-file orders_10k.bin --count 10000 --validate   # only valid orders
```

Then in Python:

```python
from zerobus_ingest.utils import read_orders_from_binary

orders = read_orders_from_binary("orders_10k.bin")  # list[Order]
```

To write orders to a file from code (e.g. after custom generation):

```python
from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import write_orders_to_binary

orders = Orders.generate_orders(10_000, seed=42)
write_orders_to_binary("orders_10k.bin", orders)

# With validation: only valid orders count toward the requested count
orders = Orders.generate_orders(10_000, seed=42, validate=True)
write_orders_to_binary("orders_10k.bin", orders)
```

### Reproducible testing: load orders from a .bin file

Use `--orders-file` to run `--publish` or `--generate` with a fixed dataset from a previously saved .bin file instead of random generation. Same orders every run for non-random testing.

**Example workflow**

1. Create a fixed dataset once (e.g. 10k orders).
2. Publish from that file (all records, or first N with `--count`).
3. Optionally print orders from file for inspection.

```bash
# 1) Create a fixed dataset once
uv run main.py --generate-orders-file orders_10k.bin --count 10000

# 2) Publish from that file (all 10k, or first N with --count)
uv run main.py --publish --orders-file orders_10k.bin
uv run main.py --publish --orders-file orders_10k.bin --count 100
uv run main.py --publish --async-publish --orders-file orders_10k.bin --count 500

# 3) Or print orders from file (e.g. first 5)
uv run main.py --generate --orders-file orders_10k.bin --count 5
```

---

## Releasing artifacts

To release the application (build, sanity-check, test, package, bump version, and publish), run the steps in order. Each step is a Makefile target.

| Step        | Command            | What it does |
|------------|--------------------|--------------|
| **build**  | `make build`       | Lint and format protos; run buf generate; format and lint Python (ruff). |
| **generate** | `make generate` | Depends on build; runs the app in generate mode (100 orders) as a sanity check. |
| **test**   | `make test`       | Runs pytest. |
| **package**| `make package`    | Builds wheel and sdist into `dist/` (`uv build`). |
| **release**| `make release [bump=minor]` | Bumps the version in `pyproject.toml`. Default is `minor`; use `bump=patch`, `bump=major`, etc. |
| **publish**| `make publish`    | Depends on package; uploads `dist/*` to PyPI (or `UV_PUBLISH_INDEX`). Requires `UV_PUBLISH_TOKEN` (or username/password). |

**Typical release flow**

```bash
make build
make generate
make test
make package
make release              # or: make release bump=patch
make publish              # or: UV_PUBLISH_TOKEN=pypi-xxx make publish
```

You can run the first four steps without credentials. For `make publish`, set a PyPI API token (e.g. `export UV_PUBLISH_TOKEN=pypi-...`) or use `uv publish --dry-run` to test without uploading.

---

## TableUtils

`TableUtils` provides helpers for Unity Catalog table operations using the Databricks SDK `WorkspaceClient` (same credentials as your workspace). Use `table_exists` to check if a table exists before publishing, and `create_table` to create a managed Delta table (e.g. for Zerobus ingest) without passing a storage path—the location is derived from the schema’s storage root.

### Check table exists and create from a protobuf descriptor

A common workflow is: **check if the table exists in Databricks; if not, derive the table schema from your protobuf message and create the table.** That way you can ensure the Unity Catalog table exists (and matches your proto) before running `--publish` or writing with `ZerobusWriter`.

- **`table_exists(workspace_client, catalog, schema, table)`** — Returns `True` if the table exists in the metastore.
- **`descriptor_to_columns(descriptor: Descriptor) -> list[ColumnInfo]`** — Converts a protobuf message `DESCRIPTOR` (e.g. `Order.DESCRIPTOR` from your generated `orders_pb2`) into a list of Unity Catalog `ColumnInfo`. Uses the inverse of the [zerobus generate_proto type mappings](https://github.com/databricks/zerobus-sdk-py/blob/main/zerobus/tools/generate_proto.py): scalars (int32→INT, int64→LONG, string→STRING, etc.), enums→INT, nested messages→`STRUCT<...>`, repeated→`ARRAY<...>`. The result is suitable for `create_table(..., columns=...)`.
- **`pretty_print_columns(columns: list[ColumnInfo]) -> str`** — Formats a list of `ColumnInfo` as a readable table string (one column per line with name and type). Use this to preview the schema before creating the table.

**Example: ensure table exists, creating it from the Order protobuf if missing**

```python
from dotenv import load_dotenv

from databricks.sdk import WorkspaceClient

from zerobus_ingest.config import Config
from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import TableUtils

load_dotenv()
config = Config.databricks()
client = WorkspaceClient(host=config["host"], token=config["token"])

catalog, schema, table = config["catalog"], config["schema"], config["table"]

if not TableUtils.table_exists(client, catalog, schema, table):
    # Get ColumnInfo from the Order message DESCRIPTOR
    descriptor = Orders.generate_orders(1, seed=42)[0].DESCRIPTOR
    columns = TableUtils.descriptor_to_columns(descriptor)

    # Optional: preview the table schema
    print("Table schema (from proto):")
    print(TableUtils.pretty_print_columns(columns))

    TableUtils.create_table(client, catalog, schema, table, columns=columns)
    print(f"Created table {catalog}.{schema}.{table}")
else:
    print("Table already exists.")
```

### create_table

Creates a table in the metastore. Defaults to a **managed** Delta table; if you omit `storage_location`, it is derived from the schema’s `storage_root`. For external tables, pass `storage_location` and `table_type=TableType.EXTERNAL`.

You can pass **columns** as a list of `ColumnInfo` from `databricks.sdk.service.catalog` (use `name` and `type_name` with `ColumnTypeName`), and **properties** for Delta table properties (e.g. change data feed, column mapping).

**Example: create a managed Delta table with columns and Delta properties**

```python
from dotenv import load_dotenv

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ColumnInfo,
    ColumnTypeName,
    TableType,
)

from zerobus_ingest.config import Config
from zerobus_ingest.utils import TableUtils

load_dotenv()
config = Config.databricks()

client = WorkspaceClient(host=config["host"], token=config["token"])

columns = [
    ColumnInfo(name="order_id", type_name=ColumnTypeName.STRING),
    ColumnInfo(name="customer_id", type_name=ColumnTypeName.STRING),
    ColumnInfo(name="total_units", type_name=ColumnTypeName.LONG),
]

delta_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.columnMapping.mode": "name",
}

table_info = TableUtils.create_table(
    client,
    catalog=config["catalog"],
    schema=config["schema"],
    table=config["table"],
    columns=columns,
    properties=delta_properties,
)
print(f"Created table {table_info.full_name}")
```

- **ColumnInfo** (`databricks.sdk.service.catalog`): use `name` and `type_name` (e.g. `ColumnTypeName.STRING`, `ColumnTypeName.LONG`, `ColumnTypeName.DOUBLE`, `ColumnTypeName.BOOLEAN`, `ColumnTypeName.TIMESTAMP`). Optional: `comment`, `nullable`, etc.
- **Delta properties** (optional): e.g. `delta.enableChangeDataFeed` for change data feed, `delta.columnMapping.mode` for column mapping. See [Delta table properties](https://docs.delta.io/table-properties).

To create an **external** Delta table with your own path, pass `storage_location` and `table_type=TableType.EXTERNAL`.

---

## VolumeUtils

`VolumeUtils` helps upload local files to [Unity Catalog volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html) using the Databricks SDK `WorkspaceClient`. You need `USE CATALOG`, `USE SCHEMA`, and `WRITE VOLUME` on the target volume.

### Upload a file to a UC volume

- **`upload_file(workspace_client, file, destination, *, overwrite=True) -> bool`** — Uploads a local file to a Unity Catalog volume path. `destination` is the full volume path (e.g. a directory). If it doesn’t end with the file’s name, the file is written to `destination/<file.name>`.

Volume paths use the form:

```
/Volumes/<catalog_name>/<schema_name>/<volume_name>/<optional_path>
```

**Example: upload descriptor.bin to a volume**

```python
from pathlib import Path

from dotenv import load_dotenv

from databricks.sdk import WorkspaceClient

from zerobus_ingest.config import Config
from zerobus_ingest.utils import VolumeUtils

load_dotenv()
config = Config.databricks()
client = WorkspaceClient(host=config["host"], token=config["token"])

# Upload the generated descriptor to a volume (e.g. for Spark or other consumers)
VolumeUtils.upload_file(
    client,
    Path("gen/python/orders/v1/descriptor.bin"),
    "/Volumes/scotts_playground/demos/apps/schemas/protos/orders/v1/",
)
# File is at: .../v1/descriptor.bin
```

You can pass a full file path as `destination` (including the filename) to control the name in the volume; otherwise the local filename is used under the given directory.

---

## ZerobusWriter

`ZerobusWriter` wraps the Zerobus ingest SDK to write protobuf (or dict) records to a Zerobus stream. The stream is created lazily on the first `write()`; when the record is a protobuf message, the message’s `DESCRIPTOR` is used for `TableProperties` so the stream schema matches the message type.

### Config

Build a writer from the same config dict used elsewhere (e.g. from `Config.databricks()`). The config must include:

- **host**, **workspace_url** — Databricks workspace
- **zerobus_client_id**, **zerobus_client_secret** — Zerobus client credentials
- **catalog**, **schema**, **table** — Unity Catalog target (e.g. from `UC_CATALOG`, `UC_SCHEMA`, `UC_TABLE`)

Load env (e.g. `python-dotenv`) before calling `Config.databricks()` so these are set.

### Basic usage

Use `from_config()` and the context manager to ensure the stream is closed after writing. Call `write(record)` for each record and `flush()` before closing.

```python
from dotenv import load_dotenv

from zerobus_ingest.config import Config
from zerobus_ingest.utils import ZerobusWriter

load_dotenv()
config = Config.databricks()

with ZerobusWriter.from_config(config) as writer:
    for record in my_records:
        writer.write(record)
    writer.flush()
# writer.close() called automatically
```

### Example: Config + datagen

Generate orders with `Orders.generate_orders()` and publish them through `ZerobusWriter`:

```python
from dotenv import load_dotenv

from zerobus_ingest.config import Config
from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import ZerobusWriter

load_dotenv()
config = Config.databricks()

orders = Orders.generate_orders(count=50, seed=42)

with ZerobusWriter.from_config(config) as writer:
    for order in orders:
        writer.write(order)
    writer.flush()
print(f"Published {len(orders)} orders to Zerobus.")
```

This matches what the CLI does with `uv run main.py --publish --count 50`.

### API summary

| Method / API | Description |
|--------------|-------------|
| `ZerobusWriter.from_config(config)` | Build a writer from a config dict (e.g. `Config.databricks()`). |
| `writer.write(record)` | Ingest one record (protobuf message or dict). Returns `RecordAcknowledgment`. Lazy-creates the stream on first write; for protobuf messages, the record’s `DESCRIPTOR` is used for `TableProperties`. |
| `writer.flush()` | Flush the stream. |
| `writer.close()` | Close the stream and release resources. Use `with ZerobusWriter.from_config(config) as writer:` to close automatically. |
| `ZerobusWriter.get_descriptor(record)` | Static: return the record’s `DESCRIPTOR` if it is a protobuf message, else `None`. Useful in tests to assert the descriptor used for `TableProperties` (e.g. `assert ZerobusWriter.get_descriptor(order).full_name == "orders.v1.Order"`). |

---

## AsyncZerobusWriter

`AsyncZerobusWriter` uses the Zerobus async SDK (`zerobus.sdk.aio`) for async ingestion. Use it when you are already in an async context (e.g. FastAPI, aiohttp) or when you want per-record offsets and fire-and-forget options. Default stream options include `record_type=RecordType.PROTO`, `max_inflight_records=5_000`, `recovery=True`, and an optional ack callback for progress logging.

### When to use async vs sync

- **Sync (`ZerobusWriter`)** — Simple scripts, CLI `--publish` without `--async-publish`, or when you prefer blocking `write()` and `ack.wait_for_ack()`.
- **Async (`AsyncZerobusWriter`)** — Async apps, or when you need `write_offset()` for session offsets, batch/offset methods, or fire-and-forget (`write_nowait` / `write_batch_nowait`). The CLI uses it when you pass `--publish --async-publish`.

### Config

Same config shape as `ZerobusWriter`: use `from_config(config)` (and optionally `ack_callback=...)`). The same env vars and `Config.databricks()` apply.

### ZerobusWriteCallback

`ZerobusWriteCallback` logs progress every N acks and can forward to an inner callback. It is used as the default ack callback when you don't pass one to `AsyncZerobusWriter.from_config()`. You can pass a custom instance or another object that implements `on_ack(offset)`.

```python
from zerobus_ingest.utils import ZerobusWriteCallback, AsyncZerobusWriter

# Default: log every 100 acks
callback = ZerobusWriteCallback(log_every_n=100)
writer = AsyncZerobusWriter.from_config(config, ack_callback=callback)

# With inner callback (e.g. your own AckCallback-like object)
inner = MyAckHandler()
callback = ZerobusWriteCallback(inner=inner, log_every_n=50)
writer = AsyncZerobusWriter.from_config(config, ack_callback=callback)
```

### Basic usage (async)

Use `async with AsyncZerobusWriter.from_config(config) as writer:` and await `write_offset(record)` to ingest one record and get back the result (offset/ack). Then `await writer.flush()` and the context manager will call `await writer.close()`.

```python
import asyncio
from dotenv import load_dotenv

from zerobus_ingest.config import Config
from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import AsyncZerobusWriter

load_dotenv()
config = Config.databricks()
orders = Orders.generate_orders(count=50, seed=42)

async def publish():
    async with AsyncZerobusWriter.from_config(config) as writer:
        for order in orders:
            await writer.write_offset(order)  # records offset per record
        await writer.flush()
    print(f"Published {len(orders)} orders (async).")

asyncio.run(publish())
```

This matches what the CLI does with `uv run main.py --publish --async-publish --count 50`.

### Overriding stream options

Use `with_stream_options()` before opening the stream (e.g. before the first write) to override defaults (e.g. `max_inflight_records` or a custom ack callback):

```python
from zerobus.sdk.shared.definitions import RecordType, StreamConfigurationOptions
from zerobus_ingest.utils import AsyncZerobusWriter, ZerobusWriteCallback

opts = StreamConfigurationOptions(
    record_type=RecordType.PROTO,
    max_inflight_records=10_000,
    recovery=True,
    ack_callback=ZerobusWriteCallback(log_every_n=200),
)
writer = AsyncZerobusWriter.from_config(config).with_stream_options(opts)
async with writer as w:
    for order in orders:
        await w.write_offset(order)
    await w.flush()
```

### API summary (async)

| Method / API | Description |
|--------------|-------------|
| `AsyncZerobusWriter.from_config(config, ack_callback=...)` | Build an async writer from a config dict. Optional `ack_callback` (default: `ZerobusWriteCallback()` for progress logging). |
| `writer.with_stream_options(options)` | Override stream options (e.g. `max_inflight_records`, `ack_callback`). Call before first write. |
| `await writer.write_offset(record)` | Ingest one record and return the result (offset/ack). Use for recording offsets per session. |
| `await writer.write_batch_offset(records)` | Ingest a batch and return a list of results (one per record). |
| `writer.write_nowait(record)` | Fire-and-forget: queue one record without waiting (must be called from an async context with a running loop). |
| `writer.write_batch_nowait(records)` | Fire-and-forget: queue a batch without waiting. |
| `await writer.flush()` | Flush the stream and wait for durability. |
| `await writer.close()` | Close the stream. Use `async with AsyncZerobusWriter.from_config(config) as writer:` to close automatically. |
| `AsyncZerobusWriter.get_descriptor(record)` | Static: same as `ZerobusWriter.get_descriptor(record)`. |
| `ZerobusWriteCallback(inner=..., log_every_n=100)` | Ack callback that logs every N acks and optionally forwards to an inner callback. |
