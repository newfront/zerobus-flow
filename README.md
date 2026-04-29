# Zerobus
What happens when you remove the friction of managing Kafka clusters and instead provide a simple `gRPC` endpoint
that will autoscale to meet your data needs? That is [zerobus](https://www.databricks.com/blog/announcing-public-preview-zerobus-ingest) in a nutshell, only you don't have to manage the server and instead can let it all run like any typical serverless offering. It is available when you need it, and without the headache of maintainence. Joy!

# Zerobus Ingest
> **status**: MVP
This is a set of classes and utility methods that assist you in your Databricks Zerobus journey. This includes methods for syncing files between your local project and Unity Catalog volumes, as well as testing for the existance of `tables` and creating tables based off of `protobuf` DESCRIPTORS. At the moment, the `zerobus-ingest` library is written entirely in python, but `rust` flows will be made available as well.

As the API currently stands, you can create a new instance of the `ZerobusWriter` and then simply iterate over a list of `list[T : message.Message]` - where `message` is the `google.protobuf.message` package - and `write` each record to the `zerobus` api endpoint.

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

---

# Running the End-to-End Demo

## Prerequisites

### 1. Install the `buf` CLI (optional, for proto toolchain)

If you want to compile protos or regenerate descriptors locally, you'll need the [buf CLI](https://buf.build/docs/installation). Follow the installation instructions for your platform at **[buf.build/docs/installation](https://buf.build/docs/installation)**.

### 2. Set up your Python environment

From the repo root, sync the virtual environment:

```bash
just setup
```

### 3. Verify your toolchain

Build protos, regenerate the descriptor, and run the test suite to confirm everything is wired up correctly:

```bash
just preflight
```

---

## Databricks Workspace Setup

Before running the demo you need a Unity Catalog destination with managed external storage.

### Create the catalog and schema

In your Databricks workspace, create:

- A **catalog** named `demos`
- A **schema** named `zerobus` inside that catalog

You can do this via the [Databricks UI](https://docs.databricks.com/en/catalogs/create-catalog.html), the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/unity-catalog-commands.html), or SQL:

```sql
CREATE CATALOG IF NOT EXISTS demos
  MANAGED LOCATION 's3://your-bucket/demos';  -- replace with your storage root

CREATE SCHEMA IF NOT EXISTS demos.zerobus;
```

### Apply a `storage_root` to the catalog

The `demos` catalog **must** have an explicit [managed storage location (storage root)](https://docs.databricks.com/en/data-governance/unity-catalog/manage-external-locations-and-credentials.html) configured at the catalog level. This is the cloud storage path (e.g. an S3 bucket/prefix) where Unity Catalog will land the data written through Zerobus.

See the Databricks docs for full details:
- [Create a catalog with a managed location](https://docs.databricks.com/en/catalogs/create-catalog.html)
- [Manage external locations and storage credentials](https://docs.databricks.com/en/data-governance/unity-catalog/manage-external-locations-and-credentials.html)

> **Important:** Do **not** rely on the default storage that is automatically provisioned with a serverless Databricks workspace. Zerobus requires an explicitly configured external storage location at the catalog level — the default workspace storage is not supported at this time.

---

## Bootstrap the Demo Table

Once your catalog and schema are ready, run:

```bash
just bootstrap-demo
```

This creates the `demos.zerobus.orders` table from the protobuf descriptor if it does not already exist.

**At this step, you also need to grant your service principal the permissions required for Zerobus to write externally to Unity Catalog.** This typically includes `WRITE VOLUME`, `USE CATALOG`, `USE SCHEMA`, and `CREATE TABLE` (or equivalent) on the `demos` catalog and `zerobus` schema, as well as the necessary privileges on the underlying external location / storage credential.

Refer to the [Unity Catalog privilege model](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html) for the full list of grants and how to apply them to a service principal.

---

## Run the Full Demo

With the table bootstrapped and permissions in place, kick off the end-to-end flow:

```bash
just demo-zerobus
```

This command generates synthetic order events, publishes them to Zerobus, and then queries the resulting Delta table so you can see the data land in Unity Catalog — all in one shot.

### Parameters

You can tune the run with two optional overrides:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `count`   | `100`   | Number of order events to generate and publish to Zerobus. |
| `limit`   | `100`   | Number of rows to return when querying the Delta table after ingestion. |

**Examples:**

```bash
# Publish 500 events and return the first 50 rows
just count=500 limit=50 demo-zerobus

# Quick smoke-test with 10 events
just count=10 limit=10 demo-zerobus
```

---

## Bring Your Own Protobuf

The demo uses the built-in `orders.v1.Order` schema, but Zerobus works with any protobuf — as long as you follow one structural rule.

### The single root-message rule

Zerobus reads your schema from a compiled [FileDescriptorSet](https://buf.build/docs/reference/descriptors) (the `.bin` descriptor file) and targets a single named message type. For this to work correctly, **everything your message depends on — nested messages, enums, and sub-types — must be declared inside that one root message.**

The `orders.proto` schema is the canonical example of this pattern. `Money`, `Address`, `OrderLineItem`, `OrderStatus`, and `PaymentMethod` are all defined as nested types *inside* `message Order`, not as separate top-level declarations:

```protobuf
message Order {
  enum OrderStatus { ... }      // nested enum
  enum PaymentMethod { ... }    // nested enum

  message Money { ... }         // nested message
  message Address { ... }       // nested message
  message OrderLineItem { ... } // nested message

  // top-level fields reference the nested types above
  repeated OrderLineItem line_items = 4;
  Money subtotal = 5;
  Address shipping_address = 9;
  ...
}
```

If supporting types live outside the root message as standalone top-level definitions, the descriptor will not be self-contained and Zerobus will be unable to resolve the full schema from a single message name.

### Steps to use your own schema

1. **Write your `.proto`** following the single root-message pattern above.
2. **Compile the descriptor** — run `buf build` (or `protoc --descriptor_set_out`) to produce a `descriptor.bin` file.
3. **Update the Justfile variables** to point at your descriptor and message name:
   ```
   descriptor_path := "gen/python/<your-package>/descriptor.bin"
   message_name    := "<your.package>.YourMessage"
   ```
4. **Run `just bootstrap-demo`** to create the table, then **`just demo-zerobus`** (or wire up your own data generator) to start streaming.

---

## Gotchas

Things learned the hard way, so you don't have to.

### Enum fields must be declared as `string` with validation constraints

Spark's protobuf support (via `from_protobuf`) deserializes enum fields as their **string name**, not as an integer or a native proto enum type. If you declare a field directly as an enum type, the resulting Delta column ends up as a plain integer — which is rarely what you want and makes downstream queries painful.

The pattern used throughout `orders.proto` is to declare the field as `string` and then use `buf.validate` constraints to enforce the allowed values at the proto level:

```protobuf
// Declare the enum for documentation and IDE tooling purposes...
enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_PENDING     = 1;
  // ...
}

// ...but store the field as a validated string so Spark lands it correctly.
string status = 3 [(buf.validate.field).string = {
  in: [
    "ORDER_STATUS_UNSPECIFIED",
    "ORDER_STATUS_PENDING",
    "ORDER_STATUS_CONFIRMED",
    "ORDER_STATUS_PROCESSING",
    "ORDER_STATUS_SHIPPED",
    "ORDER_STATUS_DELIVERED",
    "ORDER_STATUS_CANCELLED",
    "ORDER_STATUS_REFUNDED"
  ]
}];
```

The same pattern applies to `payment_method` in `orders.proto`. The enum types (`OrderStatus`, `PaymentMethod`) are still defined inside `message Order` — they serve as documentation and can be used by clients for validation — but the wire fields are `string` so the Delta table columns arrive as human-readable string values rather than opaque integers.

**Rule of thumb:** any field that would be an enum in a pure-proto world should be a `string` with a `buf.validate` `string.in` constraint when targeting Spark / Databricks ingestion.

# Zerobus Flow
> status: work in progress
This is a set of data definitions (protobuf), data generators, and other utility functions that can be used to showcase how easy it is to utilize `zerobus`. Think of this as the typical data client SDK (persona) that is emitting specific data points for different use cases. Each use case is represented by a `Scenario`. This allows us to model real-world experiences like `ecommerce core buy flow` - the process of finding items, putting them in a cart, updating the cart, and eventually ordering, or abandoning said cart.

> All sources pertaining to `zerobus-flow` are in that package.

### Scenarios
As stated above, a scenario is a collection of `events` that are generated to represent the real world. There is no limit to how a scenario can be generated, the only requirement is that each `Scenario[T]` is bound to a concrete `protobuf` module.

```
// todo - what does the class invocation look like and what does the eCommerce scenario generation feel like?
```
