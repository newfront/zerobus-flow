set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

ingest_dir := "zerobus-ingest"
catalog := "demos"
schema := "zerobus"
table := "orders"
table_name := "demos.zerobus.orders"
descriptor_path := "gen/python/orders/v1/descriptor.bin"
message_name := "orders.v1.Order"
generate := "orders"
# Number of records to generate
count := "100"
# Number of records to query
limit := "100"

# (Re)create the virtual environment. Run once after cloning or if the venv becomes stale.
setup:
    @cd {{ingest_dir}} && uv sync

# Build protos, generate descriptor, and run tests to verify everything is ready.
preflight:
    @cd {{ingest_dir}} && make build
    @cd {{ingest_dir}} && make descriptor
    @cd {{ingest_dir}} && make test

# Setup demo table from protobuf descriptor if missing.
bootstrap-demo:
    @cd {{ingest_dir}} && uv run python scripts/demo_just.py bootstrap \
      --catalog {{catalog}} \
      --schema {{schema}} \
      --table {{table}} \
      --table-name {{table_name}} \
      --descriptor-path {{descriptor_path}} \
      --message-name {{message_name}}

# Tear down demo table from Unity Catalog.
teardown-demo:
    @cd {{ingest_dir}} && uv run python scripts/demo_just.py teardown \
      --table-name {{table_name}}

# Example: just query-zerobus
# Example: just limit=25 query-zerobus
query-zerobus: bootstrap-demo
    @cd {{ingest_dir}} && uv run python scripts/demo_just.py query \
      --table-name {{table_name}} \
      --limit {{limit}}

# Example: just demo-zerobus
# Example: just generate=orders limit=25 count=20 demo-zerobus
demo-zerobus: bootstrap-demo
    @cd {{ingest_dir}} && uv run main.py --publish --async-publish --count {{count}}
    @cd {{ingest_dir}} && uv run python scripts/demo_just.py query \
      --table-name {{table_name}} \
      --limit {{limit}}
