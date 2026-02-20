# Zerobus
What happens when you remove the friction of managing Kafka clusters and instead provide a simple `gRPC` endpoint
that will autoscale to meet your data needs? That is [zerobus](https://www.databricks.com/blog/announcing-public-preview-zerobus-ingest) in a nutshell, only you don't have to manage the server and instead can let it all run like any typical serverless offering. It is available when you need it, and without the headache of maintainence. Joy!

# Zerobus Flow
This is a set of data definitions (protobuf), data generators, and other utility functions that can be used to showcase how easy it is to utilize `zerobus`. Think of this as the typical data client SDK (persona) that is emitting specific data points for different use cases. Each use case is represented by a `Scenario`. This allows us to model real-world experiences like `ecommerce core buy flow` - the process of finding items, putting them in a cart, updating the cart, and eventually ordering, or abandoning said cart.

> All sources pertaining to `zerobus-flow` are in that package.

### Scenarios
As stated above, a scenario is a collection of `events` that are generated to represent the real world. There is no limit to how a scenario can be generated, the only requirement is that each `Scenario[T]` is bound to a concrete `protobuf` module.

```
// todo - what does the class invocation look like and what does the eCommerce scenario generation feel like?
```

# Zerobus Ingest
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