"""Tests for ZerobusWriter, ZerobusWriteCallback, and AsyncZerobusWriter."""

from pathlib import Path
from unittest.mock import MagicMock

from dotenv import load_dotenv

from zerobus_ingest.config import Config
from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import AsyncZerobusWriter, ZerobusWriteCallback, ZerobusWriter

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _REPO_ROOT / ".env"

_REQUIRED_KEYS = (
    "host",
    "workspace_id",
    "workspace_url",
    "region",
    "zerobus_client_id",
    "zerobus_client_secret",
    "catalog",
    "schema",
    "table",
)


def _config_ready() -> bool:
    """True if .env exists and required config keys are set."""
    if not _ENV_FILE.exists():
        return False
    load_dotenv(_ENV_FILE)
    config = Config.databricks()
    return all(config.get(k) for k in _REQUIRED_KEYS)


def test_get_descriptor_returns_none_for_non_protobuf():
    """get_descriptor returns None for objects without DESCRIPTOR."""
    assert ZerobusWriter.get_descriptor({}) is None
    assert ZerobusWriter.get_descriptor("not a message") is None
    assert ZerobusWriter.get_descriptor(None) is None


def test_get_descriptor_from_generated_order():
    """
    get_descriptor(order)
    returns the Order message DESCRIPTOR with expected details."""
    orders = Orders.generate_orders(1, seed=42)
    assert len(orders) == 1
    order = orders[0]

    descriptor = ZerobusWriter.get_descriptor(order)

    assert descriptor is not None
    assert descriptor is order.DESCRIPTOR
    assert descriptor.name == "Order"
    assert descriptor.full_name == "orders.v1.Order"


def test_zerobus_write_callback_increments_count():
    """ZerobusWriteCallback increments ack count on each on_ack."""
    cb = ZerobusWriteCallback(log_every_n=100)
    assert cb._ack_count == 0
    cb.on_ack(1)
    assert cb._ack_count == 1
    cb.on_ack(2)
    cb.on_ack(3)
    assert cb._ack_count == 3


def test_zerobus_write_callback_forwards_to_inner():
    """ZerobusWriteCallback forwards on_ack to inner callback when provided."""
    inner = MagicMock()
    cb = ZerobusWriteCallback(inner=inner, log_every_n=1000)
    cb.on_ack(42)
    inner.on_ack.assert_called_once_with(42)


def test_zerobus_write_callback_logs_every_n(caplog):
    """ZerobusWriteCallback logs progress every log_every_n acks."""
    import logging

    caplog.set_level(logging.INFO)
    cb = ZerobusWriteCallback(log_every_n=2)
    cb.on_ack(1)
    assert "Acknowledged" not in caplog.text
    cb.on_ack(2)
    assert "Acknowledged" in caplog.text and "batch #2" in caplog.text


def test_async_zerobus_writer_get_descriptor():
    """AsyncZerobusWriter.get_descriptor matches ZerobusWriter for Order messages."""
    orders = Orders.generate_orders(1, seed=42)
    order = orders[0]
    assert AsyncZerobusWriter.get_descriptor(order) is not None
    assert AsyncZerobusWriter.get_descriptor(order) is order.DESCRIPTOR
    assert AsyncZerobusWriter.get_descriptor({}) is None


def test_async_zerobus_writer_from_config():
    """AsyncZerobusWriter.from_config builds writer with correct table name and options."""
    config = {
        "host": "h",
        "workspace_url": "https://example.databricks.com",
        "workspace_id": "ws-id",
        "region": "us-east-1",
        "zerobus_client_id": "cid",
        "zerobus_client_secret": "secret",
        "catalog": "cat",
        "schema": "sch",
        "table": "tbl",
    }
    writer = AsyncZerobusWriter.from_config(config)
    assert writer._table_name == "cat.sch.tbl"
    assert writer._stream_options is not None
    assert writer._stream is None
    assert writer._sdk is None


def test_async_zerobus_writer_from_config_with_ack_callback():
    """AsyncZerobusWriter.from_config accepts optional ack_callback."""
    config = {
        "host": "h",
        "workspace_url": "https://example.databricks.com",
        "workspace_id": "ws-id",
        "region": "us-east-1",
        "zerobus_client_id": "cid",
        "zerobus_client_secret": "secret",
        "catalog": "cat",
        "schema": "sch",
        "table": "tbl",
    }
    custom_cb = ZerobusWriteCallback(log_every_n=50)
    writer = AsyncZerobusWriter.from_config(config, ack_callback=custom_cb)
    assert writer._stream_options is not None
    assert writer._stream_options.ack_callback is custom_cb


def test_async_zerobus_writer_with_stream_options():
    """AsyncZerobusWriter.with_stream_options overwrites options and returns self."""
    from zerobus.sdk.shared.definitions import RecordType, StreamConfigurationOptions

    config = {
        "host": "h",
        "workspace_url": "https://example.databricks.com",
        "workspace_id": "ws-id",
        "region": "us-east-1",
        "zerobus_client_id": "cid",
        "zerobus_client_secret": "secret",
        "catalog": "cat",
        "schema": "sch",
        "table": "tbl",
    }
    writer = AsyncZerobusWriter.from_config(config)
    opts = StreamConfigurationOptions(record_type=RecordType.PROTO, max_inflight_records=100)
    out = writer.with_stream_options(opts)
    assert out is writer
    assert writer._stream_options is opts


# @pytest.mark.skipif(
#     not _config_ready(),
#     reason=".env missing or incomplete; set DATABRICKS_*, "
#     + "ZEROBUS_*, UC_* for create_stream test",
# )
# def test_create_stream_with_config_and_table_properties(caplog):
#     """
#     Create ZerobusSdk from .env config, build TableProperties
#     from Config + Order descriptor,
#     and call create_stream to debug stream creation
#     (requires Databricks + Zerobus connectivity).
#     """
#     load_dotenv(_ENV_FILE)
#     config = Config.databricks()
#     writer = ZerobusWriter.from_config(config).with_stream_options(
#         StreamConfigurationOptions(record_type=RecordType.PROTO)
#     )
#
#     table_name = f"{config['catalog']}.{config['schema']}.{config['table']}"
#     orders = Orders.generate_orders(1, seed=42)
#     descriptor = orders[0].DESCRIPTOR
#     table_properties = TableProperties(table_name, descriptor_proto=descriptor)
#
#     with caplog.at_level(logging.INFO):
#         sdk = writer.generate_sdk()
#     assert "Server endpoint:" in caplog.text or "zerobus" in caplog.text.lower()
#     assert "Unity catalog" in caplog.text or "unity_catalog" in caplog.text.lower()
#
#     stream = sdk.create_stream(
#         client_id=config["zerobus_client_id"],
#         client_secret=config["zerobus_client_secret"],
#         table_properties=table_properties,
#         options=writer._stream_options,
#     )
#
#     assert stream is not None
#     stream.close()
