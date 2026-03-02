"""Tests for orders_file write/read (length-delimited binary)."""

import tempfile
from pathlib import Path

from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import read_orders_from_binary, write_orders_to_binary


def test_write_and_read_orders_roundtrip():
    """Writing then reading the same orders returns identical list."""
    orders = Orders.generate_orders(50, seed=123)
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
        path = Path(f.name)
    try:
        write_orders_to_binary(path, orders)
        read_back = read_orders_from_binary(path)
        assert len(read_back) == 50
        for a, b in zip(orders, read_back):
            assert a.order_id == b.order_id
            assert a.SerializeToString() == b.SerializeToString()
    finally:
        path.unlink()


def test_read_empty_file_returns_empty_list():
    """Reading a file with zero length-delimited messages returns []."""
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
        path = Path(f.name)
        f.write(b"")
    try:
        result = read_orders_from_binary(path)
        assert result == []
    finally:
        path.unlink()


def test_write_creates_parent_dirs():
    """write_orders_to_binary creates parent directories if needed."""
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "sub" / "dir" / "orders.bin"
        orders = Orders.generate_orders(2, seed=1)
        write_orders_to_binary(path, orders)
        assert path.exists()
        read_back = read_orders_from_binary(path)
        assert len(read_back) == 2
