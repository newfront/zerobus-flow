"""Tests for main CLI (parse_args)."""

import sys

import pytest

from zerobus_ingest.main import parse_args


def test_parse_args_default_env():
    """Default --env is dev."""
    with pytest.MonkeyPatch.context() as m:
        m.setattr(sys, "argv", ["main.py"])
        args = parse_args()
    assert args.env == "dev"
    assert args.generate is False
    assert args.count == 100


def test_parse_args_env_prod():
    """--env prod is accepted."""
    with pytest.MonkeyPatch.context() as m:
        m.setattr(sys, "argv", ["main.py", "--env", "prod"])
        args = parse_args()
    assert args.env == "prod"


def test_parse_args_generate_and_count():
    """--generate and --count are parsed."""
    with pytest.MonkeyPatch.context() as m:
        m.setattr(sys, "argv", ["main.py", "--generate", "--count", "50"])
        args = parse_args()
    assert args.generate is True
    assert args.count == 50


def test_parse_args_async_publish():
    """--async-publish is parsed and can be combined with --publish."""
    with pytest.MonkeyPatch.context() as m:
        m.setattr(sys, "argv", ["main.py", "--publish", "--async-publish"])
        args = parse_args()
    assert args.publish is True
    assert args.async_publish is True


def test_parse_args_orders_file():
    """--orders-file is parsed for loading orders from .bin instead of generating."""
    with pytest.MonkeyPatch.context() as m:
        m.setattr(sys, "argv", ["main.py", "--publish", "--orders-file", "orders.bin"])
        args = parse_args()
    assert args.orders_file == "orders.bin"
    assert args.publish is True


def test_parse_args_validate():
    """--validate is parsed and can be combined with --generate or --publish."""
    with pytest.MonkeyPatch.context() as m:
        m.setattr(sys, "argv", ["main.py", "--generate", "--validate"])
        args = parse_args()
    assert args.validate is True
    assert args.generate is True


def test_get_orders_for_run_from_file(tmp_path):
    """_get_orders_for_run loads from .bin when orders_file is set and optionally limits by count."""
    from zerobus_ingest.datagen import Orders
    from zerobus_ingest.main import _get_orders_for_run
    from zerobus_ingest.utils import write_orders_to_binary

    path = tmp_path / "orders.bin"
    orders = Orders.generate_orders(10, seed=99)
    write_orders_to_binary(path, orders)

    # Load all
    loaded = _get_orders_for_run(str(path), count=100)
    assert len(loaded) == 10
    assert loaded[0].order_id == orders[0].order_id

    # Load first 3 with --count
    subset = _get_orders_for_run(str(path), count=3)
    assert len(subset) == 3
    assert subset[0].order_id == orders[0].order_id


def test_get_orders_for_run_generates_when_no_file():
    """_get_orders_for_run generates orders when orders_file is None."""
    from zerobus_ingest.main import _get_orders_for_run

    orders = _get_orders_for_run(None, count=5)
    assert len(orders) == 5
    assert orders[0].order_id  # has order_id
