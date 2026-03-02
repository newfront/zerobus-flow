"""Read/write a list of Order messages to a single binary file (length-delimited format).

Uses the standard protobuf length-delimited encoding: for each message, write
the length as a varint then the serialized message bytes. No proto change needed;
one parse per order when reading, one serialize per order when writing.
"""

from __future__ import annotations

from pathlib import Path

# Use same Order type as datagen (avoids circular import from datagen)
try:
    from orders.v1 import orders_pb2
except ImportError:
    import sys
    _root = Path(__file__).resolve().parent.parent.parent.parent
    _gen = _root / "gen" / "python"
    if str(_gen) not in sys.path:
        sys.path.insert(0, str(_gen))
    from orders.v1 import orders_pb2  # noqa: E402

Order = orders_pb2.Order


def _encode_varint(value: int) -> bytes:
    """Encode a non-negative int as a protobuf varint."""
    parts: list[int] = []
    while value > 0x7F:
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    parts.append(value & 0x7F)
    return bytes(parts)


def _decode_varint_from_stream(stream: bytes, pos: int) -> tuple[int, int]:
    """Decode one varint from stream starting at pos. Returns (value, new_pos)."""
    result = 0
    shift = 0
    while pos < len(stream):
        b = stream[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return result, pos
        shift += 7
        if shift >= 35:
            raise ValueError("Varint too long")
    raise ValueError("Truncated varint")


def write_orders_to_binary(path: Path | str, orders: list[Order]) -> None:
    """Write a list of Order messages to a binary file (length-delimited).

    Each record is stored as: varint(length) + serialized Order bytes.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        for order in orders:
            data = order.SerializeToString()
            f.write(_encode_varint(len(data)))
            f.write(data)


def read_orders_from_binary(path: Path | str) -> list[Order]:
    """Read a list of Order messages from a length-delimited binary file.

    Returns the same list[Order] shape as Orders.generate_orders().
    """
    path = Path(path)
    data = path.read_bytes()
    orders: list[Order] = []
    pos = 0
    while pos < len(data):
        length, pos = _decode_varint_from_stream(data, pos)
        if pos + length > len(data):
            raise ValueError(
                f"Truncated message at byte {pos}: need {length}, have {len(data) - pos}"
            )
        msg = Order()
        msg.ParseFromString(data[pos : pos + length])
        orders.append(msg)
        pos += length
    return orders
