"""ZerobusWriter: abstract zerobus stream writing using ZerobusSdk."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Protocol

from google.protobuf.descriptor import Descriptor
from zerobus.sdk.aio import ZerobusSdk as AioZerobusSdk, ZerobusStream as AioZerobusStream
from zerobus.sdk.shared.definitions import (
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
)
from zerobus.sdk.sync import ZerobusSdk, ZerobusStream


class AckCallbackLike(Protocol):
    """Protocol for SDK ack callback (object with on_ack(offset))."""

    def on_ack(self, offset: int) -> None: ...


class ZerobusWriteCallback:
    """
    Acknowledgment callback that logs progress and optionally forwards to an inner
    callback. Use with AsyncZerobusWriter for progress logging (e.g. every N acks).
    """

    def __init__(
        self,
        inner: AckCallbackLike | None = None,
        *,
        log_every_n: int = 100,
    ) -> None:
        self._inner = inner
        self._log_every_n = log_every_n
        self._ack_count = 0

    def __call__(self, offset: int) -> None:
        """Called by the SDK when records are acknowledged (callback is invoked as callable)."""
        self.on_ack(offset)

    def on_ack(self, offset: int) -> None:
        """Called when records are acknowledged by the server."""
        self._ack_count += 1
        if self._ack_count % self._log_every_n == 0:
            logging.getLogger(__name__).info(
                "Acknowledged up to offset: %s (batch #%s)", offset, self._ack_count
            )
        if self._inner is not None:
            if callable(self._inner):
                self._inner(offset)
            else:
                self._inner.on_ack(offset)


class ZerobusWriter:
    """Wraps ZerobusSdk and ZerobusStream for writing records to a Zerobus stream."""

    def __init__(
        self,
        *,
        host: str,
        workplace_url: str,
        workspace_id: str,
        region: str,
        client_id: str,
        client_secret: str,
        catalog: str,
        schema: str,
        table: str,
        stream_options: StreamConfigurationOptions | None = None,
    ) -> None:
        self._host = host
        self._workplace_url = workplace_url
        self._workspace_id = workspace_id
        self._region = region
        self._client_id = client_id
        self._client_secret = client_secret
        self._table_name = f"{catalog}.{schema}.{table}"
        self._stream_options = stream_options or StreamConfigurationOptions()

        self._sdk: ZerobusSdk | None = None
        self._stream: ZerobusStream | None = None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ZerobusWriter:
        """Build a ZerobusWriter from a config dict (e.g. Config.databricks())."""
        return cls(
            host=config["host"],
            workplace_url=config["workspace_url"],
            workspace_id=config["workspace_id"],
            region=config["region"],
            client_id=config["zerobus_client_id"],
            client_secret=config["zerobus_client_secret"],
            catalog=config["catalog"],
            schema=config["schema"],
            table=config["table"],
        )

    def with_stream_options(self, options: StreamConfigurationOptions) -> ZerobusWriter:
        """
        Overwrite stream options.
        Call before any write() so they apply when the stream is created.
        """
        self._stream_options = options
        return self

    def generate_sdk(self) -> ZerobusSdk:
        """
        Build and return ZerobusSdk from config;
        log server_endpoint and unity_catalog_url.
        """
        server_endpoint = (
            f"{self._workspace_id}.zerobus.{self._region}.cloud.databricks.com"
        )
        unity_catalog_url = self._workplace_url
        logging.info("Server endpoint: %s", server_endpoint)
        logging.info("Unity catalog URL: %s", unity_catalog_url)
        self._sdk = ZerobusSdk(
            host=server_endpoint,
            unity_catalog_url=unity_catalog_url,
        )
        return self._sdk

    @staticmethod
    def get_descriptor(record: Any) -> Descriptor | None:
        """Return the DESCRIPTOR for a protobuf message, or None if the record has none.

        Useful in tests to assert the correct descriptor is used for TableProperties.
        """
        return getattr(record, "DESCRIPTOR", None)

    def _ensure_stream(self, descriptor: Descriptor | None = None) -> ZerobusStream:
        if self._stream is not None:
            return self._stream
        if self._sdk is None:
            self.generate_sdk()
        if descriptor is not None:
            table_properties = TableProperties(
                self._table_name, descriptor_proto=descriptor
            )
        else:
            table_properties = TableProperties(table_name=self._table_name)
        self._stream = self._sdk.create_stream(
            client_id=self._client_id,
            client_secret=self._client_secret,
            table_properties=table_properties,
            options=self._stream_options,
        )
        return self._stream

    def write(self, record: Any) -> Any:
        """
        Ingest a single record (protobuf Message or dict).
        Returns RecordAcknowledgment."""
        descriptor = self.get_descriptor(record)
        stream = self._ensure_stream(descriptor)
        return stream.ingest_record(record)

    def flush(self) -> None:
        """Flush the stream."""
        if self._stream is not None:
            self._stream.flush()

    def close(self) -> None:
        """Close the stream and release resources."""
        if self._stream is not None:
            self._stream.close()
            self._stream = None

    def __enter__(self) -> ZerobusWriter:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def _default_async_stream_options(
    ack_callback: ZerobusWriteCallback | AckCallbackLike | None,
) -> StreamConfigurationOptions:
    """Build default stream options for async writer (PROTO, 5k inflight, recovery)."""
    callback = ack_callback if ack_callback is not None else ZerobusWriteCallback()
    return StreamConfigurationOptions(
        record_type=RecordType.PROTO,
        max_inflight_records=5_000,
        recovery=True,
        ack_callback=callback,
    )


class AsyncZerobusWriter:
    """
    Async Zerobus stream writer. Uses zerobus.sdk.aio and supports write_offset,
    write_batch_offset, write_nowait, write_batch_nowait, flush, and close.
    """

    def __init__(
        self,
        *,
        host: str,
        workspace_url: str,
        workspace_id: str,
        region: str,
        client_id: str,
        client_secret: str,
        catalog: str,
        schema: str,
        table: str,
        stream_options: StreamConfigurationOptions | None = None,
        ack_callback: ZerobusWriteCallback | AckCallbackLike | None = None,
    ) -> None:
        self._host = host
        self._workspace_url = workspace_url
        self._workspace_id = workspace_id
        self._region = region
        self._client_id = client_id
        self._client_secret = client_secret
        self._table_name = f"{catalog}.{schema}.{table}"
        self._stream_options = stream_options or _default_async_stream_options(
            ack_callback
        )
        self._sdk: AioZerobusSdk | None = None
        self._stream: AioZerobusStream | None = None

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        ack_callback: ZerobusWriteCallback | AckCallbackLike | None = None,
    ) -> AsyncZerobusWriter:
        """Build an AsyncZerobusWriter from a config dict (e.g. Config.databricks())."""
        return cls(
            host=config["host"],
            workspace_url=config["workspace_url"],
            workspace_id=config["workspace_id"],
            region=config["region"],
            client_id=config["zerobus_client_id"],
            client_secret=config["zerobus_client_secret"],
            catalog=config["catalog"],
            schema=config["schema"],
            table=config["table"],
            ack_callback=ack_callback,
        )

    def with_stream_options(
        self, options: StreamConfigurationOptions
    ) -> AsyncZerobusWriter:
        """
        Overwrite stream options.
        Call before any write so they apply when the stream is created.
        """
        self._stream_options = options
        return self

    def _server_endpoint(self) -> str:
        return (
            f"{self._workspace_id}.zerobus.{self._region}.cloud.databricks.com"
        )

    async def _ensure_stream(
        self, descriptor: Descriptor | None = None
    ) -> AioZerobusStream:
        if self._stream is not None:
            return self._stream
        if self._sdk is None:
            server_endpoint = self._server_endpoint()
            logging.info("Server endpoint: %s", server_endpoint)
            logging.info("Unity catalog URL: %s", self._workspace_url)
            self._sdk = AioZerobusSdk(
                host=server_endpoint,
                unity_catalog_url=self._workspace_url,
            )
        if descriptor is not None:
            table_properties = TableProperties(
                self._table_name, descriptor_proto=descriptor
            )
        else:
            table_properties = TableProperties(table_name=self._table_name)
        self._stream = await self._sdk.create_stream(
            client_id=self._client_id,
            client_secret=self._client_secret,
            table_properties=table_properties,
            options=self._stream_options,
        )
        return self._stream

    @staticmethod
    def get_descriptor(record: Any) -> Descriptor | None:
        """Return the DESCRIPTOR for a protobuf message, or None."""
        return getattr(record, "DESCRIPTOR", None)

    async def write_offset(self, record: Any) -> Any:
        """
        Ingest a single record and return the result (offset/ack) when acknowledged.
        Use for recording offsets for a session.
        """
        descriptor = self.get_descriptor(record)
        stream = await self._ensure_stream(descriptor)
        return await stream.ingest_record(record)

    async def write_batch_offset(self, records: list[Any]) -> list[Any]:
        """Ingest a batch of records and return results (one per record) in order."""
        if not records:
            return []
        descriptor = self.get_descriptor(records[0])
        stream = await self._ensure_stream(descriptor)
        results = []
        for record in records:
            result = await stream.ingest_record(record)
            results.append(result)
        return results

    def write_nowait(self, record: Any) -> None:
        """Fire-and-forget: queue one record without waiting for ack."""
        asyncio.get_running_loop().create_task(self.write_offset(record))

    def write_batch_nowait(self, records: list[Any]) -> None:
        """Fire-and-forget: queue a batch without waiting for acks."""
        for record in records:
            self.write_nowait(record)

    async def flush(self) -> None:
        """Flush the stream and wait for durability."""
        if self._stream is not None:
            await self._stream.flush()

    async def close(self) -> None:
        """Close the stream and release resources."""
        if self._stream is not None:
            await self._stream.close()
            self._stream = None

    async def __aenter__(self) -> AsyncZerobusWriter:
        return self

    async def __aexit__(
        self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> None:
        await self.close()
