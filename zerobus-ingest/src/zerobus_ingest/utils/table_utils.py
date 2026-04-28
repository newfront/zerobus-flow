"""TableUtils: helpers for Unity Catalog table operations."""

from __future__ import annotations

from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    ColumnTypeName,
    DataSourceFormat,
    SchemaInfo,
    TableInfo,
    TableType,
)
from google.protobuf.descriptor import Descriptor, FieldDescriptor

# Proto type to Unity Catalog scalar
# (reverse of zerobus generate_proto.py type mappings)
_PROTO_SCALAR_TO_UC: dict[int, ColumnTypeName] = {
    FieldDescriptor.TYPE_INT32: ColumnTypeName.INT,
    FieldDescriptor.TYPE_SINT32: ColumnTypeName.INT,
    FieldDescriptor.TYPE_SFIXED32: ColumnTypeName.INT,
    FieldDescriptor.TYPE_UINT32: ColumnTypeName.INT,
    FieldDescriptor.TYPE_FIXED32: ColumnTypeName.INT,
    FieldDescriptor.TYPE_INT64: ColumnTypeName.LONG,
    FieldDescriptor.TYPE_SINT64: ColumnTypeName.LONG,
    FieldDescriptor.TYPE_SFIXED64: ColumnTypeName.LONG,
    FieldDescriptor.TYPE_UINT64: ColumnTypeName.LONG,
    FieldDescriptor.TYPE_FIXED64: ColumnTypeName.LONG,
    FieldDescriptor.TYPE_STRING: ColumnTypeName.STRING,
    FieldDescriptor.TYPE_FLOAT: ColumnTypeName.FLOAT,
    FieldDescriptor.TYPE_DOUBLE: ColumnTypeName.DOUBLE,
    FieldDescriptor.TYPE_BOOL: ColumnTypeName.BOOLEAN,
    FieldDescriptor.TYPE_BYTES: ColumnTypeName.BINARY,
}
# TYPE_ENUM -> INT (store enum as integer in Delta)

# Spark DataType JSON for scalar ColumnTypeName
# (required by Create Table API).
_SCALAR_TYPE_JSON: dict[ColumnTypeName, str] = {
    ColumnTypeName.STRING: '{"type": "string"}',
    ColumnTypeName.INT: '{"type": "integer"}',
    ColumnTypeName.LONG: '{"type": "long"}',
    ColumnTypeName.FLOAT: '{"type": "float"}',
    ColumnTypeName.DOUBLE: '{"type": "double"}',
    ColumnTypeName.BOOLEAN: '{"type": "boolean"}',
    ColumnTypeName.BINARY: '{"type": "binary"}',
}


def _field_to_type_json(field: FieldDescriptor, depth: int = 0) -> str:
    """Return Spark DataType JSON for a field (for Create Table API type_json)."""
    if depth > 50:
        raise ValueError("Proto nesting too deep")
    kind = field.type
    if kind == FieldDescriptor.TYPE_MESSAGE:
        msg_desc = field.message_type
        fields_json = []
        for i, f in enumerate(msg_desc.fields, start=1):
            fj = _field_to_type_json(f, depth + 1)
            fields_json.append(
                f'{{"name": "{f.name}", "type": {fj}, "nullable": true}}'
            )
        return '{"type": "struct", "fields": [' + ", ".join(fields_json) + "]}"
    if kind == FieldDescriptor.TYPE_ENUM:
        return '{"type": "integer"}'
    scalar = _PROTO_SCALAR_TO_UC.get(kind)
    if scalar is not None:
        return _SCALAR_TYPE_JSON.get(scalar, '{"type": "string"}')
    return '{"type": "string"}'


def _field_to_uc_type_text(field: FieldDescriptor, depth: int = 0) -> str:
    """
    Return Unity Catalog type string for a field
    (e.g. STRING, STRUCT<...>, ARRAY<...>).
    """
    if depth > 50:
        raise ValueError("Proto nesting too deep")
    kind = field.type
    if kind == FieldDescriptor.TYPE_MESSAGE:
        msg_desc = field.message_type
        parts = []
        for f in msg_desc.fields:
            uc = _field_to_uc_type_text(f, depth + 1)
            parts.append(f"{f.name}:{uc}")
        return "STRUCT<" + ", ".join(parts) + ">"
    if kind == FieldDescriptor.TYPE_ENUM:
        return "INT"
    scalar = _PROTO_SCALAR_TO_UC.get(kind)
    if scalar is not None:
        return scalar.name
    raise ValueError(f"Unsupported proto field type: {field.name} (type={kind})")


def _descriptor_to_columns_impl(descriptor: Descriptor) -> list[ColumnInfo]:
    """
    Convert a message Descriptor to a list of ColumnInfo
    (UC column definitions).
    """
    result: list[ColumnInfo] = []
    for position, field in enumerate(descriptor.fields, start=1):
        name = field.name
        if field.label == FieldDescriptor.LABEL_REPEATED:
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                elem_type = _field_to_uc_type_text(field)
                elem_json = _field_to_type_json(field)
                result.append(
                    ColumnInfo(
                        name=name,
                        type_name=ColumnTypeName.ARRAY,
                        type_text=f"ARRAY<{elem_type}>",
                        type_json=f'{{"type": "array", "elementType": {elem_json},"containsNull": true}}',  # noqa: E501
                        position=position,
                    )
                )
            else:
                scalar = _PROTO_SCALAR_TO_UC.get(
                    field.type,
                    ColumnTypeName.STRING
                    if field.type == FieldDescriptor.TYPE_ENUM
                    else None,
                )
                if field.type == FieldDescriptor.TYPE_ENUM:
                    scalar = ColumnTypeName.INT
                if scalar is None:
                    scalar = ColumnTypeName.STRING
                scalar_json = _SCALAR_TYPE_JSON.get(scalar, '{"type": "string"}')
                result.append(
                    ColumnInfo(
                        name=name,
                        type_name=ColumnTypeName.ARRAY,
                        type_text=f"ARRAY<{scalar.name}>",
                        type_json=f'{{"type": "array", "elementType": {scalar_json}, "containsNull": true}}',  # noqa: E501
                        position=position,
                    )
                )
        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            type_text = _field_to_uc_type_text(field)
            type_json = _field_to_type_json(field)
            result.append(
                ColumnInfo(
                    name=name,
                    type_name=ColumnTypeName.STRUCT,
                    type_text=type_text,
                    type_json=type_json,
                    position=position,
                )
            )
        elif field.type == FieldDescriptor.TYPE_ENUM:
            result.append(
                ColumnInfo(
                    name=name,
                    type_name=ColumnTypeName.INT,
                    type_text="INT",
                    type_json=_SCALAR_TYPE_JSON[ColumnTypeName.INT],
                    position=position,
                )
            )
        else:
            type_name = _PROTO_SCALAR_TO_UC.get(field.type)
            if type_name is None:
                type_name = ColumnTypeName.STRING
            result.append(
                ColumnInfo(
                    name=name,
                    type_name=type_name,
                    type_text=type_name.name,
                    type_json=_SCALAR_TYPE_JSON.get(type_name, '{"type": "string"}'),
                    position=position,
                )
            )
    return result


class TableUtils:
    """Helpers to check table existence and other UC table operations."""

    @staticmethod
    def get_catalog_info(
        workspace_client: WorkspaceClient, catalog: str, **kwargs: Any
    ) -> CatalogInfo:
        """Load Unity Catalog metadata for a catalog (e.g. `demos`).

        The returned :class:`CatalogInfo` includes ``storage_root`` and
        ``storage_location`` when the metastore has them set (managed
        table roots for the catalog). Pass through optional API flags, e.g.
        ``include_browse=True`` if needed.

        Args:
            workspace_client: Authenticated Databricks WorkspaceClient.
            catalog: Catalog name only (not `catalog.schema`).

        Returns:
            :class:`CatalogInfo` for ``GET /unity-catalog/catalogs/{name}``.
        """
        return workspace_client.catalogs.get(catalog, **kwargs)

    @staticmethod
    def get_schema_info(
        workspace_client: WorkspaceClient,
        catalog: str,
        schema: str,
        **kwargs: Any,
    ) -> SchemaInfo:
        """Load Unity Catalog metadata for a schema (e.g. catalog `demos`, schema `zerobus`).

        The returned :class:`SchemaInfo` includes ``storage_root`` and
        ``storage_location`` for managed tables under that schema. Optional
        API flags, e.g. ``include_browse=True``, are forwarded to the client.

        Args:
            workspace_client: Authenticated Databricks WorkspaceClient.
            catalog: Parent catalog name.
            schema: Schema name (not a full `catalog.schema.table`).

        Returns:
            :class:`SchemaInfo` for ``GET /unity-catalog/schemas/{catalog.schema}``.
        """
        full_name = f"{catalog}.{schema}"
        return workspace_client.schemas.get(full_name, **kwargs)

    @staticmethod
    def table_exists(
        workspace_client: WorkspaceClient,
        catalog: str,
        schema: str,
        table: str,
    ) -> bool:
        """
        Return True if a table exists in the metastore for
        the given catalog, schema, and table name.
        """
        full_name = f"{catalog}.{schema}.{table}"
        response = workspace_client.tables.exists(full_name=full_name)
        return bool(getattr(response, "table_exists", False))

    @staticmethod
    def descriptor_to_columns(descriptor: Descriptor) -> list[ColumnInfo]:
        """Convert a protobuf message DESCRIPTOR to a list of Unity Catalog ColumnInfo.

        Uses the inverse of the [zerobus generate_proto type mappings]
        (https://github.com/databricks/zerobus-sdk-py/blob/main/zerobus/tools/generate_proto.py):
        int32->INT, int64->LONG, string->STRING, float->FLOAT, double->DOUBLE,
        bool->BOOLEAN, bytes->BINARY, enum->INT, message->STRUCT<...>,
        repeated->ARRAY<...>.

        Args:
            descriptor: A message Descriptor (e.g. Order.DESCRIPTOR from orders_pb2).

        Returns:
            List of ColumnInfo suitable for TableUtils.create_table(..., columns=...).
        """
        return _descriptor_to_columns_impl(descriptor)

    @staticmethod
    def pretty_print_columns(columns: list[ColumnInfo]) -> str:
        """Format a list of ColumnInfo as a readable table string.

        Args:
            columns: List of ColumnInfo (e.g. from descriptor_to_columns).

        Returns:
            A string with one column per line: name and type, aligned.
        """
        if not columns:
            return ""
        name_width = max(len(c.name) for c in columns)
        lines = []
        for c in columns:
            type_str = (
                c.type_text
                if c.type_text
                else (c.type_name.name if c.type_name else "?")
            )
            lines.append(f"  {c.name:<{name_width}}  {type_str}")
        return "\n".join(lines)

    @staticmethod
    def create_table(
        workspace_client: WorkspaceClient,
        catalog: str,
        schema: str,
        table: str,
        storage_location: str,
        *,
        data_source_format: DataSourceFormat = DataSourceFormat.DELTA,
        columns: list[ColumnInfo] | None = None,
        properties: dict[str, str] | None = None,
    ) -> TableInfo:
        """Create an **external** Delta table via the Unity Catalog REST API.

        The external-client ``tables.create`` API only supports EXTERNAL tables;
        use :meth:`create_managed_table` to create managed tables from outside a
        UC-enabled cluster.

        Args:
            workspace_client: Authenticated Databricks WorkspaceClient.
            catalog: Unity Catalog catalog name.
            schema: Schema name within the catalog.
            table: Table name within the schema.
            storage_location: Storage root URL (required for EXTERNAL tables).
            data_source_format: DataSourceFormat (default DELTA).
            columns: Optional list of ColumnInfo for the table schema.
            properties: Optional key-value properties for the table.

        Returns:
            TableInfo for the created table.
        """
        return workspace_client.tables.create(
            name=table,
            catalog_name=catalog,
            schema_name=schema,
            table_type=TableType.EXTERNAL,
            data_source_format=data_source_format,
            storage_location=storage_location,
            columns=columns,
            properties=properties,
        )

    @staticmethod
    def create_managed_table(
        workspace_client: WorkspaceClient,
        catalog: str,
        schema: str,
        table: str,
        columns: list[ColumnInfo],
        *,
        warehouse_id: str | None = None,
        or_replace: bool = False,
        if_not_exists: bool = True,
    ) -> TableInfo:
        """Create a managed Delta table by executing SQL via a SQL warehouse.

        The Unity Catalog REST API's ``tables.create`` endpoint only supports
        external tables from outside a UC-enabled cluster. This method works
        around that limitation by submitting a ``CREATE TABLE`` DDL statement
        through the SQL execution API, which does support managed tables.

        Args:
            workspace_client: Authenticated Databricks WorkspaceClient.
            catalog: Unity Catalog catalog name.
            schema: Schema name within the catalog.
            table: Table name within the schema.
            columns: List of ColumnInfo describing the table schema.
            warehouse_id: SQL warehouse ID to execute against. If None, uses the
                first available warehouse.
            or_replace: If True, emits ``CREATE OR REPLACE TABLE``.
            if_not_exists: If True (default), emits ``CREATE TABLE IF NOT EXISTS``.

        Returns:
            TableInfo for the created table.

        Raises:
            RuntimeError: If no SQL warehouse is available or statement fails.
        """
        import time

        from databricks.sdk.service.sql import StatementState

        if warehouse_id is None:
            warehouses = list(workspace_client.warehouses.list())
            if not warehouses:
                raise RuntimeError("No SQL warehouses available in this workspace.")
            warehouse_id = warehouses[0].id

        modifier = "OR REPLACE " if or_replace else ("IF NOT EXISTS " if if_not_exists else "")
        qualified = f"`{catalog}`.`{schema}`.`{table}`"
        col_defs = ",\n  ".join(f"`{c.name}` {c.type_text}" for c in columns)
        ddl = f"CREATE TABLE {modifier}{qualified} (\n  {col_defs}\n) USING DELTA"

        stmt = workspace_client.statement_execution.execute_statement(
            statement=ddl,
            warehouse_id=warehouse_id,
        )

        for _ in range(60):
            if stmt.status.state not in (StatementState.PENDING, StatementState.RUNNING):
                break
            time.sleep(2)
            stmt = workspace_client.statement_execution.get_statement(stmt.statement_id)

        if stmt.status.state != StatementState.SUCCEEDED:
            err = stmt.status.error
            raise RuntimeError(
                f"CREATE TABLE failed ({stmt.status.state}): "
                + (f"{err.error_code}: {err.message}" if err else "unknown error")
            )

        return workspace_client.tables.get(f"{catalog}.{schema}.{table}")
