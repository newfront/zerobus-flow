"""Tests for TableUtils (catalog/schema helpers; no live Databricks for most tests)."""

from unittest.mock import MagicMock

from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo

from zerobus_ingest.utils.table_utils import TableUtils


def test_get_catalog_info_forwards_to_client():
    """get_catalog_info calls workspace_client.catalogs.get(catalog, **kwargs)."""
    cat = CatalogInfo(
        name="demos",
        full_name="demos",
        storage_root="s3a://bucket/prefix",
        storage_location="s3a://bucket/prefix/location",
    )
    w = MagicMock()
    w.catalogs.get = MagicMock(return_value=cat)

    out = TableUtils.get_catalog_info(w, "demos", include_browse=True)

    w.catalogs.get.assert_called_once_with("demos", include_browse=True)
    assert out is cat
    assert out.storage_root == "s3a://bucket/prefix"


def test_get_schema_info_builds_full_name():
    """get_schema_info calls schemas.get('catalog.schema', **kwargs)."""
    sch = SchemaInfo(
        name="zerobus",
        full_name="demos.zerobus",
        catalog_name="demos",
        storage_root="s3a://bucket/zerobus",
    )
    w = MagicMock()
    w.schemas.get = MagicMock(return_value=sch)

    out = TableUtils.get_schema_info(w, "demos", "zerobus")

    w.schemas.get.assert_called_once_with("demos.zerobus")
    assert out is sch
    assert out.storage_root == "s3a://bucket/zerobus"
