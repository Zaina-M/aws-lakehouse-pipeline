import datetime
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DateType, TimestampType
)

from jobs.order_items import run
from utils.delta_ops import upsert


_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("days_since_prior_order", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True),
    StructField("order_timestamp", TimestampType(), True),
    StructField("date", DateType(), True),
])

_ORDERS_SCHEMA = StructType([StructField("order_id", IntegerType(), True)])
_PRODUCTS_SCHEMA = StructType([StructField("product_id", IntegerType(), True)])

_DATE = datetime.date(2025, 4, 1)

_ROWS = [
    (1, 100, 1990, 10, 10, 1, 0, None, _DATE),   # valid
    (2, 100, 1990, 10, 11, 2, 1, None, _DATE),   # valid
    (1, 100, 1990, 10, 10, 1, 0, None, _DATE),   # duplicate id → deduped
    (3, None, 1990, 10, 10, 1, 0, None, _DATE),  # null order_id → rejected
    (5, 999, 1990, 10, 10, 1, 0, None, _DATE),   # orphan order_id → rejected
    (6, 100, 1990, 10, 888, 1, 0, None, _DATE),  # orphan product_id → rejected
]


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame(_ROWS, _SCHEMA)


@pytest.fixture
def orders_df(spark):
    return spark.createDataFrame([(100,), (101,)], _ORDERS_SCHEMA)


@pytest.fixture
def products_df(spark):
    return spark.createDataFrame([(10,), (11,)], _PRODUCTS_SCHEMA)


def _run(spark, sample_df, orders_df, products_df, delta_path):
    written = {}

    def fake_upsert(sp, df, path, **kw):
        written["rows"] = df.count()
        written["ids"] = {r["id"] for r in df.collect()}
        upsert(sp, df, delta_path, **kw)

    delta_reader = MagicMock()
    delta_reader.load.side_effect = lambda path: (
        orders_df if "orders" in path else products_df
    )

    with (
        patch("jobs.order_items.list_keys", return_value=["order_items/order_items_apr_2025.xlsx"]),
        patch("jobs.order_items.read_excel", return_value=sample_df),
        patch("jobs.order_items.archive"),
        patch("jobs.order_items.write_rejects") as mock_rejects,
        patch("jobs.order_items.upsert", side_effect=fake_upsert),
        patch.object(spark.read, "format", return_value=delta_reader),
    ):
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    return written, mock_rejects


def test_valid_rows_deduplicated_and_written(spark, sample_df, orders_df, products_df, tmp_path):
    written, _ = _run(spark, sample_df, orders_df, products_df, str(tmp_path / "order_items"))
    assert written["rows"] == 2
    assert written["ids"] == {1, 2}


def test_delta_table_written_correctly(spark, sample_df, orders_df, products_df, tmp_path):
    delta_path = str(tmp_path / "order_items")
    _run(spark, sample_df, orders_df, products_df, delta_path)

    result = spark.read.format("delta").load(delta_path)
    assert result.count() == 2


def test_merge_is_idempotent(spark, sample_df, orders_df, products_df, tmp_path):
    delta_path = str(tmp_path / "order_items")
    _run(spark, sample_df, orders_df, products_df, delta_path)
    _run(spark, sample_df, orders_df, products_df, delta_path)

    result = spark.read.format("delta").load(delta_path)
    assert result.count() == 2


def test_orphan_order_id_rejected(spark, sample_df, orders_df, products_df, tmp_path):
    _, mock_rejects = _run(spark, sample_df, orders_df, products_df, str(tmp_path / "order_items"))
    rejected_df = mock_rejects.call_args[0][0]
    reasons = {r["_rejection_reason"] for r in rejected_df.collect() if r["_rejection_reason"]}
    assert "orphan_order_id" in reasons


def test_orphan_product_id_rejected(spark, sample_df, orders_df, products_df, tmp_path):
    _, mock_rejects = _run(spark, sample_df, orders_df, products_df, str(tmp_path / "order_items"))
    rejected_df = mock_rejects.call_args[0][0]
    reasons = {r["_rejection_reason"] for r in rejected_df.collect() if r["_rejection_reason"]}
    assert "orphan_product_id" in reasons


def test_no_keys_skips_all_work(spark):
    with patch("jobs.order_items.list_keys", return_value=[]):
        with patch("jobs.order_items.read_excel") as mock_read:
            run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")
            mock_read.assert_not_called()
