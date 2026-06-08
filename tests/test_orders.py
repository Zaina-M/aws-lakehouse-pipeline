import datetime
import pytest
from unittest.mock import patch
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, DateType, TimestampType
)

from jobs.orders import run
from utils.delta_ops import upsert


_SCHEMA = StructType([
    StructField("order_num", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("order_timestamp", TimestampType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("date", DateType(), True),
])

_TODAY = datetime.date.today()
_YESTERDAY = _TODAY - datetime.timedelta(days=1)
_TOMORROW = _TODAY + datetime.timedelta(days=1)

_ROWS = [
    (90, 1, 1990, None, 99.99, _YESTERDAY),
    (41, 2, 5057, None, 49.99, _YESTERDAY),
    (90, 1, 1990, None, 99.99, _YESTERDAY),   # duplicate order_id → deduped
    (12, 3, 12, None, -10.00, _YESTERDAY),    # negative total → rejected
    (13, 4, None, None, 20.00, _YESTERDAY),   # null user_id → rejected
    (14, 5, 13, None, 15.00, _TOMORROW),      # future date → rejected
]


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame(_ROWS, _SCHEMA)


def _patches(sample_df, delta_path):
    written = {}

    def fake_upsert(spark, df, path, **kw):
        written["rows"] = df.count()
        written["ids"] = {r["order_id"] for r in df.collect()}
        upsert(spark, df, delta_path, **kw)

    return (
        patch("jobs.orders.list_keys", return_value=["orders/orders_apr_2025.xlsx"]),
        patch("jobs.orders.read_excel", return_value=sample_df),
        patch("jobs.orders.archive"),
        patch("jobs.orders.write_rejects"),
        patch("jobs.orders.upsert", side_effect=fake_upsert),
        written,
    )


def test_valid_rows_deduplicated_and_written(spark, sample_df, tmp_path):
    delta_path = str(tmp_path / "orders")
    *patches, written = _patches(sample_df, delta_path)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    assert written["rows"] == 2
    assert written["ids"] == {1, 2}


def test_delta_table_partitioned_by_date(spark, sample_df, tmp_path):
    delta_path = str(tmp_path / "orders")
    *patches, _ = _patches(sample_df, delta_path)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    result = spark.read.format("delta").load(delta_path)
    assert result.count() == 2


def test_merge_is_idempotent(spark, sample_df, tmp_path):
    delta_path = str(tmp_path / "orders")
    *patches, _ = _patches(sample_df, delta_path)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    result = spark.read.format("delta").load(delta_path)
    assert result.count() == 2


def test_archive_called_with_correct_args(spark, sample_df, tmp_path):
    delta_path = str(tmp_path / "orders")
    *patches, _ = _patches(sample_df, delta_path)

    with patches[0], patches[1], patches[2] as mock_archive, patches[3], patches[4]:
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    mock_archive.assert_called_once_with(
        "raw", "orders/orders_apr_2025.xlsx", "archive", "2025-04-01"
    )


def test_no_keys_skips_all_work(spark):
    with patch("jobs.orders.list_keys", return_value=[]):
        with patch("jobs.orders.read_excel") as mock_read:
            run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")
            mock_read.assert_not_called()
