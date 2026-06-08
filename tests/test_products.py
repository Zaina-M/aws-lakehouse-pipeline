import pytest
from unittest.mock import patch, call
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from jobs.products import run
from utils.delta_ops import upsert


_SCHEMA = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("product_name", StringType(), True),
])

_ROWS = [
    (1, 4, "Books", "Product_1"),
    (2, 2, "Books", "Product_2"),
    (1, 4, "Books", "Product_1 Dup"),   # duplicate product_id → deduped
    (3, 1, "Toys", None),               # null product_name → rejected
]


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame(_ROWS, _SCHEMA)


def _patches(sample_df, delta_path):
    written = {}

    def fake_upsert(spark, df, path, **kw):
        written["rows"] = df.count()
        written["ids"] = {r["product_id"] for r in df.collect()}
        upsert(spark, df, delta_path, **kw)

    return (
        patch("jobs.products.list_keys", return_value=["products/products.csv"]),
        patch("jobs.products.read_csv", return_value=sample_df),
        patch("jobs.products.archive"),
        patch("jobs.products.write_rejects"),
        patch("jobs.products.upsert", side_effect=fake_upsert),
        written,
    )


def test_valid_rows_deduplicated_and_written(spark, sample_df, tmp_path):
    delta_path = str(tmp_path / "products")
    *patches, written = _patches(sample_df, delta_path)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    assert written["rows"] == 2
    assert written["ids"] == {1, 2}


def test_delta_table_written_correctly(spark, sample_df, tmp_path):
    delta_path = str(tmp_path / "products")
    *patches, _ = _patches(sample_df, delta_path)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    result = spark.read.format("delta").load(delta_path)
    assert result.count() == 2


def test_merge_is_idempotent(spark, sample_df, tmp_path):
    delta_path = str(tmp_path / "products")
    *patches, _ = _patches(sample_df, delta_path)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    result = spark.read.format("delta").load(delta_path)
    assert result.count() == 2


def test_archive_called(spark, sample_df, tmp_path):
    delta_path = str(tmp_path / "products")
    *patches, _ = _patches(sample_df, delta_path)

    with patches[0], patches[1], patches[2] as mock_archive, patches[3], patches[4]:
        run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")

    mock_archive.assert_called_once_with("raw", "products/products.csv", "archive", "2025-04-01")


def test_no_keys_skips_all_work(spark):
    with patch("jobs.products.list_keys", return_value=[]):
        with patch("jobs.products.read_csv") as mock_read:
            run(spark, "raw", "dwh", "archive", "rejects", "lakehouse_db", "2025-04-01")
            mock_read.assert_not_called()
